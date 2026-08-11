<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb design — gap analysis vs. `buzz-disk-db` reference

Working draft for review. Reference impl:
`/cpp/buzz-cpp/src/app/buzz-disk-db`. Design under review:
`doc/design/diskdb/design-crow-diskdb.md`. Open backlog: R71, R72.

Findings are grouped:

- **A. Corrections** — the design says something inaccurate.
- **B. Gaps** — the reference impl has it; the design does not cover it.
- **C. Extensions** — the impl suggests something worth adopting.
- **D. Open questions** — need a decision before updating the design.

Each item ends with a **Proposal** line stating the suggested design
change. Items marked **[decide]** need your call before I update the
design doc.

---

## A. Corrections

### A1. Replay ordering + compaction strategy (the big one)

Design §3.4 ("Resolved") and §7 ("Replay algorithm") claim:

> the replay scans records by key prefix — crow-kv's slot mechanism
> provides write ordering. A prefix scan returns all busy/free records
> for a zone in slot order (the order they were written). This keeps
> diskdb a pure client of crow-kv with no extension required.

This is wrong on two counts:

1. **A key prefix scan returns records in lexicographic key order**
   (= `unit_offset` order for `BusyBlockKey`), **not slot order**.
   Verified against the crow-kv scan API:
   `lib/crow-kv-client/src/client.rs` `scan()` returns items in key
   order; `KvScanItem` carries only `{key, value}` — no per-item write
   slot. `min_slot` on the scan request is a **read-freshness floor**
   (ensures the serving replica has applied at least that slot), not a
   record-slot filter.
   `lib/crow-kv/src/cluster/px_kv_store.rs` `resolve_read_point`.

2. **Snapshot compaction (§7 step 3) says** "Delete all busy/free
   records with slot ≤ snapshot_slot" — but a plain prefix scan cannot
   filter by slot, so this step is not implementable with the current
   API.

The conclusion that *"replay order does not affect the final bitmap"*
is correct: only one of busy/free exists per offset at any time (busy
is deleted on free), so per-offset state is unambiguous regardless of
scan order.

### Record model

Three record types live on the disk-group's bound data group:

- **`BusyBlockValue`** at `BusyBlockKey { disk_id, zone_index,
  unit_offset }` — written on allocate. Carries `owner_chunk`
  (the reverse reference to the block's owner), `unit_size`, `state`.
  **Deleted on free** (in the same `batch_write` that writes the
  `FreeBlockValue`). On re-allocate (after a free), a new
  `BusyBlockValue` is written at the same key (new owner,
  `state = Ok`). Bounded by the number of currently-busy blocks (≤
  disk capacity).
- **`FreeBlockValue`** at `FreeBlockKey { disk_id, zone_index,
  unit_offset }` — written on free, in the same `batch_write` that
  deletes the `BusyBlockKey`. Carries `previous_owner` (the
  `owner_chunk` from the freed `BusyBlockValue`) for audit / scanner
  cross-check. On re-allocate, the `FreeBlockKey` is deleted (the
  block is busy again). **Transient** — deleted by compaction after
  being merged into the `ZoneValue` bitmap. Bounded by the number of
  frees since the last compaction.
- **`ZoneValue`** at `ZoneKey { disk_id, zone_index }` — the
  compacted snapshot bitmap. Updated periodically by compaction.

**Current state determination** (no slot ordering needed):
- A block is **busy** iff its `BusyBlockKey` exists.
- A block is **free** otherwise. A `FreeBlockKey` may exist for a
  not-yet-compacted free (carrying `previous_owner`); after compaction
  merges the free into `ZoneValue` and deletes the `FreeBlockKey`,
  neither key exists for that offset.
- This holds for both full scan (strategy 1, key order) and journal
  scan (strategy 2, slot order) — the busy record's existence is the
  indicator, not the write order.

**Why compaction deletes only free records:**
The ideal approach on free would be to update the bitmap in the
`ZoneValue` and write the whole `ZoneValue` to KV. But `ZoneValue` is
large (full bitmap), and frees are random across all zones and disks,
so a per-free `ZoneValue` write is too expensive. Instead, each free
deletes the `BusyBlockKey` and writes a small `FreeBlockValue` in one
`batch_write`; later, compaction lists the free records for a zone (a
prefix scan — the free records of one zone are contiguous in the
crow-tree page, so this is efficient), merges them into the `ZoneValue`
bitmap (clear the freed bits), writes the updated `ZoneValue` once, and
deletes the free records in one `batch_write`. Busy records for freed
blocks are already gone (deleted on free); busy records for live blocks
are untouched by compaction.

### Three complementary strategies (not alternatives)

The three options serve different roles and **all three belong in the
design**:

- **Strategy 1 — full scan rebuild (on-demand, via RPC/API).** Scan all
  live `BusyBlockKey`s for a zone and rebuild the bitmap from scratch.
  No snapshot needed. For each offset: if a `BusyBlockKey` exists, the
  bit is set (busy); otherwise the bit is clear (free). No slot
  ordering needed — the busy record's existence is the indicator.
  (`FreeBlockKey`s carry `previous_owner` audit info but are not needed
  for state determination.) **Not in the common code flow** — provided
  as an on-demand operation via RPC and API, used for consistency
  checks (verify the in-memory bitmap matches the records) or a full
  rebuild (e.g. after corruption, or when no `ZoneValue` snapshot
  exists). Works with the existing `scan` API. Cost = O(all live busy
  records per zone). Too slow for regular restart with many zones, but
  correct and always available.

- **Strategy 2 — journal scan replay (fast restart).** Load the
  `ZoneValue` snapshot, then replay only the operations (Put / Delete)
  written after `snapshot_slot`, in slot order, and apply them to the
  snapshot bitmap. One scan per disk-group (or per disk) covers all
  its zones — batch recovery, low overhead. **Requires a new crow-kv
  RPC** (`JournalScan`: slot-range + key-prefix filter, returns ops in
  slot order). This is the primary restart path. The "diskdb is a pure
  client of crow-kv" claim in §3.4 no longer holds — diskdb needs this
  one extension.

- **Strategy 3 — compaction (ongoing maintenance).** Periodically (or
  when the free-record count for a zone exceeds a threshold), merge
  free records into the `ZoneValue` bitmap and delete the free records
  in one `batch_write`. This keeps the uncompacted record set small,
  so strategy 2's replay is fast. Uses the existing `scan` +
  `batch_write` API — no crow-kv extension needed. Batch by `disk_id`
  prefix: one scan covers all zones on a disk (free records of one
  zone are contiguous in the tree page). Only free records are deleted;
  busy records for freed blocks were already deleted on free, busy
  records for live blocks are untouched.

How they work together:

- **Steady state**: allocate writes `BusyBlockValue` (and deletes any
  prior `FreeBlockKey` for that offset — re-allocate clears the free
  marker). Free deletes the `BusyBlockKey` and writes `FreeBlockValue`
  at `FreeBlockKey` in one `batch_write` (carries `previous_owner` for
  audit). Compaction (strategy 3) runs periodically, merging free
  records into `ZoneValue` and deleting the free records.
- **Restart**: load `ZoneValue` snapshot → journal scan (strategy 2)
  replays post-`snapshot_slot` operations in slot order → apply to
  bitmap. Fast because compaction kept the record set small.
- **On-demand (RPC/API)**: full scan (strategy 1) rebuilds the bitmap
  from all live records. Triggered by an operator or the §12 scanner
  for a consistency check or full rebuild — not in the common code
  flow. Used to verify the strategy-2 result, recover from corruption,
  or cold-start when no snapshot exists.

**Proposal:** All three strategies go into the design, each in its
role:

- §3.4 — fix the "Resolved" claim: prefix scan returns key order, not
  slot order. Drop the "pure client, no extension required" claim —
  strategy 2 needs a `JournalScan` RPC on crow-kv. Note the crow-kv
  extension as a dependency (new sub-design doc for `JournalScan`).
- §7 — rewrite replay + compaction:
  - Record model: busy is deleted on free, free is transient (merged
    by compaction), `ZoneValue` is the compacted bitmap.
  - Replay (strategy 2): load snapshot → journal scan post-snapshot
    ops in slot order → apply to bitmap.
  - Compaction (strategy 3): scan free records per zone, merge into
    `ZoneValue` bitmap, delete free records in one `batch_write`.
    Only free records are deleted; busy records for freed blocks were
    already deleted on free.
  - Full rebuild (strategy 1): scan all live `BusyBlockKey`s, rebuild
    bitmap from scratch. Used by scanner / cold start.
- §12 — reference strategy 1 as the scanner's rebuild mechanism (the
  scanner triggers it via the same RPC/API an operator would use).
- §4 — add an admin/RPC endpoint for strategy 1 (full scan rebuild),
  exposed as an on-demand operation (not in the common code flow).

---

## B. Gaps

### B1. 64-bit word alignment / last-zone padding

Reference: `hw/ddb_disk_zone.cpp` lines 42-46 enforces
`seg_count % 64 == 0` (zone unit count must be a multiple of the
64-bit bitmap word); `hw/ddb_disk.cpp` lines 202-207 rejects a zone
size that is not a multiple of `64 * disk_seg_size`.

Design §3.5 says "the last zone may be smaller" but never reconciles
this with word alignment. Need to specify how the last zone's bitmap
is handled.

**Resolution — per-zone `unit_capacity`, last zone may differ:**
Each zone's `unit_capacity` must be a multiple of 64 (the 64-bit
bitmap word size). All zones except the last have `unit_capacity =
zone_size / unit_size`. The last zone has `unit_capacity =
remaining_capacity / unit_size`, rounded down to a multiple of 64; the
sub-64-unit tail (at most 63 units) is unallocated. Only the last zone
may have a different size; all other zones on a disk are uniform.

Why this option (over uniform zones or bitmap masking):

- **No real complexity added.** `unit_capacity` is already per-zone
  (the allocator checks `used_count < unit_capacity` per zone, and the
  bitmap is sized per-zone). Making the last zone's `unit_capacity`
  different is a one-line construction change — no special-case in the
  allocator, no bitmap masking, no padding bits. The only constraint
  is that the last zone's `unit_capacity` is still a multiple of 64
  (round down the remainder).
- **Avoids waste.** On a 10 TB disk with 1 GB zones, dropping the last
  partial zone wastes up to 1 GB. With this option, only the
  sub-64-unit tail within the last zone is wasted (at most 63 MB with
  1 MB units).
- **Already in the design.** §3.5 already says "the last zone may be
  smaller" — this keeps that statement true with a concrete rule.
- **Future-proofs for native zones.** When CROW later adopts native
  zoned-namespace SSD or SMR HDD (§3.5's future direction), zone sizes
  are dictated by the device and may vary. The per-zone
  `unit_capacity` model is the natural fit; a uniform-zone model would
  need to be rewritten.

**Proposal:** Add to §3.5 and §8 the rule: each zone's `unit_capacity`
must be a multiple of 64; all zones except the last are uniform
(`zone_size / unit_size`); the last zone is `remaining_capacity /
unit_size` rounded down to a multiple of 64; only the last zone may
differ.

### B2. CAS retry bound + contention metric

Reference: `hw/ddb_disk_zone.cpp` lines 246-264 caps per-bit CAS at
**100 retries** and emits a `ddb.zone.allocate.retry.cms.bit.count`
counter on each retry.

Design §8 says "on CAS failure, re-scan the same word" with no bound —
under heavy contention a thread could spin indefinitely on one bit.

**Proposal:** Add to §8: a bounded CAS retry (default 100, then fall
through to the next bit / word / zone). Add to §11 a
`zone.allocate.retry.cms.bit` counter as the key operational signal
for lock-free allocator contention. Config item in the new §
"Configuration" (see B7).

### B3. `AllocateBlocks` routing + multi-block atomicity scope

Reference: `hw/ddb_hw.cpp` lines 26-51 routes allocate by `node_id` to
a specific node. In CROW, a diskdb instance owns **multiple
disk-groups**, each on one node. Design §8 "Node-level round-robin"
never says how the caller targets a disk-group, and §3.2 says
"multi-block allocation can use a single `batch_write` (atomic within
a group)" — but §8 round-robins across disks which could span multiple
disk-groups (different paxos groups). The reference impl only ever did
single-block, so this is genuinely undecided.

**Resolution — request carries `disk_group_id`, no `node_id`:**
CROW uses disk-groups instead of nodes as the allocation routing unit.
The `AllocateBlocks` request carries `disk_group_id` (not `node_id`);
the diskdb instance routes to the named disk-group it owns. Allocate
is scoped to one disk-group; multi-block uses one `batch_write` on
that disk-group's bound paxos data group; atomic within the group.
The caller (or a future placement service) picks the disk-group.

Consequences for the design:

- **§4 request schema** — `AllocateBlocksRequest` carries
  `disk_group_id` (not `node_id`); drop `node_id` from the request.
  The reference impl's `node_id` routing is replaced by
  `disk_group_id` routing.
- **§8 "Node-level round-robin"** — rename/rewrite as
  "round-robin across disks within the named disk-group". The
  round-robin is over the disks **within one disk-group** (the named
  `disk_group_id`), not across disk-groups. Multi-block allocation
  stays within one disk-group → one `batch_write` on one paxos data
  group → atomic.
- **§3.2 atomicity scope** — confirm: multi-block atomicity holds
  within a disk-group (one paxos data group). There is no cross-group
  multi-block allocate in v1 (the request is scoped to one
  `disk_group_id`); cross-group allocation is the caller's
  responsibility (multiple `AllocateBlocks` calls, one per group).
- **`exclude_disks` (B4)** — still per-disk (skip a disk that just
  failed), applied within the named disk-group.
- **Free** — `FreeBlocks` carries `Segment`s (each with `disk_id`,
  `zone_index`, `unit_offset`, `owner_chunk`); the diskdb instance
  looks up the disk-group from the disk-id (via group-0 metadata or
  the in-memory disk-id → disk-group map). No `disk_group_id` needed
  in the free request.

**Proposal:** Add the decision to §3.2, §4 (request fields:
`disk_group_id` in, `node_id` out), and §8 (rewrite "Node-level
round-robin" as "round-robin across disks within the named
disk-group").

### B4. `exclude_disks` missing from the protocol schema

Reference: `rpc/proto/msg_ddb_allocate_block_request.cpp` lines 35-42
— the allocate request carries `exclude_disk_ids` (anti-affinity /
retry-after-failure).

Design §8 mentions "excluded disks are skipped" but §4's
`AllocateBlocks` request fields do not list it.

**Proposal:** Add `exclude_disks: repeated DiskId` to the
`AllocateBlocksRequest` schema in §4 and to the request field list in
§7. Note: anti-affinity is per-disk (skip a disk that just failed),
not per-zone.

### B5. Sync loop semantics: stale guard, missing-detection, error back-off

Reference:

- `hw/refresh_ddb_hw_task.cpp` lines 41-83 — distinct error retry
  interval (3s on failure vs 30s on success).
- `hw/ddb_node.cpp` lines 181-190 — disks/nodes absent from the sync
  response are transitioned to Offline (this is how `Missing` is
  detected).
- `hw/ddb_hw.cpp` lines 88-93 and `hw/ddb_node.cpp` lines 140-145 —
  epoch/revision guard: skip an update whose epoch ≤ current.

Design §10 only says "13 s default" with no error back-off, and §9
lists `Missing → Bad/Up` but never says `Missing` is detected by
absence from group-0 sync.

**Proposal:** Add to §10:

- **Epoch/revision guard** — skip a sync response whose epoch ≤
  current (prevents stale overwrites).
- **Missing detection** — a disk/node absent from a sync response is
  transitioned to `Missing` (then to `Bad` after confirmation or `Up`
  if rediscovered). This is the trigger for the §9 `Missing` state.
- **Fixed sync interval** — v1 uses a single fixed sync interval
  (default 10 s), the same on success and failure. No error back-off
  in v1. Add to the new § "Configuration" (B7).
- **Future: group-0 notify** — a future follow-up adds a
  watch/notify feature where group 0 pushes hw-status-change
  notifications to registered diskdb endpoints (each diskdb registers
  its endpoint on sync). This replaces polling for status changes.
  Tracked as **R78** (`doc/backlog/R78-diskdb-group0-notify-watch.md`).
  v1 ships with fixed-interval polling; the design doc §10 should
  reference R78 as the follow-up.

### B6. Graceful-shutdown FreeBatch flush

Reference: `hw/ddb_disk.cpp` lines 69-74 — the impl flushes in the
destructor on shutdown.

Design §8 free is async (500 ms batch). On graceful shutdown, unflushed
frees would become ghost allocations on restart (blocks appear busy in
KV but free in memory).

**Resolution — free is immediate in v1; batching is a follow-up:**
The first implementation (R72) does **not** batch frees — each free
deletes the `BusyBlockKey` and writes its `FreeBlockValue` (per the
record model in A1) to the bound data group immediately via one
`batch_write`. No `FreeBatch`, no timer, no background flush loop.
This is simpler, avoids the ghost-allocation-on-crash window, and
matches the "records are the source of truth" model directly.

Free batching (grouping many frees into one `batch_write` per flush)
is an **optimization** for high-free-throughput workloads. It is
tracked as a separate follow-up requirement (**R79**,
`doc/backlog/R79-diskdb-free-batch.md`). When R79 ships:

- **No timer.** The batch flush is **not** driven by a periodic timer.
  A timer-based flush introduces a ghost-allocation window (crash
  between free and flush) and adds a background task. Instead, the
  batch flush is triggered by **batch size** (flush when the batch
  reaches `free_flush_max_batch`, default 256) — a synchronous
  threshold on the free path, not a background loop.
- **Graceful shutdown** — on graceful shutdown, drain and flush the
  `FreeBatch` before exit. On ungraceful shutdown, unflushed frees are
  left for the §12 ghost-allocation scanner to reconcile. State this
  explicitly.

**Proposal:**

- §8 (free) — v1: each free is one immediate `batch_write` (no batch,
  no timer). Note R79 as the follow-up for size-threshold batching.
- §8 (shutdown) — when R79 ships: graceful shutdown drains + flushes
  the `FreeBatch`; ungraceful shutdown leaves unflushed frees for the
  §12 scanner.
- R72 scope — remove the `FreeBatch` and `FreeFlushLoop` from R72;
  free is immediate. R72's free handler writes one `FreeBlockValue`
  + keeps the `BusyBlockKey` (per A1) in one `batch_write`.

### B7. No consolidated configuration section

Reference: `config/ddb_config.h` centralizes tunables.

Design has no config section. R71/R72 mention defaults inline but
scattered.

**Proposal:** Add a new § "Configuration" enumerating (with defaults).
All settings that control flow behavior move to a config class (no
hardcoded tunables in business logic):

- sync interval (10s, fixed — same on success and failure), degraded
  miss threshold (3), temp-failure timeout (900s)
- `zone_rotate_count`, `free_flush_max_batch` (256, used by R79 when
  free batching is enabled)
- snapshot compaction threshold (record count or time)
- CAS retry limit (100)
- block / unit size (default 1M), zone size
- compaction cadence (periodic interval for strategy 3)
- `free_batch_enabled` (default false — R72 immediate free; R79
  size-threshold batching when true)

### B8. Metrics not enumerated

Reference counters:

- `hw/ddb_disk_zone.cpp` lines 27-29 — per-zone `allocate`,
  `free`, `allocate.retry.cms.bit`.
- `hw/ddb_disk.cpp` lines 33-34 — per-disk `allocate`, `free`.
- `hw/ddb_node.cpp` lines 26-27 — per-node `allocate`, `free`.

Design §11 says "reuses crow-common metrics" but lists none. Metrics
must show **internal status** (gauges reflecting current state) and a
**latency hierarchy** (where time is spent, broken down by layer) so
operators can diagnose both capacity problems and performance
bottlenecks.

**Proposal:** Add to §11 a three-category metrics design:

**1. Counters (events, monotonically increasing):**

- per-zone: `allocate.count`, `free.count`,
  `allocate.retry.cms.bit.count` (contention signal — ties to B2)
- per-disk: `allocate.count`, `free.count`
- per-disk-group: `allocate.count`, `free.count`
- per-instance: `sync.count`, `sync.error.count`,
  `compaction.count`, `compaction.error.count`,
  `free_batch.flush.count` (R79, when batching enabled)

**2. Gauges (internal status, current state snapshot):**

- per-disk: `capacity_bytes`, `used_bytes`, `free_bytes`,
  `used_pct`, `zone_count`, `active_zone_count`
- per-zone: `unit_capacity`, `used_count`, `free_count`,
  `alloc_state` (derived: Active/Available/Full),
  `hw_status` (inherited from disk)
- per-disk-group: `disk_count`, `allocatable_disk_count`,
  `capacity_bytes`, `used_bytes`, `free_bytes`
- per-instance: `owned_disk_group_count`, `degraded` (0/1),
  `free_batch_len` (current pending frees, R79),
  `uncompacted_free_record_count` (per zone — compaction backlog),
  `last_sync_slot` (group-0 sync frontier),
  `last_sync_age_secs` (time since last successful sync)

**3. Latency hierarchy (where time is spent, per layer):**

The allocate/free paths are two-phase (sync bitmap claim + async KV
persist). The latency hierarchy breaks down each phase so operators
can see whether the bottleneck is the in-memory allocator or the KV
persist round-trip.

- **Allocate path:**
  - `allocate.rpc.latency_us` — total RPC latency (handler entry →
    response). This is the top of the hierarchy.
  - `allocate.bitmap_scan.latency_us` — Phase 1 sync: time in the
    zone bitmap-scan + per-bit CAS (includes CAS retries). This is
    nanoseconds in the common case; spikes indicate contention.
  - `allocate.kv_persist.latency_us` — Phase 2 async: time awaiting
    the `batch_write` of `BusyBlockValue` to the data group. This is
    the dominant latency component (one paxos round-trip).
  - `allocate.zone_rotate.latency_us` — time spent in
    `rotate_active_zones` when the active set is exhausted (should be
    near-zero in steady state; spikes indicate all zones are near
    full).
- **Free path:**
  - `free.rpc.latency_us` — total RPC latency.
  - `free.bitmap_clear.latency_us` — time in the per-bit CAS clear
    (nanoseconds).
  - `free.kv_persist.latency_us` — time awaiting the `batch_write` of
    `FreeBlockValue` (immediate in v1; batch flush in R79).
- **Sync path:**
  - `sync.latency_us` — total `sync_once` latency.
  - `sync.read_group0.latency_us` — time reading from group 0
    (prefix scans of node/disk/disk-group/owner/bind maps).
  - `sync.apply_changes.latency_us` — time applying changes to
    in-memory state (add/remove disks, status transitions, context
    refresh).
- **Compaction path:**
  - `compaction.latency_us` — total compaction latency per zone.
  - `compaction.scan_free.latency_us` — time scanning free records
    (prefix scan).
  - `compaction.merge_bitmap.latency_us` — time merging free records
    into the `ZoneValue` bitmap (in-memory).
  - `compaction.kv_persist.latency_us` — time awaiting the
    `batch_write` (new `ZoneValue` + delete free records).

**Implementation notes:**

- Per-disk and per-zone hot-path counters stay as atomics and flush
  into the crow-common registry at reporting intervals (already in
  §3.7).
- Latency metrics use `LatencyHistogram` (percentile precision) for
  the hot paths (allocate/free bitmap scan, KV persist) and
  `LatencySummary` (count + sum + max + avg) for the cold paths
  (sync, compaction, zone rotate). This matches crow-kv's own
  metrics convention (`design-crow-kv-observability.md`).
- Gauges are updated on the reporting interval (not on every change)
  by reading the in-memory state — they are derived snapshots, not
  hot-path writes.
- `degraded` and `last_sync_age_secs` are the key health indicators
  for alerting (degraded = sync failures exceeded threshold;
  last_sync_age > 2x interval = sync stuck).

### B9. Free-path lookup structures undescribed

Reference: `hw/ddb_node.cpp` lines 90-110 (disk-by-id lock-free hash
map) and `hw/ddb_disk.cpp` lines 139-161 (zone-by-index vec) under RCU
contexts for O(1) free.

Design §14 describes RCU publish for the **allocate** context but says
nothing about the **free**-side lookup structures.

**Resolution — two lookup layers, different complexity:**

Free requires: (1) find the in-memory zone to clear the bitmap bit,
and (2) find the matching `BusyBlockKey` in KV so the free can write a
`FreeBlockValue` in the same `batch_write`. These are different
lookups with different complexity characteristics.

**1. In-memory zone lookup (O(1), hash-indexed):**

- disk-id → disk: a hash map (RCU-published alongside the allocate
  context on add/remove/status-change). O(1) average.
- zone-index → zone: a vec indexed by zone-index (RCU-published).
  O(1) — direct index.
- These match the reference impl's `free_context` pattern. The bitmap
  clear is O(1) after the lookups.

**2. KV busy-block lookup (NOT guaranteed O(1)):**

The free must delete the `BusyBlockKey` and write a `FreeBlockValue`
at `FreeBlockKey { disk_id, zone_index, unit_offset }`. The key is
fully determined by the `Segment` in the free request (`disk_id`,
`zone_index`, `unit_offset` are all in the segment) — so the
`FreeBlockKey` is constructed directly, no lookup needed. **But** the
free also needs the `owner_chunk` from the existing `BusyBlockValue`
(to carry it in the `FreeBlockValue` as `previous_owner`, or to
validate ownership before freeing). Two options:

- **(a) Carry `owner_chunk` in the `Segment`** — the free request
  already has `owner_chunk` (it's part of the segment the caller
  received on allocate). No KV read needed; the free is one
  `batch_write` (Delete `BusyBlockKey` + Put `FreeBlockValue` per the
  record model in A1). Truly O(1) — no KV read on the free path. The
  caller is trusted to pass the correct `owner_chunk`; a mismatch
  indicates a bug or stale free.
- **(b) Read the `BusyBlockValue` from KV first** — `get` the
  `BusyBlockKey` to fetch `owner_chunk`, validate it matches the
  caller, then `batch_write` (Delete `BusyBlockKey` + Put
  `FreeBlockValue`). This is one KV read + one KV write per free — not
  O(1) in the KV sense (the read is a paxos round-trip). The read
  provides ownership validation but doubles the free latency.

**Recommend (a) for v1** — carry `owner_chunk` in the `Segment`, no KV
read on free. The free is one `batch_write`, O(1) in both the
in-memory and KV sense. Ownership validation is deferred to the §12
scanner (which can cross-check `Segment.owner_chunk` against the
`BusyBlockValue` in KV). If strict ownership validation is needed
before free, (b) is the fallback — note it as a config toggle
(`validate_owner_on_free`, default false).

**Proposal:** Add to §14:

- In-memory free lookup: disk-id → disk (hash, O(1) avg), zone-index
  → zone (vec, O(1)), both RCU-published alongside the allocate
  context. Bitmap clear is O(1) after lookups.
- KV free path: the `FreeBlockKey` is constructed directly from the
  `Segment` (no lookup). `owner_chunk` is carried in the `Segment`
  (option a) — no KV read on free in v1; the free is one
  `batch_write`. Note option (b) as a config toggle
  (`validate_owner_on_free`, default false) for strict ownership
  validation.

### B10. Initial zone record creation on disk-add

Design §3.1 says "a zone is created when the disk is added, and is
maintained as a separate zone record on the disk-group's bound paxos
data group" — but never describes the **initial write**: when a disk
is added, diskdb must create baseline `ZoneValue` snapshots (empty
bitmaps, one per zone) on the bound data group, which are the replay
baseline (§7).

**Resolution — operator adds disk in group 0, diskdb initializes on
sync:**

The disk-add flow is split across group 0 and diskdb:

1. **Operator adds the disk in group 0** — via the admin/console API
   (`AddDisk` on `DiskdbAdminService`, §4). This writes the `DiskMeta`
   to group 0 (store 0, group 0) at `DiskKey { node_id,
   disk_group_id, disk_id }`. No zone records are created yet — group
   0 holds disk metadata only (§3.1: zones are NOT maintained in
   group 0).
2. **diskdb sync detects the new disk** — on the next sync tick
   (R71's `SyncLoop`), diskdb fetches the updated `DiskMeta` from
   group 0 and sees a disk it does not yet have in its in-memory
   state.
3. **diskdb initializes the disk** — diskdb creates the in-memory
   `ZoneDisk` with one `Zone` per zone (zone count = `capacity /
   zone_size`, last zone sized per B1's rule). For each zone, it
   writes a baseline `ZoneValue` (empty bitmap, `snapshot_slot = 0`)
   to the disk-group's bound paxos data group at `ZoneKey { disk_id,
   zone_index }` in one `batch_write`. These are the replay baselines;
   subsequent allocates write `BusyBlockValue` records on top.

This matches the reference impl's pattern: `hw/ddb_node.cpp` lines
157-167 — on sync, a disk absent from the local state is loaded (or
created empty) and `update()` is called, which creates the zones
(`hw/ddb_disk.cpp` lines 218-227). CROW replaces the local-file load
with group-0 fetch + bound-data-group `ZoneValue` writes.

**Proposal:** Add to §10's disk-add flow the three-step sequence
above: operator adds disk in group 0 → diskdb sync detects → diskdb
initializes zones (in-memory + baseline `ZoneValue` per zone to the
bound data group in one `batch_write`). Note that group 0 holds disk
metadata only; zone records live on the bound data group.

---

## C. Extensions

### C1. Per-block `state` field

Reference: `hw/disk_block.h` carries `FBDiskBlockState`. Design
replaces `tag` with `owner_chunk` but is silent on per-block state.

**Resolution — keep a per-block state in `BusyBlockValue`:**
A busy block carries a `state` field that controls I/O behavior for
the block's data (which diskdb does not read/write itself, but a
future diskio/object-store component will). The state lets diskdb
mark a block so the data-IO layer can react:

- **`Ok`** — normal; data I/O proceeds as usual. Default on allocate.
- **`Suspect`** — the block's data may be unreadable (disk health
  degrading, slow reads reported). The data-IO layer tries to read
  with a timeout; on timeout, falls back to a mirror copy or EC
  rebuild. diskdb transitions a block to `Suspect` on disk health
  degradation (the disk goes `Suspect` in §9, and its busy blocks
  inherit `Suspect` state).
- **`Corrupt`** — the block's data is confirmed unreadable. The
  data-IO layer skips reading it and rebuilds from EC parity or reads
  a mirror copy. diskdb transitions a block to `Corrupt` when a read
  failure is confirmed (reported by the data-IO layer via a future
  `MarkBlockCorrupt` RPC), or when the §12 scanner detects a CRC
  mismatch in the block's zone.

The state is **not** a CAS state machine (no concurrency contention
on it) — it is updated by the sync loop / health probe / scanner
(background, single writer per disk-group), not by the allocate hot
path. The allocate hot path always writes `state = Ok`.

**`FreeBlockValue` does not carry state** — a free block has no data
to read; state is irrelevant. On re-allocate, the new `BusyBlockValue`
starts at `Ok` regardless of the prior block's state.

**Proposal:** Add to §7 (`BusyBlockValue` fields): `state:
BlockState` enum (`Ok`, `Suspect`, `Corrupt`). Document the semantics
above (I/O behavior control, not allocation state). Note that the
state is updated by background paths (sync, health probe, scanner),
not the allocate hot path. Add a future `MarkBlockCorrupt` /
`MarkBlockSuspect` admin RPC to §4 (served by diskdb, called by the
data-IO layer or operator). The `ZoneAllocationState` in §9 remains
the derived zone-level reporting enum (Active/Available/Full) — it is
distinct from this per-block `BlockState`.

### C2. `BusyBlockValue` / `FreeBlockValue` field schemas

R72's `BusyBlockValue { unit_count, owner_chunk, allocate_count: 1 }`
mentions `allocate_count`, but design §7 never documents the value
schemas.

**Resolution — no refcount, no version; `unit_size` in v1:**
There is no `allocate_count` field — no refcount, no version. A free
deletes the `BusyBlockKey` and writes a `FreeBlockValue`; a later
allocate at the same offset writes a new `BusyBlockValue` and deletes
the `FreeBlockKey`. The record model (A1) handles this without
versioning — the busy record's existence is the sole state indicator.

The value schemas for v1:

- **`BusyBlockValue`**:
  - `unit_count: u32` — number of units in this allocation (≥ 1;
    multi-unit allocations span consecutive offsets).
  - `unit_size: u32` — size of one unit in bytes (e.g. 1 MB). Carried
    per-block so the data-IO layer knows the block's granularity
    without a separate lookup. All units in one allocation have the
    same `unit_size`.
  - `owner_chunk: ChunkId` (192-bit) — reverse reference to the
    block's owner (the chunk that holds this block's data). Present
    only while the block is busy (the `BusyBlockValue` is deleted on
    free; `owner_chunk` is captured into `FreeBlockValue.previous_owner`
    at free time, per A1).
  - `state: BlockState` — per-block I/O-behavior state (`Ok`,
    `Suspect`, `Corrupt`) per C1. Default `Ok` on allocate.
- **`FreeBlockValue`**:
  - `unit_count: u32` — number of units freed (matches the
    corresponding `BusyBlockValue.unit_count`).
  - `previous_owner: ChunkId` (192-bit) — the `owner_chunk` from the
    freed `BusyBlockValue`, captured at free time from the `Segment`
    (no KV read needed). Carried for audit / scanner cross-check. No
    `state` field — a free block has no data.
- **`ZoneValue`** (the compacted snapshot):
  - `usage_bitmap: bytes` — the full zone bitmap (one bit per unit;
    bit set = busy, bit clear = free). Sized to the zone's
    `unit_capacity` (multiple of 64 bits per B1).
  - `snapshot_slot: u64` — the slot at which this snapshot was
    written. Strategy 2 (journal scan replay) replays operations
    after this slot.
  - `crc32: u32` — CRC32 checksum over `usage_bitmap` for integrity
    verification (§12 scanner).

**Proposal:** Document the three value schemas above in §7. Drop
`allocate_count` entirely (no refcount, no version). Add `unit_size`
to `BusyBlockValue` (carried per-block for the data-IO layer). Note
that re-allocate at the same offset writes a new `BusyBlockValue`
(the old one was deleted on free) and deletes the `FreeBlockKey` — no
version tracking needed.

### C3. Per-disk usage summary piggybacked on keepalive

Reference: `hw/refresh_ddb_hw_task.cpp` lines 43-48 pushes per-disk
space usage to the metadata store on every sync
(`co_update_space_usage`).

Design §11 keeps usage in-memory/derived. v1 non-goal per §2 (no
aggregated dashboards), but a lightweight per-disk usage summary in
group 0 would let the console show cluster-wide usage without querying
every instance.

**Resolution — piggyback on keepalive, group 0 maintains at
disk-group level:**
Rather than a separate usage-summary push, diskdb piggybacks basic
disk usage info on the **keepalive message** (the instance heartbeat
that R71 already sends to group 0 on each sync tick). The keepalive
already carries `instance_id` and owned `disk_group_id`s; it gains a
per-disk-group usage summary:

- `disk_group_id`
- `capacity_bytes` (sum of all disks in the group)
- `used_bytes` (sum of busy units across all zones in the group)
- `free_bytes` (capacity - used)
- `disk_count`, `allocatable_disk_count`

Group 0 maintains this basic info at the **disk-group level** (not
per-disk — per-disk detail stays in the diskdb instance's in-memory
state and is queried on demand via R74's `query_disk_usage` API). The
console reads the disk-group-level summary from group 0 for
cluster-wide overview; drill-down to per-disk/per-zone is via R74.

The summary is **derived** (recomputed from the in-memory bitmap on
each sync tick before sending the keepalive) and is a convenience for
the console, not a source of truth — the bitmap records are the source
of truth.

**Proposal:** Update §10 (sync loop / keepalive) and §11:

- §10 — the keepalive message gains a per-disk-group usage summary
  (`capacity_bytes`, `used_bytes`, `free_bytes`, `disk_count`,
  `allocatable_disk_count`). Computed from in-memory bitmap on each
  sync tick. Group 0 stores it at the disk-group level
  (`DiskGroupUsageKey { disk_group_id }`).
- §11 — note that the disk-group-level usage summary is available in
  group 0 for console cluster-wide overview; per-disk/per-zone
  drill-down is via R74's `query_disk_usage` API. The summary is
  derived, not a source of truth.
- This is **in scope for v1** (it rides on the existing keepalive, no
  separate push path).

### C4. Three-level effective status (node/group/disk)

Reference checks two levels (node up + disk up). CROW adds the
disk-group layer. Design §9 has this (`max(node, group, disk)`) — just
needs an explicit note that the impl's two-level check becomes
three-level, and the group layer is new in CROW.

**Proposal:** Add a one-line note to §9 that the reference impl's
two-level (node + disk) check becomes three-level (node + disk-group +
disk) in CROW; the disk-group layer is new.

---

## D. Resolved and open questions

### Resolved

- **Q1 (A1) — RESOLVED:** All three strategies go into the design, each
  in its role:
  - Strategy 1 (full scan) — on-demand, via RPC/API; consistency check
    or full rebuild; not in the common code flow.
  - Strategy 2 (journal scan) — fast restart; needs a new `JournalScan`
    RPC on crow-kv.
  - Strategy 3 (compaction) — ongoing maintenance; merges free records
    into `ZoneValue`, deletes only free records. Busy records are
    deleted on free (not persisted); busy records for live blocks are
    untouched by compaction.
  - The "pure client" claim in §3.4 is dropped — diskdb needs one
    crow-kv extension (`JournalScan`).
- **Q2 (B1) — RESOLVED:** Per-zone `unit_capacity`, last zone may
  differ. Each zone's `unit_capacity` must be a multiple of 64; all
  zones except the last are uniform (`zone_size / unit_size`); the
  last zone is `remaining_capacity / unit_size` rounded down to a
  multiple of 64; only the last zone may differ. No bitmap masking,
  no padding bits.
- **Q3 (B3) — RESOLVED:** Request carries `disk_group_id` (not
  `node_id`). CROW uses disk-groups instead of nodes as the allocation
  routing unit. Allocate is scoped to one disk-group; multi-block uses
  one `batch_write` on that group's bound paxos data group; atomic
  within the group. No cross-group multi-block allocate in v1. §8's
  "Node-level round-robin" is rewritten as "disk-group-level
  round-robin" within one group.
- **Q4 (C1) — RESOLVED:** Keep a per-block `state` field in
  `BusyBlockValue` (`Ok`, `Suspect`, `Corrupt`). Controls I/O
  behavior for the block's data (skip corrupt, timeout suspect).
  Updated by background paths (sync, health probe, scanner), not the
  allocate hot path. `FreeBlockValue` does not carry state. Add a
  future `MarkBlockCorrupt` / `MarkBlockSuspect` admin RPC.
- **Q5 (C2) — RESOLVED:** No `allocate_count` field — no refcount, no
  version. Re-allocate at the same offset overwrites the
  `BusyBlockValue` and deletes the `FreeBlockKey`. Value schemas:
  `BusyBlockValue { unit_count, unit_size, owner_chunk, state }`,
  `FreeBlockValue { unit_count, previous_owner }`,
  `ZoneValue { usage_bitmap, snapshot_slot, crc32 }`.

### Open

None — all questions resolved.

---

## Next step

Q1 is resolved. After you answer Q2–Q5, I will update
`doc/design/diskdb/design-crow-diskdb.md`:

- §3.4 — fix the "Resolved" claim (prefix scan = key order, not slot
  order); drop "pure client, no extension required"; note the
  `JournalScan` crow-kv extension dependency.
- §3.5 — add the last-zone alignment rule per Q2.
- §3.2, §4, §8 — add `disk_group_id` routing + atomicity scope per Q3;
  add `exclude_disks` to the request schema (B4).
- §7 — rewrite replay + compaction per the three-strategy model:
  - Record model: busy is deleted on free, free is transient (merged
    by compaction), `ZoneValue` is the compacted bitmap.
  - Strategy 1 (full scan rebuild), strategy 2 (journal scan replay),
    strategy 3 (compaction — deletes only free records).
  - Document `BusyBlockValue`/`FreeBlockValue` fields per Q5; drop
    per-block state per Q4.
- §8 — add CAS retry bound (B2); rewrite free as immediate in v1 (no
  batch, no timer) per B6; note R79 as the size-threshold batching
  follow-up; add graceful-shutdown drain+flush note (applies when R79
  ships).
- §9 — add the three-level note (C4); add Missing detection (B5).
- §10 — add epoch guard, Missing detection, fixed sync interval (10s)
  (B5); add initial zone record creation on disk-add (B10); note the
  future group-0 notify/watch follow-up.
- §11 — enumerate metrics (B8); note the disk-group-level usage
  summary piggybacked on keepalive (C3, in scope for v1).
- §12 — reference strategy 1 (full scan) as the scanner's rebuild
  mechanism.
- §14 — add free-side RCU lookup structures (B9).
- New § "Configuration" — consolidate tunables (B7).
- New sub-design doc for `JournalScan` crow-kv RPC (dependency for
  strategy 2) — or note it as a follow-up if you prefer to scope it
  separately.

Items that also touch backlog scope (R71 / R72):

- B5 (sync semantics) → R71; group-0 notify follow-up → R78.
- B6 (free batch) → R72 scope change (remove FreeBatch/FreeFlushLoop,
  free is immediate); R79 (size-threshold batching follow-up).
- B7 (configuration) → R71 / R72 (defaults already inline there).
- Strategy 2 (journal scan) → R73 (crash recovery).
- Strategy 3 (compaction) → R73 (snapshot compaction).

I will update the backlog notes only if you want — they are deleted
after merge, so the design doc is the long-term home.
