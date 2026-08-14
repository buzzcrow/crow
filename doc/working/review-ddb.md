<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW diskdb — Design + Code Review

Scope: full diskdb stack — `app/crow-diskdb` (server), `lib/crow-protocol`
(diskdb types/keys), `lib/crow-kv-client` (`HardwareClient`,
`ServiceRegistryClient`, `journal_scan`), `lib/crow-console-shared`
(`DiskEntry`), `app/crow-web` (disk/disk-group handlers), `app/crow-cli`
(disk commands), against the three design docs
(`doc/design/diskdb/design-crow-diskdb.md`,
`design-crow-diskdb-zone-management.md`,
`design-crow-diskdb-space-metrics.md`). Reviewed on `task-diskdb`
(14 commits ahead of origin). Easy/low-risk findings were fixed
directly (see "Fixed in this pass"); everything below is either a
design decision, a behavior change, or a larger implementation gap.

Verdict: the core model is sound — records-as-source-of-truth,
persist-only free, bitmap derived, two-phase allocate, watermark
compaction, three-strategy recovery all match the design and are
implemented carefully (verified against invariants I1–I10). The gaps
cluster in four areas: (1) disk-status management is only partially
implemented (no node/group effective status, no group-0 write-back,
no Suspect path); (2) console-side disk lifecycle is best-effort and
non-atomic; (3) the latency-hierarchy metrics are registered but not
observed; (4) several small robustness holes (now mostly fixed).

## Fixed in this pass

- `DiskIdExt::from_display_string` panicked on 32-byte non-ASCII input
  (byte 16 not a char boundary) — reachable from user-controlled URL
  path data. Now gated on `s.is_ascii()`; returns `Err` instead of
  panicking (`lib/crow-protocol/src/diskdb_type_util.rs`). Regression
  tests added in `lib/crow-protocol/tests/diskdb_types_test.rs`.
- Console-added disks were written to group 0 with `zone_count: 0` —
  the diskdb server derives its entire zone list from this field, so a
  production-added disk was created with **zero zones** and could never
  allocate (`app/crow-web/src/lifecycle.rs`). `http_add_disk` now
  computes `zone_count = capacity / zone_size` and rejects degenerate
  input (zero `unit_size`/`zone_size`, zone size not a multiple of unit
  size, capacity < zone size, malformed `disk_id`) with `400` before
  mutating config.
- GC-gap fallback was broken end-to-end: the server sends error code
  `KV_ERROR_JOURNAL_SCAN_GC_GAP` but `crow-kv-client` discarded the
  code and retried until `RetriesExhausted`, and diskdb matched the
  message text `"gc gap"` which never appears in the server's actual
  message (`"…slots GC'd"`). New `Error::JournalScanGcGap` variant,
  no-retry mapping in `journal_scan`, variant match in
  `recovery/journal_replay.rs`. Ghost-scan and recalc now correctly
  fall back to strategy 1 on GC gap.
- Init→Up race: a sync tick could flip a newly-added disk `Init → Up`
  while `background_zone_load` was still loading zones, making it
  allocatable with zero/partial zones. `reconcile_existing_disk` now
  skips status reconciliation for `Init` disks — the background load
  owns the `Init → target` transition (`liveness/keepalive.rs`).
- `compact_zone` RPC indexed `zones[zi]` unchecked against the
  in-memory zone vec (which can lag `disk_value.zone_count` while a
  disk loads) — potential panic. Now bounds-checked
  (`service/diskdb_service.rs`).
- `query_capacity_stats` zone-level path used `expect("disk exists")`
  — a disk removed between two reads would panic the server. Now a
  `not_found` error.
- Stale compact_ts watermark read in `compact_zone_inner` — the design
  requires re-reading `compact_ts` under the zone lock; the code used
  the pre-lock value, so concurrent compactions could regress
  `compact_ts` (watermark invariant I7). Re-read under the lock
  (`model/zone.rs`).
- Dead validation in `allocate_blocks` handler: `unit_count *
  unit_size % unit_size != 0` is always false (any multiple is
  divisible) — removed.
- `next_freed_ts` could wrap `u64::MAX → 0` (breaks monotonicity) —
  now saturating (`model/disk_group.rs`).
- Stale comments/messages in the free path: `alloc.rs` described the
  pre-persist-only "Phase 2: clear bitmap locally" design and logged
  "bitmap clear failed — ghost-busy" when the in-memory zone lookup
  failed. Rewritten to the actual persist-only contract.
- `health.rs` documented phase `recovering` — actual phases are
  `init / syncing / loading / up`.
- Duplicated `wait_for_disks_ready` (byte-identical in
  `diskdb_e2e_test.rs` and `recovery_test.rs`) moved to
  `tests/common/cluster.rs`.
- Dead scaffolds removed: `_arc_use` (`recovery/full_scan.rs`),
  `_assert_disk_metrics_used` (`metrics/reporting.rs`).
- Redundant double-attach of the CAS-retry metric in
  `background_zone_load`'s strategy-1-failed branch
  (`liveness/keepalive.rs`).
- Stale `testkit/console.rs` doc reference in
  `app/crow-cli/src/bench/provision.rs`.
- Restored runtime artifact `lib/crow-console-shared/conf/node-config.json`
  (was modified by a test run).
- Tracked config `app/crow-diskdb/conf/crow_diskdb_config.toml` was
  stale — old scanner field names (`detect_ghost_allocations`/
  `verify_record_integrity`), missing `[scanner.ghost]`,
  `[scanner.integrity]`, `reverify_delay_ms`, and `[notify]` — so the
  `tracked_config_file_loads_and_validates` guard test failed. Updated
  to the current schema.
- `scanner_test` — two e2e tests were broken (verified failing at
  `origin/task-diskdb` too): they injected drift into **active** zones,
  which both scanners correctly skip, so detection always returned 0;
  on this branch they also hit the R81 Init-load race (allocate before
  background zone load → `NoSpace`). Fixed by waiting for disks ready
  and targeting a non-active zone (6 zones, active set 4).
- `state_machine_test` — two stale tests asserted pre-R76 behavior:
  `Bad → Up` illegal (it is now a legal operator override) and zones
  marked `Bad` on disk transition (zones now follow the disk-level
  status; no per-zone marking). Updated to the current design.

## HIGH — need design decisions

- **Effective status ignores node and disk-group status.** Design §8:
  effective = `max(node, group, disk)`. The sync loop reads only the
  disk's status from group 0; `DdbDiskGroup.status` stays `Up` forever
  (`DdbDiskGroup::new`) and node status is never read.
  `HwStateMachine::effective_status()` and `transition_disk_group()`
  exist but are never called. Consequence: if the operator sets a node
  or disk-group to `Offline`/`Maintenance` in group 0, diskdb keeps
  allocating from those disks. Decide: read node/disk-group status in
  the sync tick and compute effective status (matching the reference
  impl), or explicitly drop the three-level rule from the design.
- **diskdb-detected status changes are not written back to group 0.**
  Design §8: "Any local status change (disk found, disk bad, disk
  added/removed) is written to group 0 first, then reflected locally."
  The code only mutates in-memory state
  (`status_machine.transition_disk`); `Missing`/`Bad` transitions from
  `reconcile_absent_disk` never reach group 0, so operators monitoring
  group 0 never see a disk go Bad. `HardwareClient` has
  `set_disk_status` (used only by the console move handler). Decide:
  write detected status changes back to group 0, or update the design
  to "diskdb reports status via keepalive only".
- **Disk move is non-atomic and can silently lose the disk**
  (`app/crow-web/src/lifecycle.rs::http_move_disk`). Step 5 copies
  records from old bind to new bind with no source lock (an allocate
  between `set Maintenance` and the copy is not copied); step 6
  remove-then-add each `warn!`-and-continue — a failed `add_disk` at
  the new placement leaves the disk in group 0 nowhere; step 7 always
  rewrites the console config regardless of group-0 success. Minimum
  fix: only update the console config when both group-0 ops succeeded,
  and return an error instead of committing on failure. Real fix needs
  a design (record copy protocol, ordering, idempotency).
- **Baseline `ZoneValue` records are never written during disk-add
  init.** Design §8: disk-add init writes baseline records (empty
  bitmap, `snapshot_slot = 0`) to the bound data group. The keepalive
  field comment claims it ("Optional DdbKvClient for writing baseline
  ZoneValue records during disk-add init") but no code path writes
  them — `background_zone_load` only *loads*. A fresh disk has no
  snapshot, so every restart replays its journal from slot 0 (with
  GC-gap risk on old groups) and the design's "snapshots exist →
  recover, else initialize" ownership-transfer discrimination is
  dead. Decide: implement the baseline batch_write, or update design +
  comment.
- **Concurrent compaction of the same zone can double-free.**
  Compaction sources — allocate-path `compact_fallback`,
  `CompactionEngine` periodic loop, `PreparatoryThread`, the
  `compact_zone` RPC — can target the same zone at once. The KV
  free-record scan runs unlocked (I9), so a second compaction can
  partition a stale record list: if the block was re-allocated after
  the first compaction's atomic batch deleted its free record, the
  second's `range_clear` clears a live allocation. The compact_ts
  re-read fix prevents watermark regression but not this overlap.
  Suggest a per-zone `AtomicBool` compaction-in-progress guard
  (`try`-set at `compact_zone` entry, cleared on exit) so the
  fallback paths skip a zone that is already being compacted.
- **`Suspect` path is unimplemented.** Design §8: `Up → Suspect` after
  3 missed syncs, `Suspect → Missing/Up/Offline`. The state machine
  knows the transitions but nothing drives them: `reconcile_absent_disk`
  goes `Up → Missing` on the first absence, and
  `HwStateMachine::check_suspect_timeout` is never called. Health
  probing (existence/size/read-write) is not implemented at all (v1 is
  config-driven disk lists). This may be an accepted v1 simplification
  — either implement the probe + Suspect drive, or mark the Suspect
  states as reserved in the design and remove `check_suspect_timeout`
  (currently dead).
- **Epoch/revision guard is missing.** Design §8: "An epoch/revision
  guard skips a sync response whose epoch ≤ current." No group-0 value
  carries a revision, and the sync loop applies every fresh scan
  unconditionally. Since each tick re-reads group 0 (and diskdb is a
  passive reader), the practical risk is low, but the design text and
  implementation diverge. Either implement revisions on the group-0
  maps or drop the sentence from the design.

## MEDIUM

- **Latency-hierarchy metrics are registered but never observed.**
  `DiskdbMetrics` registers `allocate.rpc/bitmap_scan/kv_persist`,
  `free.*`, `compaction.*`, `sync.read_group0`, `sync.apply_changes`,
  `recovery_duration_ms`, `allocate_errors_total`,
  `compaction_records_deleted_total` — only `sync_latency` and the
  scanner metrics are actually observed (verified by grep). The
  design's §6/§7 core deliverable (where time is spent, per layer) is
  effectively absent. Needs `Arc<DiskdbMetrics>` on `DiskdbService` +
  instrumentation in `alloc.rs`, `compaction.rs`, `recovery.rs`, and
  the keepalive sub-steps. Also note `free.bitmap_clear.latency_us` is
  a misnomer under persist-only free (rename to something like
  `free.persist.latency_us` when wiring).
- **`free_blocks` does not validate that all segments belong to the
  same disk-group.** The handler resolves one node from the first
  segment and persists the whole batch to that group's bind — a
  misbuilt request would write frees to the wrong group. Validate all
  `disk_id`s map to the resolved group and reject/404 otherwise.
- **`journal_replay::load_zone_inner` hardcodes `disk_group_id = 0`**
  on zones loaded from a snapshot (and strategy-1 fallback does the
  same via `DdbZone::new(..., 0, ...)`). The field is currently never
  read, so this is cosmetic — but a future reader (metrics, query
  paths) would see a wrong group id. Pass the real `disk_group_id`
  through the loaders.
- **`compact_zone` uses `get_applied_slot(...).unwrap_or(0)`** — on a
  read failure the snapshot is anchored at slot 0, so the next restart
  replays the whole journal (correct but slow). Log a warning on the
  failure path (full_scan.rs already does this; compaction.rs does
  not).
- **`unit_capacity_for_zone` casts `zone_size_units as u32`** for
  non-last zones — truncation if zone size exceeds `u32::MAX` units
  (~2^32 × unit_size, unrealistic at 1M units, but a config validation
  could rule it out explicitly).
- **`http_add_disk` silently defaults unknown `disk_type` strings to
  `BlockHdd`** — a typo'd type mislabels the disk in group 0. Consider
  rejecting unknown types with 400 (the CLI default is `"Hdd"` which
  maps fine).
- **`query_capacity_stats` disk-level and zone-level shapes return
  zeroed group-level aggregates** (`capacity_bytes: 0, busy_bytes: 0,
  free_bytes: 0, allocatable_disk_count: 0` on the wrapping
  `DiskGroupInfo`). Matches the design's "brief per-disk view" intent
  but is easy for a console to misread; populate the group aggregates
  or document the shape.
- **Test gap: no end-to-end coverage for the console add-disk path.**
  The `zone_count: 0` bug shipped because nothing asserts that a disk
  added via `http_add_disk` ends `Up` with `zone_count` zones and
  becomes allocatable. Add an e2e (web handler → group 0 → diskdb
  sync → allocate) once the move/console paths are stabilized.

## LOW / accepted

- `RwLock` `.unwrap()` on read/write — pervasive convention; only
  panics if a thread panics while holding (poisoning); accepted
  codebase-wide.
- `#![allow(clippy::must_use_candidate, missing_errors_doc,
  missing_panics_doc, match_same_arms)]` at crate root
  (`app/crow-diskdb/src/lib.rs`) — broad but pre-existing; prefer
  per-site allowances going forward.
- Per-allocate `cas_retry_limit` is a whole-call budget rather than a
  per-bit cap (design §11 wording). Behavior is a stricter bound; no
  change needed.
- `ImpactedBlocksGauge` lacks a `Debug` derive (review checklist) —
  trivial to add when next touched.
- `DETAILS_CAP = 256` on scanner details is arbitrary and not
  configurable; counts are always exact, only the per-block list is
  capped.
- Details in `recalc`/`ghost` are reported for zones skipped during
  active rotation windows (documented best-effort).
- CLI disk commands don't validate `disk_id` locally — the web handler
  now returns 400, which surfaces through the CLI error path.

## Verified good

- Persist-only free (I1/I3) — bitmap untouched on free, `used_count`
  never decremented by free, `rollback_allocate` is allocate-only (I8);
  stale comments fixed.
- Two-phase allocate with rollback on Phase-2 failure; compaction
  fallback on `NoSpace`.
- Compaction watermark (I7) — stale vs new partition, atomic
  snapshot+delete batch (I6), monotonic `compact_ts` (fixed stale read).
- Recovery: strategy 2 replay applies only `Put BusyBlockKey` (I10),
  GC-gap and CRC fallbacks now correctly routed via typed error.
- Scanner: ghost-busy vs uncompacted-lag classification, re-verify,
  auto-correct gating on fallback, integrity scan catches records
  `read_zone_records` skips.
- Recovery scan task: resume-from-progress with `NO_ZONE_COMPLETED`
  sentinel, cluster-aggregated impacted-blocks gauge, cancel-on-Up.
- Background-task runner: race-free stop flag, per-cycle error logging.
- Notify handler: subscription lifetime handled correctly, merge tasks
  aborted on stop.
- Keepalive piggyback (per-group usage summaries), reporting loop,
  recalc engine — all match the space-metrics design.
