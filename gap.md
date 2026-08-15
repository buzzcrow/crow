<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Gaps and Open Questions — R99 + R100

Issues that need user input before they can be resolved. Each item lists
the question, the alternatives, the approach taken in this implementation
(marked **Decision taken**), and why it may need revisiting.

---

## R99: Dynamic Range Binding Framework + chunkdb Instance Sharding

### GAP-R99-1: Common framework or separate implementations?

**Question:** Should chunkdb instance sharding and diskdb disk-group
binding share one common framework, or be two separate implementations?

**Alternatives:**
- (a) One common framework with pluggable algorithms (range-based for
  chunkdb, table-based for diskdb) — more upfront work, less duplication.
- (b) Two separate implementations sharing the group-0 binding schema
  pattern but not code — simpler, some duplication.

**Decision taken:** (a) for the binding client + schema (shared in
`crow-kv-client`), (b) for the binding monitor — chunkdb gets a monitor
now; diskdb migration is a follow-up requirement (GAP-R99-5). The
framework is designed to be reusable, but diskdb's monitor is not
implemented in R99.

**Needs input if:** you want diskdb migration in R99's scope rather than
a follow-up.

### GAP-R99-2: Where does the framework library live?

**Question:** Which crate hosts the binding client and binding monitor?

**Alternatives:**
- (a) Binding client in `crow-kv-client` (already the sysdata API
  surface), binding monitor in `crow-kv-server` (group-0 leader).
- (b) `crow-common` — wrong layer (low-level primitives).
- (c) A new `crow-binding` crate — clean but premature.

**Decision taken:** (a) — `RangeBindingClient` in `crow-kv-client`,
`BindingMonitor` logic in `crow-chunkdb` (as a library module) with
wiring into `crow-kv-server` deferred (see GAP-R99-6).

**Needs input if:** you prefer a separate `crow-binding` crate.

### GAP-R99-3: Range assignment algorithm

**Question:** Consistent hashing or explicit bucket ranges?

**Alternatives:**
- (a) Consistent hashing — minimal data movement, implicit ranges.
- (b) Explicit bucket ranges — simple, matches R88's design §5.4a.

**Decision taken:** (b) — explicit bucket ranges, matching the existing
`BucketBinding` pattern in `routing.rs`. Ranges are contiguous bucket
intervals `[start, end)` over 0-65535.

**Needs input if:** you want consistent hashing for minimal migration.

### GAP-R99-4: Binding monitor location

**Question:** Where does the dynamic binding monitor run?

**Alternatives:**
- (a) Background task in the `crow-kv-server` group-0 leader.
- (b) A separate `crow-binding-manager` service.

**Decision taken:** (a) — the monitor logic is implemented as a library
(`BindingMonitor` in `crow-chunkdb`), but the actual wiring into the
`crow-kv-server` group-0 leader is deferred (GAP-R99-6). The operator
can manually write the binding table via `RangeBindingClient` until the
monitor is wired.

**Needs input if:** you prefer a separate service.

### GAP-R99-5: diskdb migration in R99 or a follow-up?

**Question:** Should diskdb disk-group rebinding (item 6) be in R99 or
a separate requirement?

**Decision taken:** Follow-up — R99 lands the framework + chunkdb
sharding. diskdb migration reuses the framework in a future R-item.

**Needs input if:** you want diskdb migration in R99.

### GAP-R99-6: Binding monitor wiring into crow-kv-server

**Question:** How does the `BindingMonitor` background task get spawned
in the `crow-kv-server` group-0 leader?

**Status:** The `BindingMonitor` logic (monitor service registry, compute
range assignment, write binding table) is implemented as a library
module in `crow-chunkdb`. The actual wiring into `crow-kv-server`'s
group-0 leader startup sequence is **not done** — it requires
understanding `crow-kv-server`'s background task architecture (how
sysdata management loops are spawned, how leader identity is
determined). This is left as a gap.

**Needs input:** Confirm that the monitor should run in the group-0
leader, and provide guidance on where to wire it in
`crow-kv-server`'s startup.

### GAP-R99-7: NotMyRange error — proto or gRPC status detail?

**Question:** Should `NotMyRange` be a new `ErrorCode` enum value, or a
gRPC status with custom details?

**Decision taken:** New `ErrorCode` value (`ERROR_CODE_NOT_MY_RANGE`)
plus a `NotMyRangeHint` detail message carrying the current owner's
range + endpoint, attached as gRPC status details (following the
`NotLeaderHint` pattern). The client extracts the hint, refreshes its
binding cache, and retries against the correct instance.

**Needs input if:** you prefer a different error encoding.

### GAP-R99-8: Range migration — full implementation or stub?

**Question:** Should R99 implement the full range migration flow
(`ChunkdbRangeMigrationValue` with Copying/Cutover/Complete states), or
stub it for a follow-up?

**Decision taken:** The migration state proto is defined, but the full
migration flow (dual-serve during cutover, background copy task) is
**stubbed** — the `MigrationTask` in `migration.rs` already handles KV
group migration; chunkdb instance migration is a different concern
(moving which instance serves a range, not which KV group stores the
data). The instance migration flow is left as a gap. The binding table
can be updated atomically (old instance rejects, new instance accepts)
without a dual-serve phase if the KV group routing is unchanged — only
the serving instance changes.

**Needs input:** Confirm that atomic cutover (no dual-serve) is
acceptable for v1, or if a full Copying/Cutover/Complete flow is needed.

---

## R100: Per-Chunk-ID Lifecycle Lock + Chunk Cache

### GAP-R100-1: Lock hold time during diskdb network RPCs

**Question:** Should diskdb RPCs (`allocate_strip`, `commit_blocks`,
`free_blocks`) happen inside or outside the per-chunk lock?

**Alternatives:**
- (a) Keep all diskdb calls inside the lock — correct; guards the entire
  RMW + commit/free cycle. Acceptable if same-chunk concurrency is rare.
- (b) Release the lock after `put_chunk`, before commit/free — halves
  lock hold time but risks concurrent delete freeing uncommitted blocks.

**Decision taken:** (a) — keep all diskdb calls inside the lock. v1
same-chunk concurrency is expected to be rare, and (a) is simpler to
reason about. Revisit (b) if `LockTimeout` becomes frequent.

**Needs input if:** you want (b) for higher concurrency throughput.

### GAP-R100-2: reap_idle cadence and trigger

**Question:** Should `reap_idle` run as a periodic background task or
opportunistically on every Nth acquire?

**Decision taken:** Periodic background task, 60s interval. chunkdb
already runs background tasks (topology refresh), so this is idiomatic.

**Needs input if:** you prefer opportunistic reap.

### GAP-R100-3: Cache capacity default

**Question:** What should the default `quick_cache` capacity be?

**Decision taken:** 100_000 entries (configurable via
`lifecycle.cache_capacity` in `crow-chunkdb.toml`). At ~1-2 KB per
`Chunk`, this is ~100-200 MB.

**Needs input if:** you want a different default or memory-derived
capacity.

### GAP-R100-4: quick-cache version pin

**Question:** Which `quick-cache` version to use?

**Decision taken:** `quick-cache = "0.7"` (the version cited in the R100
backlog doc). Will verify the published version is at least 7 days old
at add time per CROW dep policy.

**Needs input if:** you want a different version.
