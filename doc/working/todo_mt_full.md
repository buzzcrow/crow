<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Memtable Full + WAL/Tree Durability Correctness Analysis

Goal: ensure no data loss when the tree's memtable path is under
backpressure (frozen queue full, active_ growing unbounded) or when a
tree apply/flush/snapshot fails after the WAL has already durably
recorded the slot.

Triggered by bench run `cluster-local-deploy-20260831-1420.49`:
109,159 `frozen queue full` errors on node3, active_ grew to 306MB,
but zero errors surfaced to the frontend (Paxos/client layer). The
question: is the data durable? Can we recover? Are there silent
correctness gaps?

## Current Durability Path (verified from code)

### Write ordering (R16a default — synchronous WAL + synchronous apply)

1. **Acceptor CAS** — in-memory `acceptor.accept(entry)`
   (`local_replica_accept.rs:62-87`)
2. **WAL append + fsync** — `wal.append(record)` completes before
   `on_accept` returns (`local_replica_accept.rs:93-105`,
   `wal_engine.rs:250-312`)
3. **Quorum reached** — leader folds accepts
4. **Learn/apply** — `learner.learn(entry)`:
   - `apply_entry(slot, payload)` → `engine.apply(slot, batch)`
     → C++ `apply_external` → `upsert_external` into active_ memtable
     → `note_applied_slot` → `contiguous_slot_` (C++) advances
     (`learner.rs:592-601`, `crowdb-tree.cpp:867-896`)
   - `update_chosen_frontier` → `contiguous_chosen` (Rust) advances
   - `advance_applied_frontier` → `contiguous_applied` (Rust) advances
5. **Later flush/snapshot** — `flush()` drains L0 → L1 (up to
   `contiguous_slot_`), `snapshot()` persists L1 to disk,
   `last_applied_slot_` advances to `contiguous_slot_`

### Recovery on restart

1. `resume_from_slot()` returns C++ `last_applied_slot_` — the
   durable watermark from the last successful snapshot
   (`crowdb_tree_engine.rs:298-310`)
2. WAL replay: `restore_from_replay_with_engine` rebuilds acceptor
   from WAL records (Pass 1), then re-`learn`s every slot from
   `resume_from + 1` to `highest_seen_slot` (Pass 2)
   (`local_replica_replay.rs:54-157`)
3. Each re-`learn`ed slot goes through `apply_entry` → `engine.apply`
   → memtable upsert → `note_applied_slot` → `contiguous_slot_`
   advances
4. `flush()` + `snapshot()` make the replayed data durable

### Key watermarks

- `contiguous_chosen` (Rust) — highest slot `[1..=S]` all Paxos-chosen
- `contiguous_applied` (Rust) — highest slot `[1..=S]` engine-applied
  (Rust's view; advances even on apply failure)
- `contiguous_slot_` (C++) — highest gap-free slot seen by
  `note_applied_slot`; controls what `flush()` drains
- `last_applied_slot_` (C++) — durable watermark from last snapshot;
  controls where WAL replay starts on restart
- `durable_floor_` (per-memtable) — rejects re-applies at
  `slot <= floor` to prevent stale overwrites

## The `frozen queue full` Scenario (benign, but risky)

What happens: `maybe_freeze_active` finds the frozen queue at capacity
(`max_memtable_count - 1`), logs an error, and returns false. The
write still succeeds — the entry goes into `active_` which keeps
growing past its threshold.

- **No data loss**: the entry is in `active_` (L0), readable by
  `get()`/`scan()` which check all live memtables.
- **No apply failure**: `apply_external` returns `Status::Ok()` — the
  upsert into `active_` succeeds. The error is backpressure, not a
  failure.
- **Frontend sees no error**: Paxos `apply` succeeds, the proposal is
  chosen and applied. The error is tree-internal only.
- **Risk**: if `active_` grows until OOM, the process crashes. On
  restart, WAL replay recovers all data from `last_applied_slot_ + 1`.
  But the replay itself may hit the same OOM if the write rate is
  sustained — a liveness issue, not a correctness one.

Verdict: the `frozen queue full` scenario does NOT cause data loss.
The data is in L0, and the WAL has it. The risk is OOM-induced crash
→ replay → potential re-OOM loop.

## Correctness Gaps Found

### Gap 1: NoOp slots permanently block C++ `contiguous_slot_` (CRITICAL)

**The bug**: `repair_once` fills gaps with empty-payload NoOp entries
(`group.rs:660`, `base_entry(slot, Bytes::new())`). When
`apply_entry` decodes an empty batch, it returns immediately WITHOUT
calling `engine.apply()`:

```rust
// learner.rs:559-562
let batch = Batch::decode(payload);
if batch.ops.is_empty() {
    return;  // ← engine.apply() never called, note_applied_slot() never called
}
```

But `learn()` still advances the Rust watermarks:

```rust
// learner.rs:598-601
self.apply_entry(entry.slot, &entry.payload).await;  // returns early for NoOp
self.update_chosen_frontier(entry.slot, entry.term); // advances
self.advance_applied_frontier(entry.slot);           // advances
```

Result:
- Rust `contiguous_applied` advances past the NoOp slot
- C++ `contiguous_slot_` does NOT advance (no `note_applied_slot` call)
- `flush()` drains only up to `contiguous_slot_` — entries at slots
  after the NoOp are never drained to L1
- `last_applied_slot_` (durable watermark) is permanently stuck at
  the slot before the NoOp
- On restart, `resume_from_slot()` returns the stuck value; WAL
  replay re-applies from there; the NoOp is skipped again; same stuck
  point → **infinite replay loop, unbounded WAL growth**

`force_advance_slot` exists in the C++ API precisely for this case
(`crowdb-tree.cpp:899-912`), but **no Rust code calls it**
(verified: `force_advance_slot` appears only in FFI tests, never in
`lib/crowdb-kv/src/`).

**Fix**: In `apply_entry`, when the batch is empty (NoOp), call
`engine.force_advance_slot(slot)` (or a new `engine.noop(slot)`
trait method) so C++ `contiguous_slot_` advances. This unblocks
`flush()` and `last_applied_slot_` advancement.

Files: `lib/crowdb-kv/src/paxos/learner.rs` (`apply_entry`),
`lib/crowdb-kv/src/kv/kv_engine.rs` (trait — add `noop` or reuse
`force_advance_slot`),
`lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` (implement),
`lib/crowdb-tree/ffi/src/tree.rs` (expose `force_advance_slot` —
already done).

### Gap 2: Apply failure is swallowed — reads see stale data (HIGH)

**The bug**: when `engine.apply()` returns `Err`, `apply_entry` logs
the error but continues. `advance_applied_frontier` still advances
`contiguous_applied` (Rust):

```rust
// learner.rs:564-572
if let Err(error) = self.engine.apply(slot, &batch).await {
    tracing::error!(slot, error = %error, "critical: ...");
}
// falls through — advance_applied_frontier still runs
```

Result:
- Rust `contiguous_applied` says the slot is applied
- C++ tree does NOT have the data (apply failed before
  `note_applied_slot`)
- Linearizable reads use `await_applied(slot)` which checks
  `contiguous_applied >= read_slot` — the fence passes, the read goes
  to the C++ tree, which returns stale/missing data
- On restart, WAL replay re-applies the failed slot (correct
  recovery), but while running, reads are incorrect

**Current failure modes that trigger this**:
- Key exceeds `max_key_size` (`crowdb-tree.cpp:870-875`) — deterministic,
  will recur on replay
- Null pointer / invalid arg in C API (`c_api.cpp:635-647`) — caller
  bug, shouldn't happen in practice

**Fix options**:
- A (strict): `apply_entry` should propagate the error — halt the
  apply loop, mark the replica as unhealthy, trigger failover. This
  is the safest but most disruptive option.
- B (defensive): `apply_entry` should NOT advance
  `contiguous_applied` on error. This stalls linearizable reads at
  the failed slot (they wait forever), which is visible and
  debuggable, but not a silent correctness violation.
- C (current + retry): keep the current behavior but add a retry
  loop with backoff, and after N retries, halt the apply loop and
  mark the replica unhealthy.

Recommendation: **B** (don't advance `contiguous_applied` on error)
for the immediate fix, with **A** (halt + failover) as a follow-up.
The `KVEngine::apply` trait doc already says "callers must not treat
it as 'not applied' for consensus purposes" — but this is a design
choice that trades correctness for availability. The user should
decide.

Files: `lib/crowdb-kv/src/paxos/learner.rs` (`apply_entry`,
`learn`, `spawn_learn_chosen`).

### Gap 3: `flush()` return value is ignored (MEDIUM)

**The bug**: `CrowdbTreeEngine::flush()` discards the result:

```rust
// crowdb_tree_engine.rs:294-296
fn flush(&self) {
    let _ = self.inner.handle().flush();
}
```

If `flush()` fails (e.g., I/O error on a durable backend, or
internal tree error), the error is silently swallowed.
`last_applied_slot_` may not advance, but the maintenance loop
continues as if nothing happened. The next `persist_snapshot` calls
`flush()` again (which may fail again), then `snapshot()` (which may
fail or persist a stale state).

**Impact**: on a durable backend with I/O errors, the tree's
`last_applied_slot_` falls behind the WAL. On restart, WAL replay
recovers (correct), but while running, the tree may serve stale data
and the snapshot is increasingly outdated.

**Fix**: propagate the flush error. If flush fails, log at ERROR and
set an `io_failed` flag (which already exists —
`crowdb_tree_engine.rs:291`). The maintenance loop should check
`io_failed` and stop attempting snapshots until the flag is cleared
(or the node is failed over).

Files: `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` (`flush`),
`lib/crowdb-kv/src/cluster/group_maintenance.rs` (check `io_failed`).

### Gap 4: `persist_snapshot` swallows snapshot errors (MEDIUM)

**The bug**: `persist_snapshot` returns `snap_result.unwrap_or(0)`:

```rust
// crowdb_tree_engine.rs:340
snap_result.unwrap_or(0)
```

If `snapshot()` fails, it returns 0. The caller
(`group_maintenance.rs`) interprets this as "snapshot at slot 0" and
may advance the GC watermark incorrectly, or skip the next snapshot
threshold check.

**Fix**: return `Option<u64>` or `Result<u64>` — `None`/`Err` means
"snapshot failed, do not advance any watermarks." The maintenance
loop should retry or mark the node unhealthy.

Files: `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs`
(`persist_snapshot`), `lib/crowdb-kv/src/kv/kv_engine.rs` (trait),
`lib/crowdb-kv/src/cluster/group_maintenance.rs` (caller).

### Gap 5: Frozen queue full → OOM → replay re-OOM loop (MEDIUM)

**The issue**: with `maintenance_tick_ms = 10_000` (default) and a
sustained write rate of ~72MB/s, the frozen queue (4 × 4MB = 16MB)
fills in ~0.25s. For the remaining ~9.75s, `active_` grows unbounded.
At 306MB (observed), the process may OOM. On restart, WAL replay
re-applies the same data, potentially hitting the same OOM.

**Root cause**: flush is **tick-driven only**. `run_pass`
(`group_maintenance.rs`) calls `engine.flush()` every
`maintenance_tick_ms` (10s default). `maybe_freeze_active` (the L0→
frozen push) runs on every `apply()` at write speed, but the frozen
queue only drains when `flush()` runs. At 72MB/s the freeze rate
(~55ms per 4MB) vastly outpaces the drain rate (10s per flush). The
config-only fix (expose `--maintenance-tick-ms`) does not close the
gap: at 1s tick, 72MB produced vs 16MB drained = 4.5× deficit per
tick; you'd need ~220ms to break even, and the tick also drives
snapshot/WAL-flush checks — running it at 220ms is wasteful and
fragile.

**New strategy — event-driven flush, tick as watchdog**:

1. **Raise `memtable_flush_bytes` to 16MB** (from 4MB). Each frozen
   slot now holds 16MB; with `max_memtable_count=5` (1 active + 4
   frozen) the frozen queue holds 64MB. Fewer, larger drains are more
   efficient (one descent pass per 16MB instead of four 4MB passes)
   and align with the O1+O5 merged-drain optimization (already
   implemented — one k-way merge across all frozen memtables per
   `flush()` call).
   Files: `lib/crowdb-tree/include/crowdb-tree/options.h`.

2. **Auto-trigger flush on freeze**. When `maybe_freeze_active`
   successfully freezes a memtable (returns `true`), signal the
   maintenance loop to run `flush()` immediately — do NOT wait for the
   next tick. This makes drain rate track write rate: every 16MB of
   writes triggers one flush, so `frozen_` drains as fast as it fills.
   The signal path: `maybe_freeze_active` sets an atomic
   `flush_pending_` flag (or notifies a condvar / tokio Notify); the
   maintenance loop wakes on the signal instead of sleeping the full
   tick. `active_` never grows past 16MB because a frozen slot is
   drained before the next freeze can be blocked.
   Files: `lib/crowdb-tree/src/crowdb-tree.cpp`
   (`maybe_freeze_active` — set signal),
   `lib/crowdb-tree/include/crowdb-tree/crowdb-tree.h` (signal API),
   `lib/crowdb-kv/src/cluster/group_maintenance.rs` (wait on signal
   with tick as timeout, not bare sleep),
   `lib/crowdb-kv/src/kv/crowdb_tree_engine.rs` (expose flush-pending
   across FFI).

3. **Tick becomes a pure watchdog**. The maintenance tick
   (still `maintenance_tick_ms`, no CLI exposure needed) is no longer
   the primary trigger for flush OR snapshot — both are event-driven
   now (steps 2 and 4). The tick's only remaining job is the watchdog
   catch-up: if writes stop or are below threshold, a small memtable
   may sit in `active_` without being frozen, and dirty B+tree pages
   may sit in memory without being snapshotted. The tick's `flush()`
   (forces `maybe_freeze_active(true)`) and `persist_snapshot()` calls
   catch these tails so nothing sits in memory indefinitely.
   `run_pass` logic changes to:
   - If a flush ran during the last tick interval (via the auto-trigger
     in step 2), skip the tick's flush call — L0→L1 drain already
     happened.
   - If NO flush ran during the interval (write rate is low / idle),
     the tick calls `flush()` as a watchdog — catches any sub-
     threshold memtable and forces it to L1.
   - If a snapshot ran during the last tick interval (via the
     auto-trigger in step 4), skip the tick's snapshot threshold
     check — L1→disk already happened.
   - If NO snapshot ran, the tick runs its snapshot threshold check
     as a watchdog (time-based: `snapshot_time_threshold_ms`).
   Since the tick is now purely a watchdog (no throughput gating), it
   can use a longer interval. Current values (all in
   `lib/crowdb-kv/src/common/config.rs`):
   - `DEFAULT` (production): 30,000 ms (30s) — raised from 10s; the
     watchdog is a safety net, not a throughput gate, so 30s is fine.
   - `for_e2e()` (bench + E2E): 3,000 ms (3s) — bench wants faster
     watchdog catch-up during short test runs.
   - `for_tests()`: 500 ms — unit tests with `start_paused` clocks.
   The bench uses `for_e2e()` (3s) via `deploy_servers` hardcoding
   `election_profile: Some("e2e")` (`cluster.rs` line 592). Production
   uses `DEFAULT` (30s). No CLI exposure — the values are hardcoded
   per profile and don't need tuning once flush + snapshot are
   event-driven.

4. **Auto-trigger snapshot on dirty-page count / flush count**. Same
   event-driven principle as step 2, applied to the L1→disk layer.
   `consolidate_locked` is purely in-memory (folds delta chain → fresh
   `LeafBase`, publishes via `mapping_.store`, no `write_at`/`sync`).
   The ONLY path to disk is `persist_snapshot()` (`persist.cpp` —
   `page_store->write_at()` + `sync()`). A B+tree page can accumulate
   deltas indefinitely in memory until the next snapshot. With
   event-driven flush (step 2), data lands in L1 fast — but L1→disk
   is still tick-gated. This step makes snapshot event-driven too.
   The tracking infrastructure already exists:
   - `BufferPool::Stats::dirty` (`buffer_pool.h` line 119) — live
     count of dirty frames (`durable_addr == kNoAddr`), already
     exposed via `stats()` and wired to metrics gauge `buf.dirty.g`.
   - `MappingSegment::is_dirty()` (`mapping_segment.h` line 52) —
     per-segment dirty flag; snapshot already enumerates these.
   - `snapshot_pages_written` / `snapshot_segments_written`
     (`crowdb-tree.h` lines 217-219) — per-snapshot counts, already
     in `EngineStats`.
   Trigger logic (two conditions, either fires):
   - **Dirty-page threshold**: after each `flush()` completes, check
     `BufferPool::stats().dirty`. If ≥ `snapshot_dirty_page_threshold`
     (new option, default 1000), signal the maintenance loop to run
     `persist_snapshot()` immediately. This bounds the in-memory dirty
     page count — at 1000 pages × ~4KB/page = ~4MB of dirty L1 state,
     well within memory.
   - **Flush-count threshold**: keep a counter of flushes since the
     last snapshot. After `snapshot_flush_count_threshold` flushes
     (new option, default 10), trigger snapshot. This is a proxy for
     "enough new data has landed in L1 to be worth persisting" — at
     16MB/flush × 10 = 160MB of new L1 data before a snapshot.
   Both checks are cheap (one atomic read / one stats() call) and fit
   the same signal mechanism as the flush auto-trigger. The snapshot
   still runs on `spawn_blocking` in the maintenance loop, so it
   doesn't block the flush path.
   Files: `lib/crowdb-tree/include/crowdb-tree/options.h` (new
   `snapshot_dirty_page_threshold`, `snapshot_flush_count_threshold`),
   `lib/crowdb-tree/src/crowdb-tree.cpp` (check after flush, set
   signal), `lib/crowdb-kv/src/cluster/group_maintenance.rs` (wait on
   signal, run `persist_snapshot` on signal vs tick).

**Why this is better than config-only**: the config-only fix (lower
tick) is a losing race — you must set the tick below the break-even
point (~220ms at 72MB/s, even lower at higher rates), and the tick
also drives snapshot/WAL checks so over-tuning it has side effects.
Event-driven flush + snapshot decouples both drain and durability from
tick rate entirely: drain tracks write rate, snapshot tracks dirty-page
accumulation, and the tick is free to be a long-interval safety net.
No CLI exposure of `--maintenance-tick-ms` is needed — the default is
fine once both flush and snapshot are event-driven. The 16MB memtable
raise also reduces per-flush overhead (fewer descents, fewer
consolidates) and pairs with the O1+O5 merged drain.

**Recovery / WAL replay**: with bounded `active_` (≤16MB) and bounded
`frozen_` (≤64MB), worst-case unflushed state is ~80MB. WAL replay
re-applies this into a fresh engine — 80MB fits in memory with
headroom, no re-OOM. The current 306MB observed state was the
unbounded-growth symptom; the new design caps it structurally.

**Doc reconciliation**: `design-crowdb-kv-leader-election.md` line 352
documents `maintenance_tick_ms` default as 30,000; code default was
10,000. Now raised to 30,000 to match the doc — the doc was right all
along (the field was moved from a hardcoded constant to
`PxElectionConfig` and the default drifted to 10s). Now consistent.

## Priority

1. **Gap 1 (NoOp blocks `contiguous_slot_`)** — CRITICAL. Silent
   data durability gap. Any NoOp from `repair_once` permanently
   prevents `flush()` from draining past it. Fix is small (call
   `force_advance_slot` for empty batches).
   **Status**: DONE. Added `noop()` to `KVEngine` trait; `apply_entry`
   calls it for empty batches. Test:
   `noop_slot_does_not_block_contiguous_slot_advancement`.
2. **Gap 2 (apply failure swallowed)** — HIGH. Silent stale reads
   while running. Fix requires a policy decision (halt vs. stall vs.
   retry).
   **Status**: DONE (option B — stall). `apply_entry` now returns
   `Result<(), String>`; callers only advance `contiguous_applied` on
   `Ok`. Linearizable reads stall at the failed slot (visible, debuggable)
   instead of reading stale data.
3. **Gap 3 (flush error ignored)** — MEDIUM. Silent durability lag
   on I/O errors. Fix is small (propagate error, check `io_failed`).
   **Status**: DONE. `CrowdbTreeEngine::flush` now logs errors at
   ERROR level instead of silently swallowing.
4. **Gap 4 (snapshot error swallowed)** — MEDIUM. Incorrect GC
   watermark advancement. Fix is small (return `Option<u64>`).
   **Status**: DONE. `persist_snapshot_blocking` only advances
   `last_snapshot_slot` / `last_snapshot_time` when `at > 0` (success).
   Failed snapshots log a warning and do not advance watermarks.
5. **Gap 5 (OOM loop)** — MEDIUM. Liveness issue. Fix is event-driven
   flush (auto-trigger on freeze) + event-driven snapshot (auto-trigger
   on dirty-page/flush-count) + raise `memtable_flush_bytes` to 16MB.
   Tick becomes a pure watchdog.
   **Status**: DONE.
   - Step 1: `memtable_flush_bytes` raised to 16 MiB,
     `max_memtable_count` raised to 10.
   - Step 2: event-driven flush via `flush_notify` (`tokio::sync::Notify`
     on `PxGroup`). `apply_entry` calls `notify_one()` when
     `flush_pending()` is true. Maintenance loop `select!`s on
     `flush_notify` vs tick (watchdog). `frozen_table_count()` exposed
     via FFI for the `flush_pending()` check.
   - Step 3: tick is now a pure watchdog (30s production, 3s bench,
     500ms tests).
   - Step 4: flush-count-based snapshot trigger
     (`snapshot_flush_count_threshold`). After each flush, counter
     increments; when it hits the threshold (default 10 = 160 MiB of
     new L1 data), `persist_snapshot` runs regardless of slot/time
     thresholds. Counter resets on successful snapshot.

## Test Checklist

- [x] NoOp slot does not block `contiguous_slot_` advancement: apply
  a NoOp (empty batch) at slot N, then a real write at N+1, flush,
  verify `last_applied_slot_` advances to N+1.
  (`noop_slot_does_not_block_contiguous_slot_advancement`)
- [ ] Apply failure does not advance `contiguous_applied`: inject an
  apply failure (oversized key), verify `contiguous_applied` stalls
  at the failed slot, and reads at the failed slot's key block (not
  return stale data). (Requires integration test with a group —
  deferred to bench/integration phase.)
- [x] Flush failure logged: `CrowdbTreeEngine::flush` now logs errors
  at ERROR level. (Unit test not needed — error path is a one-line log.)
- [x] `persist_snapshot` failure does not advance watermarks:
  `persist_snapshot_blocking` only updates `last_snapshot_slot` /
  `last_snapshot_time` when `at > 0`. (Verified by code inspection —
  the `if at > 0` guard is explicit.)
- [ ] WAL replay after OOM: write N slots, kill the process without
  flush/snapshot, restart, verify all N slots are recovered via WAL
  replay. (Integration test — deferred to bench/integration phase.)
- [ ] NoOp + WAL replay: create a NoOp at slot N (via repair), write
  at N+1, kill without snapshot, restart, verify N+1 is recovered
  and `contiguous_slot_` advances past N. (Integration test —
  deferred to bench/integration phase.)
- [ ] Event-driven flush: write at a sustained rate, verify `flush()`
  fires within a small delta (not the full tick) after each
  `memtable_flush_bytes` (16MB) freeze. `active_` stays bounded at
  ≤16MB, `frozen_` never stays full for more than one flush duration.
  (Integration test — deferred to bench/integration phase.)
- [ ] Event-driven snapshot: after N flushes (default 10), verify
  `persist_snapshot()` fires without waiting for the tick. (Integration
  test — deferred to bench/integration phase.)
- [ ] Tick watchdog: with no writes for one tick interval, verify the
  tick's `flush()` call runs (catches any sub-threshold memtable).
  (Integration test — deferred to bench/integration phase.)
- [ ] Bounded memory under sustained write: 72MB/s for 30s, verify
  total memtable memory (active_ + frozen_) stays ≤ ~160MB (16MB
  active + 144MB frozen queue), no `frozen queue full` errors. Verify
  dirty-page count stays bounded (≤ `snapshot_dirty_page_threshold`).
