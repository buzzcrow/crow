# CrowKV WAL Follow-up Plan

Depends on: [`requirement.md`](requirement.md), [`design/design-wal.md`](design/design-wal.md), [`design/design-parallel-slots.md`](design/design-parallel-slots.md), [`design/design-leader-election.md`](design/design-leader-election.md), [`bug-wal.md`](bug-wal.md)

This plan tracks the follow-up work created by resolving the `ai-todo` review notes in [`design/design-wal.md`](design/design-wal.md). The old WAL milestone plan was completed and deleted; this is the new implementation plan for the remaining WAL correctness and flush-pipeline changes.

## 1. Current State

- `WalEngine` writes directly in `append`, selects disk by round-robin, and calls `fdatasync` inline.
- `fsync_worker.rs` exists but is not wired into `WalEngine`'s append path.
- `WalConfig` still exposes `wal_fsync_batch_bytes`, `wal_fsync_batch_interval_ms`, and `wal_fsync_watchdog_ms`.
- `restore_from_replay` restores acceptor state and then calls `learn()` for every accepted slot, which can apply locally accepted but not quorum-chosen values.
- `run_bulk_phase1` already computes a quorum-derived ceiling, fills empty slots with `NoOp`, and moves `next_slot` past the recovery range.
- Open restart-recovery bugs are tracked in `bug-wal.md`: web-layer read routing resurrection, election up-to-date based on learner watermark, and unbounded / stale recovery floor without a durable commit watermark.

## 2. Target Decisions

- Slot-addressed WAL records use deterministic slot affinity: `disk = hash(group_id, slot) % wal_disks.len()`.
- Metadata records (`VoteGranted`, `DedupCheckpoint`, future `ConfigChange`) use the group metadata lane: `hash(group_id, 0) % wal_disks.len()`.
- Segment physical order is append order only; it is not a contiguous slot range.
- Durable flush is backend-neutral: filesystem `fdatasync`, block-device aligned writes, or backend-specific persistence for RAM / SCM / simulated backends.
- Default flush behavior is event-driven wake-drain-flush with no fixed 1 ms latency floor.
- Replay restores acceptor/election/dedup state; learner application requires durable commit evidence, snapshot evidence, or new-leader / heartbeat re-learning.
- Empty slots in the recovery interval become chosen `NoOp` slots.

## 3. Work Items

### W1 — Rename WAL flush config and defaults

- [ ] Replace `wal_fsync_batch_bytes` with `wal_flush_batch_bytes`.
- [ ] Replace `wal_fsync_batch_interval_ms` with `wal_flush_coalesce_us`.
- [ ] Replace `wal_fsync_watchdog_ms` with `wal_flush_watchdog_ms`.
- [ ] Default `wal_flush_coalesce_us = 0` for wake-drain-flush behavior.
- [ ] Update code, tests, and docs using old names.

**Tests:**

- [ ] Config default test verifies new field names and defaults.
- [ ] No code path references old fsync interval field names.

### W2 — Slot-affinity WAL placement

- [ ] Replace `WalEngine::select_pipeline()` round-robin with deterministic selection from the `WALRecord`.
- [ ] Slot records (`slot != 0`) hash `(group_id, slot)`.
- [ ] Metadata records (`slot == 0`) hash `(group_id, 0)`.
- [ ] Remove `rr_counter` if no longer needed.
- [ ] Keep `SegmentIndex` able to store multiple records per slot or explicitly document highest-location overwrite as cache-only.

**Tests:**

- [ ] Multiple appends for the same slot always land on the same disk.
- [ ] Adjacent slots distribute across multiple disks.
- [ ] Replay succeeds with non-contiguous slot ranges per segment.

### W3 — Wire event-driven flush workers into `WalEngine`

- [ ] Replace inline write + inline `fdatasync` in `WalEngine::append` with per-pipeline pending queues.
- [ ] Worker waits when empty and wakes on enqueue.
- [ ] Worker drains immediately-ready records up to `wal_flush_batch_bytes`.
- [ ] Optional coalescing uses microsecond budget only when configured.
- [ ] Watchdog remains as a safety path.
- [ ] Record futures resolve only after backend durable flush succeeds.
- [ ] Errors mark the WAL failed and fail queued records.

**Tests:**

- [ ] Single record completes without waiting for millisecond interval.
- [ ] Burst records coalesce into fewer durable flushes than records.
- [ ] Worker error fails all records in the batch and marks WAL failed.
- [ ] Append after failed WAL returns error.

### W4 — Backend-neutral durable flush and alignment

- [ ] Introduce a `WalPipelineBackend` append / durable-flush API used by `WalEngine` workers.
- [ ] Add human-readable UTF-8 line codec for `WALRecord`: one record per line, stable field names, escaped/base64 payload, and text decode for debugging/tests.
- [ ] Add a WAL record format selector so file/test backends may use the text line codec while block backends use the binary framed codec.
- [ ] File backend writes bytes then runs filesystem durable flush.
- [ ] Block backend routes through alignment planner and direct / aligned write semantics.
- [ ] Sim backend exposes deterministic counters for write count, durable flush count, and bytes.
- [ ] Update log messages and docs from `fsync` to durable flush except where specifically describing filesystem behavior.

**Tests:**

- [ ] Text codec round-trips every `RecordType`, including non-UTF-8 payload bytes.
- [ ] File backend durable flush called once per drained batch.
- [ ] Block backend respects 4 KiB alignment / RMW planning.
- [ ] Sim backend can assert batching behavior without real disk.

### W5 — Replay-only restore for uncommitted accepted values

- [ ] Change `PxLocalReplica::restore_from_replay` to restore `Promised` / `Accepted` into acceptor state without calling `learn()` for arbitrary accepted slots.
- [ ] Preserve `current_term`, `voted_for`, role, and dedup restoration.
- [ ] Keep a future hook for durable commit watermark / snapshot-covered apply.
- [ ] Update tests that currently expect restore to warm the KV engine from accepted records.

**Tests:**

- [ ] Locally accepted but unchosen value is not visible in `KVEngine` immediately after restore.
- [ ] Restored accepted value can still be adopted by Phase 1 and chosen.
- [ ] Dedup cache survives replay independently of learner apply.

### W6 — Durable commit watermark (P2)

- [ ] Add WAL record payload for the durable committed prefix / safe local chosen prefix.
- [ ] Persist watermark only after local learner has applied the contiguous prefix.
- [ ] Replay exposes `durable_commit_watermark` in `ReplayResult`.
- [ ] Restore may apply only slots covered by this watermark, or seed learner state from snapshot once P3 exists.
- [ ] Use watermark as the correct recovery floor to avoid full floor=0 sweeps.

**Tests:**

- [ ] Crash/restart applies only watermark-covered slots.
- [ ] Slots above watermark are recovered by bulk Phase 1 / heartbeat catch-up.
- [ ] Restart recovery no longer needs to over-claim `contiguous_chosen` from local accepts.

### W7 — Election up-to-date check from durable acceptor log tip

- [ ] Add `PxAcceptor` accessor for highest accepted slot and that slot's term.
- [ ] Change vote request payload to advertise accepted-log tip instead of learner `last_chosen_slot` / `last_chosen_term`.
- [ ] Change `candidate_log_up_to_date` to compare durable accepted log tip.
- [ ] Revisit `note_chosen` behavior after the accepted-tip check is in place.

**Tests:**

- [ ] Re-enable and pass `crowkv-server` dynamic replica test currently ignored.
- [ ] Election rejects candidates missing a higher accepted log tip.
- [ ] Election allows candidates whose accepted log tip is up to date even if learner watermark lags.

### W8 — Web restart read-routing hardening

- [ ] Investigate `crowkv-web` post-restart leader / endpoint resolution for per-store ephemeral gRPC ports.
- [ ] Force monitor-cache refresh after server restart before KV reads are forwarded.
- [ ] Gate linearizable reads until the target group reports a stable healthy leader.
- [ ] Retry `NotLeader` with refreshed topology rather than stale cached endpoint.

**Tests:**

- [ ] Re-enable and pass `crowkv-web` cluster restart recovery test currently ignored.
- [ ] Full restart with deleted keys verifies no stale leader / endpoint read resurrection.

### W9 — ConfigChange and snapshot integration (P3/P4 prep)

- [ ] Persist future `ConfigChange` records on the metadata lane.
- [ ] Wire `SnapshotMarker` and snapshot slot into replay result.
- [ ] Wire `compute_gc_slot` to real snapshot slot instead of stubbed value.
- [ ] Health-gate repair / bulk Phase 1 on reachable configured quorum once config persistence exists.

**Tests:**

- [ ] Snapshot-covered WAL prefix can be GC'd safely.
- [ ] Replayed config membership matches pre-restart group membership.
- [ ] Repair refuses to proceed without reachable configured quorum.

## 4. Implementation Order

1. W1 config rename.
2. W2 slot-affinity placement.
3. W3/W4 event-driven backend-neutral flush pipeline.
4. W5 replay-only restore.
5. W6 durable commit watermark.
6. W7 election accepted-tip fix and re-enable dynamic-replica test.
7. W8 web restart read-routing fix and re-enable web restart test.
8. W9 snapshot / config-change persistence prep.

## 5. Validation Commands

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test -p crowkv --test wal
cargo test -p crowkv --test cluster full_cluster_restart_keeps_deletes
cargo test -p crowkv-server --test cluster_e2e_test -- --ignored
cargo test -p crowkv-web --test cluster_restart_recovery_test -- --ignored
```

Run ignored tests only when working on their corresponding fixes. Do not un-ignore them until they pass in the normal suite.

## 6. Decision Log

- `Accepted` remains the only durable copy of the operation value; no separate KV op-log is introduced.
- Physical segment slot ranges are metadata only and may contain holes.
- Slot affinity is required for same-slot locality; per-record round-robin is no longer allowed.
- Wake-drain-flush replaces fixed 1 ms batching as the default latency policy.
- Restore must not treat one replica's accepted value as chosen.
