<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R20: Durable Flush on Graceful Shutdown — Design

## Problem

`PxLocalReplica::shutdown` (`crowkv/src/cluster/local_replica.rs:652`)
is a no-op. When the server receives SIGINT/SIGTERM, the shutdown
cascade stops the gRPC server, cancels background tasks, and closes
remote channels — but never flushes the engine's in-memory state to
disk.

For `CrowtreeEngine`, data lives in:
- **L0** (MemTable) — in-memory write buffer
- **L1** (B+tree) — in-memory, populated by `flush()`
- **Page store** (block files) — durable, populated only by `snapshot()`

`snapshot()` is called by the maintenance loop
(`group_maintenance.rs:run_pass`) only when slot/time thresholds are
met (`snapshot_slot_threshold`, `snapshot_time_threshold_ms`). If the
process exits before those thresholds fire, all in-memory state is
lost. The block file remains at 0 bytes. On restart, `resume_from_slot`
returns 0, forcing a full WAL replay.

## Current Shutdown Cascade

```
main.rs: graceful_shutdown(registry)
  └─ PxKvStore::shutdown(per_layer_timeout)
       ├─ 1. shutdown_server() — stop gRPC listener, join task
       └─ 2. for each group: PxGroup::shutdown(per_layer_timeout)
            ├─ 0. cancel tenure_cancel + await election driver + maintenance loop
            ├─ 1. for each remote: PxRemoteReplica::shutdown() — close gRPC channels
            └─ 2. PxLocalReplica::shutdown()  ← NO-OP (the gap)
```

## Proposed Approach

Wire `KVEngine::flush` + `KVEngine::persist_snapshot` into
`PxLocalReplica::shutdown`. The maintenance loop is already cancelled
(step 0 of `PxGroup::shutdown`) before the local replica shuts down,
eliminating concurrent flush/snapshot risk.

### Shutdown method changes

`PxLocalReplica::shutdown` will:

1. **Idempotency gate** — `shutdown_started` AtomicBool (already
   present).
2. **`engine.flush()`** — drain L0 memtable into L1 B+tree. Cheap,
   in-memory, always safe. No-op for `InMemKV`.
3. **`engine.persist_snapshot()`** — write dirty L1 pages + superblock
   to the page store. Synchronous FFI call. Returns the snapshot slot
   (0 if nothing persisted).
4. **Log** — `info` with snapshot slot when > 0; `debug` when 0
   (non-durable engine or no data).
5. **Error handling** — `persist_snapshot` returns `u64` (not
   `Result`), so errors are not directly reportable. The C++ side logs
   internally. If the snapshot slot is 0 but data was applied, log at
   `warn` as a potential durability issue.

### Why not wrap in timeout?

Both `flush()` and `persist_snapshot()` are synchronous FFI calls. The
Rust `per_layer_timeout` parameter uses `tokio::time::timeout` which
only works on async futures — it cannot cancel a synchronous FFI call
in progress. If the C++ `snapshot()` hangs, the process is already in
shutdown and the OS will eventually SIGKILL it. The `per_layer_timeout`
is retained for cascade uniformity but not used for engine calls.

### Why not call `run_pass`?

`group_maintenance::run_pass` does flush + conditional snapshot + GC +
WAL GC. Calling it during shutdown would:
- Re-check thresholds (we want unconditional snapshot on shutdown)
- Run GC (unnecessary during shutdown)
- Run WAL GC (unnecessary, and `group_safe_slot` may be 0)

Direct `flush()` + `persist_snapshot()` calls are cleaner and match the
shutdown contract (do the minimum needed for durability).

## Alternatives Considered

- **Flush-only (no snapshot)**: L1 is still in-memory; data is still
  lost on exit. Only `snapshot` writes to the page store. Rejected.
- **New `KVEngine::shutdown` trait method**: `flush` +
  `persist_snapshot` already cover the semantics. Rejected.
- **Background snapshot task on shutdown signal**: Adds complexity
  (task lifecycle, ordering with gRPC stop). The synchronous call is
  simpler and the cascade already ensures gRPC is stopped first.

## Acceptance Test Plan

1. **Block file non-zero after shutdown**: Start a server with block
   backend, write data, gracefully shut down, verify `*.blk-XXXX` file
   is non-zero.
2. **Data readable after restart**: Start server, write keys, shut
   down, restart with same data dir, read keys back — verify values
   match.
3. **`resume_from_slot` non-zero**: After shutdown+restart, verify
   `resume_from_slot` returns the snapshot slot (not 0).
4. **InMemKV unaffected**: `persist_snapshot` returns 0, no errors,
   shutdown report is clean.
5. **Idempotency**: Calling `shutdown` twice returns empty report on
   second call (existing behavior preserved).
