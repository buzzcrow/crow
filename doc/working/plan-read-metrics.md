<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R19 Plan — Read Performance Profiling and Metrics

## Task Breakdown

- [ ] T1: Add `ReadRegistryHandles` struct + registration on `PxGroup`
- [ ] T2: Instrument `linearizable_read_barrier` (barrier latency, path
      counters, lease-valid gauge)
- [ ] T3: Instrument `resolve_read_point` (MinSlot-fallback counter,
      contiguous-applied + safe-slot gauges)
- [ ] T4: Instrument `kv_get` engine_get timing
- [ ] T5: Add read bandwidth + forward counters to `KvMetrics` and
      instrument `get` / `scan` handlers
- [ ] T6: Write integration tests
- [ ] T7: Lint + relevant tests pass

## File-Level Changes

- `crowkv/src/cluster/group.rs`
  - Add `ReadRegistryHandles` struct (fields: `lease_path`,
    `readindex_path`, `minslot_fallback` counters; `barrier`,
    `engine_get` summaries; `lease_valid`, `contiguous_applied`,
    `safe_slot` gauges).
  - Add `read_handles: OnceLock<ReadRegistryHandles>` field on
    `PxGroup`.
  - Register read handles in `PxGroup::set_metrics_registry`.
  - Add accessor `fn read_handles(&self) -> Option<&ReadRegistryHandles>`.
- `crowkv/src/cluster/group_election.rs`
  - In `linearizable_read_barrier`: time the barrier, observe
    `read.barrier.l`, increment `read.lease_path.c` or
    `read.readindex_path.c` on `Ready`, set `read.lease_valid.g`.
- `crowkv/src/cluster/px_kv_store.rs`
  - In `resolve_read_point`: on MinSlot `NotLeader` branch increment
    `read.minslot_fallback.c`; bridge `read.contiguous_applied.g`
    and `read.safe_slot.g` on every call.
  - In `kv_get`: time `engine_get_bytes`, observe
    `read.engine_get.l`.
- `crowkv/src/rpc/kv_service.rs`
  - Add to `KvMetrics`: `read_bytes_in_bw`, `read_bytes_out_bw`
    (Bandwidth), `get_forwarded_c`, `get_forward_failed_c` (Counter).
  - In `get`: observe read bandwidth on all three paths; increment
    `get_forwarded_c` on successful forward, `get_forward_failed_c`
    on forward failure.
  - In `scan`: observe read bandwidth on all paths.
- `crowkv/tests/` — new integration test file for read metrics.

## Test Checklist

- [ ] New handles flush in correct sections (counter, summary,
      bandwidth, gauge).
- [ ] Lease-path linearizable get → `read.lease_path.c` +
      `read.barrier.l` + `read.engine_get.l` + read bandwidth.
- [ ] ReadIndex-path linearizable get → `read.readindex_path.c` +
      `read.barrier.l` avg > lease-path avg.
- [ ] MinSlot fallback → `read.minslot_fallback.c`.
- [ ] Forward success → `kv.get_forwarded.c`; forward failure →
      `kv.get_forward_failed.c`.
- [ ] Gauges reflect state after a read.
- [ ] `read.lease_path.c + read.readindex_path.c` == total
      linearizable get count in window.

## Dependency Ordering

T1 → T2, T3, T4 (handles must exist before instrumentation).
T5 is independent (lives in `KvMetrics`, not `ReadRegistryHandles`).
T6 after T1–T5. T7 after T6.
