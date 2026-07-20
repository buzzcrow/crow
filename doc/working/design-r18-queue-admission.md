<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R18 Design — Queue-based admission control for inflight proposals

## Problem

The current inflight admission control in `PxGroup::propose()`
(`crowkv/src/cluster/group.rs:1007`) uses `Semaphore::try_acquire` on a
single semaphore. When the window is full, the proposal immediately
returns `ProposeResult::Busy`. The client retries up to `max_retries`
(default 3), generating a reject-retry storm under high concurrency.

In the README benchmark (window=1, 16 threads), 40% of RPCs are `Busy`
rejections — wasted CPU on both sides, artificially depressing
throughput. Raft-style systems queue instead of rejecting.

## Current behavior

- `PxGroup` holds a single `inflight_window: tokio::sync::Semaphore`
  sized to `PaxosConfig::max_inflight_proposals` (default 32).
- `propose()` calls `try_acquire()`; on failure returns `Busy`.
- `set_inflight_window(max_inflight)` replaces the semaphore (called
  from `startup.rs:154`).
- CLI flag `--max-inflight` (default 32) flows through
  `store_registry.rs` → `create_group_with_wal` → `set_inflight_window`.
- Test hook `inflight_window()` (under `test-util`) returns `&Semaphore`
  for the test `propose_returns_busy_when_window_is_full`.
- `inflight_slot_count()` derives occupied count from
  `window_size - available_permits`.

## Proposed approach

Replace the single semaphore with an `InflightAdmission` struct owned
by `PxGroup`:

```rust
struct InflightAdmission {
    queues: Vec<Semaphore>,
    queue_count: usize,
    window_per_queue: usize,
    policy: AdmissionPolicy,
    route_counter: AtomicU64,
    // Metrics
    total_enqueued: AtomicU64,
    total_wait_us: AtomicU64,
    waiting: AtomicU64,
}

enum AdmissionPolicy {
    Reject,  // current behavior
    Queue,   // block on acquire().await
}
```

### Routing

Round-robin via `route_counter.fetch_add(1) % queue_count`. Each queue
gets `ceil(max_inflight / queue_count)` permits. Round-robin is chosen
over `hash(client_id)` for benchmark fairness — a single-client
benchmark would route all traffic to one queue under hash routing.

### Propose flow

1. Route to queue `q = route_counter++ % queue_count`.
2. `try_acquire()` on `queues[q]`.
3. If success → proceed to Paxos (fast path, zero overhead).
4. If fail and `Reject` → return `ProposeResult::Busy` (current
   behavior).
5. If fail and `Queue` → record `Instant::now()`, increment `waiting`,
   `acquire().await`, decrement `waiting`, record wait duration in
   `total_wait_us`, increment `total_enqueued`, proceed to Paxos.

The permit is held for the whole proposal (released on drop at every
return path), same as today.

### Correctness

Multi-Paxos slots are independent Paxos instances. The order in which
proposals acquire inflight permits does not affect safety — each slot
is decided by its own Paxos round. Multi-queue routing only changes
admission ordering, not the consensus protocol. No correctness impact.

### Metrics

New fields on `InflightAdmission`, exposed via `PxGroup` methods:
- `inflight_queue_depth()` — `waiting` atomic (current waiters)
- `inflight_total_enqueued()` — `total_enqueued` atomic
- `inflight_total_wait_us()` — `total_wait_us` atomic
- `inflight_occupied()` — sum of `window_per_queue -
  available_permits()` across all queues

These are surfaced in `GroupStatus` as an `InflightStatus` sub-struct.

### CLI configuration

New flags on `crowkv-server`:
- `--inflight-queues N` (default 1)
- `--inflight-admission <reject|queue>` (default `reject`)

These flow through the same path as `--max-inflight`:
`cli.rs` → `store_registry.rs` → `create_group_with_wal` →
`PxGroup::set_inflight_config(max_inflight, queues, policy)`.

The bench provision path (`DeployNodeServerBody`) gets corresponding
`inflight_queues` and `inflight_admission` fields.

### Backward compatibility

- Default `AdmissionPolicy::Reject` + `queue_count=1` preserves
  exact current behavior.
- `#[serde(default)]` on new `GroupStatus` fields keeps old clients
  working.
- `set_inflight_window(max_inflight)` is replaced by
  `set_inflight_config(max_inflight, queues, policy)`; all callers
  updated.

## Alternatives considered

- **Single semaphore with `acquire().await`**: simplest, but doesn't
  support multi-queue or the reject/queue toggle. Doesn't allow
  comparison benchmarks.
- **`tokio::sync::Notify` + manual queue**: reimplements what
  `Semaphore` already does. No benefit.
- **Lock-free MPSC queue**: tokio Semaphore is already lock-free on the
  fast path. A custom queue adds complexity for no measurable gain at
  this scale.
- `hash(client_id) % N` routing: unfair for single-client benchmarks.
  Round-robin is simpler and fairer.

## Acceptance test plan

- `propose_returns_busy_when_window_is_full` — updated to work with
  multi-queue (exhaust all queues).
- New test: `propose_queues_when_policy_is_queue` — set policy to
  `Queue`, exhaust all permits, verify `propose()` blocks then succeeds
  after a permit is released.
- New test: `multi_queue_distributes_permits` — verify permits are
  distributed across queues.
- Existing Paxos tests pass unchanged with default config (reject, 1
  queue).
- Benchmark: queue mode window=1 shows no `Busy` rejections.
