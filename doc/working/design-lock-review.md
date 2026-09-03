<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Lock Review Fixes (R121/R122)

This draft covers [R121](../backlog/R121-tree-cpp-lock-review.md) and
[R122](../backlog/R122-kv-rust-lock-review.md). It refines the concurrency
rules in the tree engine, RPC transport, consensus, WAL, client, and metrics
designs. Architecture decisions and rationale are in the root designs; this
doc does not repeat them.

## 1. Lock Disposition

### 1.1 Remove or move off the hot path

- `BufferPool::mu_`: keep metadata serialization initially, but reserve a
  victim under the lock, perform read/write I/O after unlock, then validate
  and publish under the lock. Concurrent loads of one PID share an in-flight
  state rather than duplicate publication.
- `HandlerRegistry::mu_`: publish an immutable handler table with atomic
  shared ownership. Registration clones and swaps; dispatch only loads.
- `thread_name_flag` mutex: store the current thread's name in thread-local
  storage. Formatting reads its own thread-local value without shared state.
- `PxLearner::out_of_order` and `applied_out_of_order`: shard gap ownership by
  slot and serialize only the short contiguous-drain cursor. Publish the
  `(last_chosen_slot, last_chosen_term)` pair through one atomic state so the
  pair cannot tear.
- `ClientMetrics::window_lat`: record into sharded histograms selected by a
  stable thread hash; flush drains shards one at a time. Keep
  `leader_changes` separately locked because it is event-rate, not request-rate.
- `RangeBindingClient::bindings`: use `ArcSwap<Vec<_>>`; refresh/replace builds
  a complete sorted vector and atomically publishes it.

### 1.2 Shard or shorten

- `WalEngine::index`: one index shard per pipeline. Point lookups select the
  shard from the encoded location; replay and GC take bounded snapshots of
  all shards without holding one lock across I/O.
- Engine metrics collector locks: take short snapshots of store/registry
  references, release locks, collect values, then register newly discovered
  handles in a separate bounded phase.
- `ConnectionPool`: retain the mutex until profiling shows contention; if
  needed, publish an immutable connection snapshot and use an atomic
  round-robin cursor.
- `Crowdbtree::load_mutex_`: retain for the cold path initially. Replace with
  per-PID in-flight loading only if cold-load concurrency measurements justify
  its state-machine complexity.

### 1.3 Keep

- `PxGroup::coalescer`: keep; it protects a small mutation transaction and is
  never held across await or I/O.
- `peer_applied` and `peer_durable`: keep until replica sets exceed their
  current small bound; critical sections are bounded map updates.
- `PxLocalReplica::gap_slots`: keep; gaps are exceptional and continuously
  drained. Revisit with measured sustained gap contention.
- `DdbDiskGroup::allocating_disks`: convert to `ArcSwap` only with the diskdb
  allocation work; current read section is a single `Arc` clone.
- `election_state`: keep for compound election transitions. Observation and
  logging callers use existing atomic mirrors where stale snapshots are safe.
- `slot_mutex_`: keep for bounded out-of-order slot tracking; replace only if
  measurements show persistent large gaps.
- Lifecycle mutexes, test-only gates, condition variables, and initialization
  locks remain unchanged.

## 2. Correctness Rules

- L1: No storage or network I/O occurs while a global cache/index mutex is held.
- L2: RCU readers observe one complete old or new publication, never a mixture.
- L3: Frontier slot and term are published as one logical state.
- L4: Deferred or sharded state has a single bounded drain owner.
- L5: A lock is retained when removing it would widen invariants without a
  measured hot-path benefit.

## 3. Scope

- `lib/crowdb-tree/{include,src,tests}` — buffer-pool I/O split and spin backoff.
- `lib/crowdb-rpc/{include,tests}` — immutable handler publication.
- `lib/crowdb-common/cpp/{src,tests}` — thread-local log context.
- `lib/crowdb-kv/src/{paxos,wal}` — frontier and WAL-index sharding.
- `lib/crowdb-kv-client/src` — latency shards and range-binding RCU.
- `app/crowdb-kv-server/src/engine_collector.rs` — collect outside locks.

## 4. Complexity

High. Buffer-pool reservation and learner frontier publication carry the main
correctness risk. The RCU client cache and spin backoff are isolated starting
changes; WAL and metrics changes reuse existing pipeline and snapshot bounds.

## 5. Test Design

- UT: block one buffer-pool miss I/O, then verify an unrelated hit and miss
  complete without waiting; verify one-PID concurrent loads publish once.
- UT: contend every skip-list writer and verify completion plus map parity.
- UT: dispatch concurrently while replacing the handler table; each lookup
  sees a complete old or new handler.
- UT: refresh range bindings during routing; every route sees a complete table.
- UT: deliver chosen/applied slots in randomized parallel order and verify both
  contiguous frontiers and the final slot/term pair.
- UT: flush WAL pipelines and run GC concurrently; verify index completeness.
- UT: record client latency concurrently and verify drained sample counts.
- Integration: metrics discovery during flush neither deadlocks nor changes
  emitted metric values.

## 6. Module Structure

```text
doc/working/design-lock-review.md       combined implementation design
doc/working/plan-lock-review.md         ordered execution checklist
lib/crowdb-tree/...                     cache and memtable concurrency
lib/crowdb-rpc/...                      dispatch publication
lib/crowdb-kv/...                       consensus and WAL concurrency
lib/crowdb-kv-client/...                client RCU and metric shards
app/crowdb-kv-server/...                collector lock boundaries
```
