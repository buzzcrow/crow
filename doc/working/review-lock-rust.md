# Rust Lock Review

Review of all mutex / `RwLock` / `parking_lot` / `tokio::sync` lock usage
across the Rust crates in the workspace. Each file with lock usage was
inspected; findings are organized by severity — **Critical** (hot path,
significant performance or correctness impact), **Medium** (potential
issue or suboptimal pattern), and **OK** (correct, no action needed).

Scanned 75 production files with `Mutex`, `RwLock`, `parking_lot::`,
`tokio::sync::{Mutex, Semaphore, Notify}`, `Condvar`, `Barrier`, plus
`Atomic*` / `OnceLock` / `arc_swap` for lock-free patterns. Test files
and `build.rs` are excluded from the writeup (covered at the end).

The codebase leans heavily on lock-free patterns (`DashMap`, `Atomic*`,
`OnceLock`, `arc_swap`, `tokio::sync::Notify`, per-bit CAS) and reserves
mutexes for genuinely rare or read-heavy paths. The findings below are
therefore fewer and milder than the C++ review.

---

## Critical — Hot Path / Significant Impact

### 1. `PxLearner::out_of_order` Mutex on the chosen-frontier advance

- **File:** `lib/crow-kv/src/paxos/learner.rs` lines 89, 344, 373, 404, 413
- **Lock:** `parking_lot::Mutex<BTreeMap<SlotIndex, PxTerm>>` taken in
  `update_chosen_frontier()` (line 413) and `advance_applied_frontier()`
  (line 448, on `applied_out_of_order`).
- **Problem:** `update_chosen_frontier` is called from
  `handle_accept_inner` / `BatchChosen` / `handle_heartbeat` — i.e. on
  every accepted slot on a follower and every chosen notice. The mutex
  is held for the full BTreeMap insert + contiguous-watermark drain
  loop. Under out-of-order delivery (multi-pipeline WAL, batched
  accepts), concurrent frontier advances serialize on this single lock.
  The `last_chosen_term` store at lines 373/404 also takes the lock
  purely to fence the atomic store — a pattern that adds lock
  acquisitions even on the in-order fast path.
- **Mitigating factor:** On the leader, propose slots are sequential so
  the map stays empty and the drain loop is O(1). Contention is real
  only on followers under out-of-order apply.
- **Recommendation:** The `last_chosen_term` fence (lines 373/404) can
  use a `compare_exchange` loop on a packing atomic instead of the
  mutex. For the frontier drain, a lock-free skip list or a sharded
  map (by `slot % N`) would reduce contention; alternatively, since
  `contiguous_chosen` is the fast-path check, defer the out-of-order
  map insert to a background drain. Medium-high priority if follower
  apply throughput is a bottleneck.

### 2. `WalEngine::index` Mutex held during batch index insert

- **File:** `lib/crow-kv/src/wal/pipeline_writer.rs` lines 447, 655;
  `lib/crow-kv/src/wal/wal_engine.rs` line 82
- **Lock:** `Arc<parking_lot::Mutex<SegmentIndex>>` — taken in
  `update_index_locked` (line 447) after every flush batch to insert
  `batch.len()` slot locations, and in `register_segment` (line 655)
  on segment rotation.
- **Problem:** Every flush batch (which may contain many records) holds
  the index mutex for the full insert loop. With multiple pipelines
  (multi-disk WAL), each pipeline's writer task contends on the same
  `index` mutex — flush completions from different disks serialize.
  GC and replay also take this lock (`wal_engine.rs` line 346 exposes
  it), so a GC pass can stall pipeline flushes.
- **Mitigating factor:** The append path itself (`WalEngine::append`)
  is fully lock-free (unbounded mpsc to the writer task, no index
  touch on append). The lock is only on the flush-completion side, so
  append latency is unaffected; only flush throughput (and thus
  steady-state ack rate under batching) is impacted.
- **Recommendation:** Sharded index (one mutex per pipeline, merged on
  read) or a lock-free concurrent map. Lower priority than #1 since
  append is lock-free; revisit if multi-disk flush throughput is
  bounded by index contention.

### 3. `ClientMetrics::window_lat` std::Mutex on every op

- **File:** `lib/crow-kv-client/src/metrics.rs` lines 161, 166–168
- **Lock:** `std::sync::Mutex<WindowLatency>` taken in
  `record_put_latency` / `record_get_latency` / etc. — called on
  **every client RPC completion**.
- **Problem:** Every put/get/delete/scan/batch_write records its
  latency into `window_lat` under the mutex. On a high-QPS
  multi-threaded client, all completion paths serialize on this single
  mutex. The `PreciseHistogram::record` inside is non-trivial (bucket
  scan), so the critical section is not just a counter bump.
- **Mitigating factor:** The error counters are all lock-free atomics
  (good). Only latency recording takes the lock. If the client is
  single-threaded or low-QPS, this is fine.
- **Recommendation:** Per-thread histograms drained into the window on
  flush (thread-local accumulation), or a lock-free histogram. Medium
  priority for high-QPS multi-threaded clients.

---

## Medium — Potential Issues / Suboptimal Patterns

### 4. `PxGroup::coalescer` Mutex on the propose path

- **File:** `lib/crow-kv/src/cluster/group.rs` line 244;
  `group_election_leader.rs` (drain on round completion)
- **Lock:** `parking_lot::Mutex<Option<PendingBatch>>` — taken on every
  propose to append to the pending batch, and on round completion to
  drain it.
- **Problem:** The coalescer serializes all concurrent proposers that
  arrive while a round is in flight. Under high propose concurrency
  (the intended use case for coalescing), this is a single lock on the
  write path.
- **Mitigating factor:** The critical section is short (Vec push, no
  I/O). The `max_keys` overflow path starts a concurrent round without
  holding the coalescer lock. `parking_lot::Mutex` is fair and fast.
- **Recommendation:** Acceptable. If profiling shows contention,
  consider an MPSC channel into the coalescer instead of a shared
  mutex.

### 5. `PxGroup::peer_applied` / `peer_durable` Mutex on heartbeat replies

- **File:** `lib/crow-kv/src/cluster/group.rs` lines 145, 157
- **Lock:** Two `parking_lot::Mutex<HashMap<PxNodeId, SlotIndex>>`,
  updated on every heartbeat reply (`note_peer_applied` /
  `note_peer_durable`).
- **Problem:** Heartbeat replies arrive concurrently from N peers; each
  takes both mutexes to update its entry. Under a large replica set
  and tight heartbeat interval, this serializes reply processing.
- **Mitigating factor:** Replica sets are small (3–7 typically), so
  contention is bounded. The `group_safe_slot` / `group_snapshot_slot`
  watermarks are atomic (lock-free reads).
- **Recommendation:** Low priority. A `DashMap` or per-peer atomic
  `SlotIndex` would eliminate the lock, but the gain is marginal at
  typical replica-set sizes.

### 6. `PxLocalReplica::gap_slots` Mutex on the apply loop

- **File:** `lib/crow-kv/src/cluster/local_replica_apply.rs` lines 117,
  147, 167, 347
- **Lock:** `Arc<Mutex<BTreeSet<SlotIndex>>>` — inserted into on every
  gap detected by the apply loop (line 347) and on `ChosenNotice`
  gaps (line 117); drained by the FetchGap driver (line 167).
- **Problem:** Under a large gap window (many missing slots), the
  BTreeSet insert and the drain both hold the mutex. The apply loop
  and FetchGap driver contend on it.
- **Mitigating factor:** Gaps are bounded by `MAX_INFLIGHT_FETCHGAP`
  and drained continuously. Steady-state (no gaps) never touches the
  lock.
- **Recommendation:** Low priority. A `DashMap<SlotIndex, ()>` or a
  lock-free bounded ring would reduce contention under heavy gap
  load, but the steady-state path is fine.

### 7. `RangeBindingClient::bindings` RwLock read on every route

- **File:** `lib/crow-kv-client/src/range_binding.rs` line 98
- **Lock:** `parking_lot::RwLock<Vec<ChunkdbRangeBinding>>` — read lock
  on every `route()` call (line 103, 108).
- **Problem:** Every chunk routing decision takes a read lock. Under
  high chunk-write QPS, this serializes routing on the RwLock even
  though the table is rarely refreshed.
- **Mitigating factor:** `parking_lot::RwLock` allows concurrent
  readers, so contention is only with the rare refresh (write lock).
  The critical section is a binary search (short).
- **Recommendation:** Use `arc_swap::ArcSwap<Vec<ChunkdbRangeBinding>>`
  for lock-free reads (refresh = swap to a new `Arc<Vec>`). This
  matches the pattern already used for `watch_registry` in the learner.

### 8. `DdbDiskGroup::allocating_disks` RwLock read on every allocate

- **File:** `app/crow-diskdb/src/model/disk_group.rs` lines 144, 195
- **Lock:** `RwLock<Arc<AllocateDiskContext>>` — read on every
  `allocate_block` / `allocate_blocks` call (the diskdb allocation
  hot path).
- **Problem:** Every block allocation takes a read lock to clone the
  `Arc<AllocateDiskContext>`. The lock is held only for the clone
  (brief), but it's still a shared mutex on the alloc path.
- **Mitigating factor:** This is an RCU pattern — the read lock is held
  only for `Arc::clone`, then dropped. `parking_lot` RwLock reads are
  cheap. The actual allocation (per-bit CAS on the zone bitmap) is
  lock-free.
- **Recommendation:** Convert to `arc_swap::ArcSwap<AllocateDiskContext>`
  for truly lock-free reads (no RwLock at all). Low priority since the
  critical section is one `Arc::clone`.

### 9. `MetricsRunner` + `engine_collector` mutex chain on every flush

- **File:** `lib/crow-common/rust/src/metrics/runner.rs` lines 116–134;
  `app/crow-kv-server/src/engine_collector.rs` lines 135–220
- **Lock:** The flush task takes `registry.lock()` then `writer.lock()`
  then `system_collector.lock()` in sequence. The engine collector
  callback takes `handles.lock()`, `known_keys.lock()`,
  `last_snapshot_pages.lock()`, `last_block_device.lock()` — up to 6
  mutexes per flush.
- **Problem:** This is a background flush tick (not the hot path), but
  the collector callback runs *inside* the `registry.lock()` held by
  the runner (line 116 → line 113 `col()` → engine_collector). So the
  registry mutex is held for the entire collector duration, including
  all the per-group `for_each_group` scans and `last_snapshot_pages`
  updates. Any thread trying to register a new metric during a flush
  blocks for the whole collector run.
- **Mitigating factor:** Flush interval is typically 1–10s and the
  collector is O(groups). Registration is rare (only on group
  add/remove). The `unwrap_or_else(into_inner)` pattern avoids
  poisoning panics.
- **Recommendation:** Run the collector *before* taking
  `registry.lock()` (collect into a local buffer, then lock + apply).
  This is the same fix C++ `MetricsRegistry` needs (#4 in
  `review-lock.md`). Medium priority.

### 10. `PxLocalReplica::election_state` Mutex — locked reads on status path

- **File:** `lib/crow-kv/src/cluster/local_replica.rs` lines 555, 561,
  567, 587, 659, 667, 671, 675
- **Lock:** `parking_lot::Mutex<ElectionPersistentState>` — taken for
  every `current_term()`, `voted_for()`, `vote_lockout_until()`,
  `believed_leader_id()`, etc.
- **Problem:** These are called by the status/diagnostic path and some
  election handlers. The hot path (propose leadership gate, `is_leader`)
  correctly uses the atomic mirrors (`role_atomic`, `current_term_atomic`)
  — good. But `current_term()` (locked) is also called in a few
  non-hot paths that could use the atomic snapshot instead.
- **Mitigating factor:** The atomic mirrors exist precisely for this;
  the locked reads are the source-of-truth for *decisions*, not
  observations. The split is documented at lines 87–97.
- **Recommendation:** Audit callers of `current_term()` / `voted_for()`
  and switch to `current_term_snapshot()` where a stale snapshot is
  safe (observations, logging). Low priority.

---

## OK — Correct, No Action Needed

These were reviewed and found correct, several representing notably
good lock-free design:

- **`PxAcceptor`** (`paxos/acceptor.rs`) — uses `DashMap` for per-slot
  promised/accepted state. **No mutex at all** on the prepare/accept
  hot path. Each slot's state is a sharded map entry. Correct and
  efficient.

- **`PxLearner::dedup`** (`learner.rs` line 105) — `DashMap<u64,
  DedupWindow>`. Lock-free sharded dedup cache. Correct.

- **`PxLearner::watch_registry`** (`learner.rs` line 133) —
  `arc_swap::ArcSwapOption<...>`. Lock-free read on the apply path,
  gated by `has_watchers: AtomicBool` (one Acquire load, zero overhead
  when no watchers). The `RwLock<PrefixTrie>` in `WatchRegistry` is
  only touched when `has_watchers` is true, and `emit` takes a **read**
  lock (concurrent emits allowed). Good design.

- **`PxGroup::snapshots`** (`group.rs` line 259) — `DashMap<u64, Arc<
  SnapshotHandle>>`. Lock-free snapshot handle registry. Reaped
  lazily. Correct.

- **`PxKvStore::groups`** (`px_kv_store.rs` line 25) — `DashMap<u64,
  Arc<PxGroup>>`. Lock-free group lookup. `server_state: Mutex` only
  guards crow-rpc task lifecycle (not hot). Correct.

- **`PxGroup::driver_handle` / `maintenance_handle` / `fetchgap_handle`**
  (`group.rs` lines 109, 116, 119) — `tokio::sync::Mutex<Option<
  JoinHandle>>`. Async mutex so `shutdown` can `await` the handle
  cooperatively without blocking other readers. Correct (async mutex
  is the right choice for `await`-holding critical sections).

- **`InflightAdmission`** (`group_inflight.rs`) — `tokio::sync::Semaphore`
  with fast-path `try_acquire`. Async-aware admission control. Correct.

- **`WalEngine::append`** (`wal_engine.rs` lines 257–312) — fully
  lock-free: unbounded mpsc to the writer task, `oneshot` ack. No
  mutex on the append path. `select_pipeline` is deterministic
  (hash-based, no lock). `failed` / `snapshot_slot` / counters are
  atomic. Excellent design.

- **`WalEngine::writer_tasks`** (`wal_engine.rs` line 95) —
  `parking_lot::Mutex<Vec<JoinHandle>>` only touched in `Drop`. Not
  hot. Correct.

- **`BlockDevice` / `BlockDeviceController`** (`block_backend.rs`) —
  all counters are `AtomicU64` / `AtomicBool`. The `corrupt_requests`
  and `apply_corruptions` mutexes are test-injection only. Correct.

- **`PxLocalReplica` atomic mirrors** (`local_replica.rs` lines 192,
  196) — `current_term_atomic` / `role_atomic` updated under the
  mutex with `Release`, read lock-free with `Acquire`. The
  `from_u8` fallback (line 75) logs + falls back to `Follower` on
  impossible discriminants. Correct defensive design.

- **`PxLocalReplica` lease fields** (`local_replica.rs` lines 202, 205)
  — `lease_read_until_ms` / `last_quorum_heartbeat_at_ms` are
  `AtomicU64` updated via `fetch_max` (lock-free lease extension).
  Correct.

- **`PxLocalReplica::known_commit_slot` / `apply_notify`**
  (`local_replica.rs` lines 256, 260) — `Arc<AtomicU64>` +
  `Arc<tokio::sync::Notify>`. Lock-free commit-slot advance, Notify
  wakes the apply loop. Correct async pattern.

- **`TopologyCache`** (`crow-kv-client/src/topology.rs`) —
  `leaders: DashMap`, `replicas: DashMap` (lock-free lookups). `seeds:
  RwLock` only on refresh. `refresh_gate: AsyncMutex` for single-flight
  HTTP. Correct.

- **`ClientMetrics` error counters** (`crow-kv-client/src/metrics.rs`)
  — all `AtomicU64` with `Relaxed` ordering. `leader_changes: Mutex`
  only on rare leader-change events (documented as non-hot). Correct.

- **`DdbZone` allocation** (`app/crow-diskdb/src/model/zone.rs`) —
  per-bit CAS on `usage_bits` (lock-free allocate). `zone_lock:
  RwLock<()>` is explicitly **not held on allocate** (documented line
  76–78); only compaction/scanner take it, and never across `.await`
  (I9). `compacting: AtomicBool` prevents concurrent compaction.
  Excellent lock-free hot-path design.

- **`DdbDiskGroup::free_ts_source`** (`disk_group.rs` line 44) —
  `AtomicU64` with `fetch_max` for monotonic timestamp. Lock-free.
  Correct.

- **`DdbDisk::active_zone_context`** (`disk.rs` line 33) — RCU via
  `RwLock<Arc<ActiveZoneContext>>`. Read lock only for `Arc::clone`.
  Correct.

- **`KeepAlive` RwLocks** (`liveness/keepalive.rs` lines 96, 99, 108)
  — `disk_miss_counts`, `recovery_scans`, `disk_suspect_since` — all
  on the background sync loop, not the allocate hot path. Correct.

- **`diskdb_service.rs` disk/group reads** — `disks.read()`,
  `disk_value.read()`, `status.read()` on the crow-rpc service path. These
  are admin/query RPCs (list disks, query usage), not the allocate hot
  path. Brief read locks. Correct.

- **`BindingCache`** (`app/crow-chunkdb/src/routing.rs`) —
  `parking_lot::RwLock<BindingTable>`. Read lock on route, write on
  refresh. Same pattern as `RangeBindingClient` (#7) but chunk routing
  is lower QPS. Acceptable; could use `arc_swap` for consistency.

- **`chunkdb` topology / range_guard / lifecycle** — `parking_lot::Mutex`
  / `RwLock` for routing tables and range-guard state. Refresh-only
  writes, read-heavy. Correct.

- **`crow-console-shared` ops_log / monitor / ssh / known_hosts** —
  console-side mutexes for SSH session map, ops log, known-hosts
  cache. Not on a data-plane hot path. Correct.

- **`crow-web` lifecycle / state / mgmt** — `parking_lot::Mutex` /
  `RwLock` for cluster state cache, node lifecycle, mgmt ops. These
  are the console's in-process state, mutated by HTTP handlers and
  background pollers. Not a high-QPS hot path. Correct.

- **`store_registry.rs`** (`crow-kv-server`) — `DashMap` for stores
  with `Mutex`-guarded registration. Correct.

- **`MetricsRegistry` global singleton** (`registry.rs` line 326) —
  `OnceLock<Mutex<MetricsRegistry>>`. Registration is rare (static
  init via `global_counter` etc.). Correct.

- **`group_election_leader.rs`** — `pending_leader_handoff`,
  `pending_read_barrier`, `readindex_round_gate` (test-only) — all
  `parking_lot::Mutex`, taken on leader-state transitions and
  ReadIndex round start/completion. Not per-op hot path. Correct.

---

## Tests / Benches

Test and bench files (`*_test.rs`, `*_bench.rs`, `tests/common/*`)
use `Mutex`/`RwLock`/`Atomic*` for test-harness synchronization
(barriers, shared counters, net-lock fixtures, cluster harness state).
These are not production code and were not analyzed for hot-path
impact. Notable test-infrastructure patterns:

- `tests/common/net_lock.rs` — `std::Mutex` + `Condvar` for a
  cross-process test lock fixture. Correct for test use.
- `tests/common/cluster.rs` — `parking_lot::Mutex` for cluster
  harness state (process handles, logs). Correct.
- `benches/slot_list.rs` — `AtomicU64` for benchmark counters.
  Correct.

---

## Summary

- **3 critical findings** — learner frontier mutex on the apply path,
  WAL index mutex on flush completion, client latency mutex on every
  op.
- **7 medium findings** — coalescer mutex, peer-watermark mutexes,
  gap-slots mutex, range-binding RwLock, diskdb RCU RwLock, metrics
  collector mutex chain, election-state locked reads.
- **~30 OK** — notably good lock-free design (DashMap everywhere,
  atomic mirrors, arc_swap, per-bit CAS, async-aware semaphore).

The highest-impact fix is **#1 (PxLearner::out_of_order mutex)** —
moving the chosen-frontier advance off the mutex (or sharding it)
would unblock concurrent out-of-order applies on followers. The
codebase's heavy use of `DashMap`, `Atomic*`, `arc_swap`, and
`OnceLock` keeps most hot paths lock-free; the mutexes that remain
are generally on the right paths.
