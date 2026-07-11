# CrowKV - Plan: P1 Consensus Core (remaining work)

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md),
[`plan.md`](plan.md) §1 (P1), [`design-leader-election.md`](design/design-leader-election.md),
[`design-parallel-slots.md`](design/design-parallel-slots.md),
[`design-storage-engine.md`](design/design-storage-engine.md),
[`design-slot.md`](design/design-slot.md), [`design-paxos-error.md`](design/design-paxos-error.md).
Satisfies: the unfinished portion of [`plan.md`](plan.md) **P1 — Consensus Core**.

> **Design grounding (resolved gaps).** This plan was first drafted at a
> milestone granularity. The implementation detail below is now grounded in
> the existing design docs so no silent guesses are needed:
> - **C1** engine surface ⇐ `design-storage-engine.md` §2–§9 (trait methods,
>   per-key `(slot, value_or_tombstone)`, apply idempotency, `compare`/`iter_all`).
> - **C2** read modes/lease/ReadIndex ⇐ `requirement.md` §6.2–§6.5,
>   `design-leader-election.md` (lease bookkeeping already in `local_replica.rs`).
> - **C3** window/repair ⇐ `design-parallel-slots.md` §2 (invariants I1–I5),
>   `design-slot.md` (slot-list reclamation), `requirement.md` §7.3, §12.1.
> - **C4** dedup ⇐ `requirement.md` §10.2 (N=64, T=60s, persisted stream).
>
> **Known wiring points discovered in code (must change, easy to miss):**
> - `cluster/px_kv_store.rs::kv_get` / `kv_scan` read **directly** from
>   `group.local_replica().learner.store()` (the bare `DashMap`). C1 must
>   reroute these through the engine read surface.
> - `paxos/learner.rs` owns the `DashMap` and the `apply_payload` decoder.
>   The decoder (wire format documented there) moves to a shared
>   `Batch::decode`; the `DashMap` becomes `Box<dyn Engine>`.
> - Tests touching the store: `crowkv/tests/paxos/election_test.rs`
>   (`noop_apply_path` uses `learner.store().len()`) and
>   `crowkv/tests/paxos/learner_note_chosen_test.rs` — migrate to the new
>   accessors when C1 lands.
> - `Learner::learn(&self, entry)` is called from `group.rs` and
>   `group_election.rs` via `replica.learn(&entry)`; keep that signature
>   stable (engine apply happens inside `learn`).

This is a temporary, milestone-level plan for **closing out Phase 1**. It
exists because P1 is not yet complete: M1–M3 are done, M4 is partial, and
M5/M6 are not started. Delete this file once the P1 freeze gates in
[`plan.md`](plan.md) §1 are all green and [`plan-wal.md`](plan-wal.md) (P2)
opens.

---

## 1. Status Snapshot (as built)

| Milestone | State | Evidence |
|---|---|---|
| **M1** Core types, `SlotList`, `PxAcceptor`, reclamation | ✅ Done | `paxos/slot_list.rs`, `paxos/slot_node.rs`, `paxos/acceptor.rs` |
| **M2** Minimal gRPC, proto, harness, proposer (classic/optimized/multi) | ✅ Done | `rpc/px_service.rs`, `rpc/kv_service.rs`, `cluster/group.rs::propose` |
| **M3** Election + election-side lease + `PxPeerStream` + bulk Phase 1 + watermarks + counters | ✅ Done | `cluster/group_election.rs`, `cluster/local_replica.rs`, `cluster/peer_stream.rs`, `paxos/learner.rs` |
| **M4** Proposer window / admission queue / `Replicator` / background `Repair` | ✅ Done (C3) | `proposer_window` semaphore + `ProposeResult::Busy`; `repair_once` wired into the leader heartbeat tick; per-slot quorum in `run_accept_phase` + `PxPeerStream` backpressure |
| **M5** `Engine` trait + `InMemoryEngine` (**C1**); read-side lease, ReadIndex, read modes, safe-slot publication (**C2**) | ✅ Done (C1+C2) | `engine/` module + per-mode `resolve_read_point`, `linearizable_read_barrier`, `group_safe_slot`; engine + read-mode + lease tests green |
| **M6** Per-`client_id` dedup cache + `DedupCheckpoint` | ✅ Done (C4) | `PxLearner.dedup` cache + `dedup_lookup`/`record_dedup`; proposer short-circuits retries. `DedupCheckpoint` emission deferred to P2 M4 replay (no WAL consumer in P1) |

**Conclusion:** M1–M6 are now implemented (C1–C4 landed, tests green). The
remaining P1 exit item is the **G1** integration scenario (3-node writes
survive a forced leader step-down); all in-memory, no WAL yet.

---

## 2. Remaining Milestones

Ordering is dependency-driven: **C1 (Engine trait) first** because M5 reads
and M6 dedup both consume a stable engine surface; then C2 (read pipeline),
C3 (proposer window/repair), C4 (dedup). C3 is independent of C1/C2 and may
proceed in parallel.

### C1 — Engine trait + `InMemoryEngine` (closes M5 part 1; freeze gate)

Lift the KV state out of `PxLearner`'s bare `DashMap` behind a trait so P3
can swap in `OrderedFileEngine` / `CrowtreeEngine` without touching
consensus code.

| Task | Detail |
|---|---|
| `Engine` trait | `apply(slot, batch)`, `get(k) -> Option<(slot, value)>`, `scan(range, limit)`, `snapshot_export`/`snapshot_import` (stubs OK for P1), `compare(other) -> Diff`, `iter_all`. All `async` (per `plan.md` §5). |
| Per-key slot tracking | Engine stores `(slot, value)` per key; apply only when `slot > current_slot` for that key (`requirement.md` §7.3.1, §8.3). `Delete` writes a tombstone with its slot. |
| `InMemoryEngine` | BTree-backed implementation of the trait. |
| Rewire `PxLearner` | `PxLearner::learn` decodes payload → `engine.apply(slot, batch)`; the `DashMap` store is removed. Watermark logic (`contiguous_chosen`/`applied`, `note_chosen`) stays in the learner. |
| Rewire reads | `px_kv_store.rs::kv_get`/`kv_scan` call new learner accessors (`engine_get` / `engine_scan`) instead of `learner.store()`. |

Concrete surface (`crowkv/src/engine/mod.rs`), grounded in
`design-storage-engine.md` §4–§8:

```rust
pub enum Op { Put(Vec<u8>), Delete }            // value or tombstone
pub struct BatchOp { pub key: Vec<u8>, pub op: Op }
pub struct Batch { pub ops: Vec<BatchOp> }       // Batch::decode(payload)->Batch

/// Per-key single-version entry as stored by the engine.
pub enum Cell { Value(Vec<u8>), Tombstone }

pub trait Engine: Send + Sync {
    /// Atomic, idempotent. Skips any op whose `slot <= resolved_slot(key)`.
    fn apply(&self, slot: u64, batch: &Batch);
    /// Live value + its resolved slot; `None` for unset/tombstoned keys.
    fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)>;
    /// Ordered live entries (no tombstones) with prefix, capped by `limit`
    /// (0 = unlimited). Returns `(items, truncated)`.
    fn scan(&self, prefix: &[u8], limit: usize) -> (Vec<(Vec<u8>, u64, Vec<u8>)>, bool);
    /// Ordered full stream incl. tombstones, for `compare`.
    fn iter_all(&self) -> Vec<(Vec<u8>, u64, Cell)>;
    /// Logical diff (sorted by key) of `(slot, cell)`; empty == equal.
    fn compare(&self, other: &dyn Engine) -> Vec<EngineDiff>;
    fn live_key_count(&self) -> usize;
    // snapshot_export/import: stub signatures in P1; bodies land in P3.
}
```

- `InMemoryEngine` backs this with `parking_lot::RwLock<BTreeMap<Vec<u8>, (u64, Cell)>>`
  — `BTreeMap` gives ordered scan/`iter_all` for free and a write lock makes
  `apply` atomic to readers (`design-storage-engine.md` §4.3, §9.1).
- `apply` is **not** `async` in P1 (in-memory is synchronous); the trait stays
  object-safe (`Box<dyn Engine>`). Async/streaming snapshot methods are added in
  P2/P3 without breaking the P1 freeze (additive).
- Tombstone retention/compaction (`design-storage-engine.md` §7) is **deferred to
  P2** (needs `safe_slot`/`snapshot_slot`); P1 keeps tombstones forever.

**Acceptance:** unit tests — per-key highest-slot-wins apply order; tombstone
on delete; `compare()` zero divergence after 100 random ops between two
`InMemoryEngine`s; `InMemoryEngine` implements the trait with no warnings.

**Freeze gate:** Engine trait surface frozen at end of C1 (P3 M1 only
reviews it; no additions without a version bump).

### C2 — Read pipeline: modes + read-side lease + ReadIndex (closes M5 part 2)

The lease *state* exists (`local_replica.rs::renew_lease`,
`lease_read_until_ms`) but is never read. Wire it to reads and add the
fallback + the client-facing read modes.

> **Status: DONE.**
> - `PxLocalReplica::lease_read_valid(now)` — leader + valid-lease gate for the
>   linearizable read fast path (test `crowkv/tests/paxos/lease_read_test.rs`).
> - Group **safe-slot publication** — `PxGroup::{note_peer_applied,
>   group_safe_slot}` track per-peer `contiguous_applied` (refreshed from the
>   heartbeat round, conservative: unheard peer = 0, monotonic per tenure) and
>   publish `min` across the local replica + all voting peers. The watermarks
>   are reset at each leader-tenure entry (`reset_safe_slot_tracking`) so a new
>   leader cannot inherit a previously elevated safe-slot. Test
>   `crowkv/tests/cluster/safe_slot_test.rs::group_safe_slot_is_min_applied_across_voting_members`.
> - **Quorum-confirmed ReadIndex** — `HeartbeatOutcome::Continued { quorum_acked }`
>   + `PxGroup::linearizable_read_barrier` (lease fast path, else a ReadIndex
>   quorum heartbeat; `NotLeader` / `NoQuorum` outcomes).
> - **Wire format** — `kv.proto` gained `ReadMode` enum + `read_mode`/`client_slot`
>   on `KvGetRequest`, `read_mode` on `KvScanRequest`, `read_slot`/`safe_slot` on
>   `KvResponse`, `read_slot` on `KvScanResponse` (all append-only). All request
>   construction sites across `crowkv` / `crowkv-server` / `crowkv-console` updated.
> - **Routing** — `PxKvStore::resolve_read_point` implements the per-mode matrix
>   (Linearizable→barrier on leader, **redirect (`NotLeader`) off-leader or when
>   the barrier is lost — never a stale local serve**; `ReadYourWrites`→ local
>   once applied ≥ `client_slot` else redirect; `BoundedStale`/`BestEffort`→
>   local); `kv_service` forwards only linearizable reads. Tests
>   `crowkv/tests/cluster/node_test.rs::read_modes_serve_value_with_slots_on_single_leader`
>   and `kv_forward_test.rs::forwarded_request_does_not_re_forward` (loop-guarded
>   linearizable read redirects rather than serving stale).

| Task | Detail |
|---|---|
| Safe-slot publication | Leader computes group `safe-slot = min(contiguous_applied)` across learners (heartbeat already carries `committed_safe_slot`); expose it in write/read responses and a lightweight RPC (`requirement.md` §6.3). |
| Point read modes | `Linearizable` (leader, lease-valid fast path), `ReadYourWrites` (any replica with per-key resolved-slot ≥ client slot), `BoundedStale` (replica with global resolved-slot ≥ `last_known_safe_slot`), `BestEffortStale` (`requirement.md` §6.4). |
| Scan read modes | `Linearizable` (leader waits for own contiguous applied frontier ≥ captured `target`), `SafeSlot`, `AtSlot(N)` (`requirement.md` §5.2, §6.5). |
| Lease fast path | `Get(Linearizable)` serves locally iff `lease_read_until > now`; else ReadIndex. |
| ReadIndex fallback | Quorum-heartbeat round-trip before responding when the lease is invalid/disabled (`requirement.md` §6.2(b)). |
| Wire-format | Add read-mode + client-supplied slot fields to `kv.proto` (append-only); replace the current "transparent leader-forward only" path in `rpc/kv_service.rs`. |

**Acceptance:** unit tests — write acked value immediately readable on leader;
lease-valid fast read (no RPC); lease-expired path takes ReadIndex; bounded-stale
read served by follower at/after safe-slot; `Scan(Linearizable)` waits across an
artificial gap; `compare()` zero divergence after mixed leader/follower reads.

### C3 — Proposer window + admission queue + background Repair (closes M4)

> **Status: DONE.**
> - **Sliding window + admission** — `PaxosConfig::proposer_window` (default 16)
>   + `PxGroup::proposer_window` (`tokio::Semaphore`). `propose` `try_acquire`s a
>   permit held for the proposal's lifetime; a full window returns the new
>   `ProposeResult::Busy` (mapped to the retryable `busy` keyword in
>   `propose_and_respond`) instead of blocking. Test
>   `crowkv/tests/cluster/proposer_test.rs::propose_returns_busy_when_window_is_full`.
> - **Background repair** — `PxGroup::repair_once` (+ `RepairOutcome`) finds the
>   lowest gap (`contiguous_chosen+1` below `last_chosen_slot`) and closes it via
>   classic Paxos (adopts a half-committed value or fills an empty `NoOp`), then
>   learns + fans out the chosen notice so the frontier/safe-slot advance. Wired
>   into the leader heartbeat tick (`run_leader_state`); a no-gap leader returns
>   immediately with no RPCs. Test
>   `crowkv/tests/cluster/proposer_test.rs::repair_once_fills_gap_and_advances_frontier`.
> - **`Replicator`** — the per-slot quorum count already lives in
>   `run_accept_phase` and per-peer flow control in `PxPeerStream`'s bounded mpsc
>   (full → `Busy`); no separate struct was introduced (no behavior gap).
>
> _Test-only `PxGroup` hooks (`proposer_window`, `repair_once_for_tests`,
> `note_peer_applied_for_tests`) are exposed under the `crowkv` `test-util`
> feature (auto-enabled for `cargo test` via a self dev-dependency), so these
> live as integration tests under `tests/` rather than inline `#[cfg(test)]`
> modules._

| Task | Detail |
|---|---|
| Sliding window | Cap in-flight (allocated-but-not-chosen) slots at `window` (default 16, `requirement.md` §7.3 / §12.1). |
| Admission queue | Bounded queue in front of slot allocation; beyond capacity return retryable `Busy` (`PxPaxosError::Busy` already exists). The leader never blocks indefinitely on a full window. |
| `Replicator` | Per-slot quorum bitmap + per-peer flow control on top of the existing `PxPeerStream` bounded mpsc. |
| Background `Repair` task | Periodic gap detection (age/count threshold) over the open prefix; classic-Paxos repair (`NoOp` fill); advance safe-slot / frontier. Distinct from the one-shot bulk Phase 1 (M3). |

**Acceptance:** unit tests — 10 parallel slots chosen out of order; window-full
yields `Busy`; background repair fills a deliberately abandoned slot and the
frontier advances; `Replicator` quorum bitmap reaches the chosen threshold.

### C4 — Dedup cache + `DedupCheckpoint` (closes M6)

> **Status: DONE** (functional dedup; checkpoint emission is a P2-replay concern).
> - **Per-`client_id` cache** — `PxLearner.dedup` (`DashMap<client_id, {last_seq,
>   last_slot}>`), updated on every `learn` carrying a `(client_id, seq)`
>   (`record_dedup`, monotonic per client; `client_id == 0` = no-dedup sentinel).
> - **Dedup check** — `PxLearner::dedup_lookup` returns the prior commit slot for
>   any `seq <= last_seq`; `PxGroup::propose` consults it after the leadership
>   gate and before window admission, returning `Chosen { cached_slot }` without
>   re-running Paxos. Test `node_test.rs::dedup_suppresses_retried_client_seq`.
> - **`DedupCheckpoint`** — the in-memory cache is authoritative for P1; emitting
>   `PxLogEntryKind::DedupCheckpoint` log entries and rebuilding from them is
>   deferred to **P2 M4 replay** (no WAL consumer exists in P1), per the row below.
>
> _Note: existing `crowkv/tests/cluster/kv_forward_test.rs` reused `seq:1` for
> three distinct writes; corrected to monotonic seq so they are not (correctly)
> collapsed by dedup._

| Task | Detail |
|---|---|
| Per-`client_id` cache | Last applied `sequence_number` + last result, updated on apply. Retention ≥ last 64 requests and ≥ 60 s per active client (`requirement.md` §10.2). |
| Dedup check | On propose/apply, a retried `(client_id, seq)` ≤ last-applied returns the cached result without re-running Paxos. |
| `DedupCheckpoint` entry | Emit `PxLogEntryKind::DedupCheckpoint` log entries (in-memory only for P1; P2 M4 rebuilds from them on replay). |

**Acceptance:** unit tests — retry of same `(client_id, seq)` returns the
identical result; a higher `seq` advances; eviction past the retention window
behaves as documented (unknown outcome).

---

## 3. P1 Exit Criteria (freeze gates from `plan.md` §1)

P1 is complete — and [`plan-wal.md`](plan-wal.md) (P2) may open — when:

- Engine trait surface is frozen (end of **C1**).
- Read modes + lease + ReadIndex pass (**C2**); the §6.5 implementation
  invariants are enforced in code.
- Proposer window / admission / background repair pass (**C3**). ✅
- Dedup idempotency passes (**C4**). ✅
- **G1** (`plan.md` §3): 3-node test — writes survive a forced leader
  step-down — is green. ✅
  (`crowkv/tests/cluster/g1_step_down_survival_test.rs::write_survives_forced_leader_step_down`).

> **All P1 exit gates are green** (C1–C4 + G1; full `cargo test --workspace`
> passes). P1 is feature-complete in-memory; [`plan-wal.md`](plan-wal.md) (P2)
> may open.

> All P1 state remains in-memory (no WAL yet). `current_term` / `voted_for`
> durability and dedup-cache durability are explicitly deferred to **P2 M4
> replay** (`plan.md` §1 P2 M4), not part of this plan.

## 4. Test Pairing

Every task above lands with its unit test in the same change
(`plan.md` §4). Integration scenarios reuse the existing `testkit`
harness (`crowkv/tests/testkit/`, `cluster/`, `paxos/`). Add the G1
forced-step-down survival test under `crowkv/tests/cluster/`.
