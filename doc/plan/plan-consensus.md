# CrowKV - Plan: Consensus Core Implementation

Depends on: [`plan.md`](plan/plan.md), [`design.md`](design.md) §3–5, [`design-leader-election.md`](design-leader-election.md), [`design-parallel-slots.md`](design-parallel-slots.md), [`test-design-consensus.md`](test/test-design-consensus.md)
Satisfies: [requirement.md §6.1](requirement.md#61-write-guarantee), [requirement.md §6.2](requirement.md#62-leader-read-fencing), [requirement.md §6.5](requirement.md#65-parallel-slot-linearizability-analysis), [requirement.md §7.2](requirement.md#72-leader-election-and-terms), [requirement.md §7.3](requirement.md#73-parallel-slot-processing)

This document specifies Phase 1: in-memory consensus core with no persistence and no networking.

## Table of Contents

- [1. Milestones](#1-milestones)
- [2. Module Breakdown](#2-module-breakdown)
- [3. Data Types to Implement](#3-data-types-to-implement)
- [4. In-Process Test Harness](#4-in-process-test-harness)
- [5. Freeze Checklist](#5-freeze-checklist)

---

## 1. Milestones

### M1 — Core data types + Acceptor state machine

- **Workspace bootstrap** (greenfield repo): cargo workspace at repo root, `crowkv` crate created. Edition `2021`, MSRV `1.75`. `rustfmt.toml` and `clippy.toml` minimal configs. No production dependencies in M1; dev-dep `tokio` with `macros, rt, test-util` for `start_paused = true`.
- `PxTerm`, `PxBallot`, `PxSlot`, `PxLogEntry`, `PxGroupConfig`, `PxNodeId`
- Acceptor in-memory state: `promised_ballot[slot]`, `accepted[slot] → (ballot, value)`
- Acceptor handlers: `Prepare`, `Accept` (both `async fn`, per [`plan.md`](plan/plan.md) §7)
- Unit tests: prepare/accept basic Paxos rounds, ballot ordering, term fencing

**Acceptance:** unit test shows a single-slot classic Paxos round succeeds and rejects stale ballots.

### M2 — Leader election + bulk Phase 1

- `LeaderElector`: Raft-style randomized timeout, `RequestVote`/`Vote`, term persistence (in-memory)
- Role transitions: Follower → Candidate → Leader, step-down on higher term
- Bulk Phase 1: new leader scans open prefix, adopts values, fills gaps with `NoOp`
- Unit tests: election with 3 in-process nodes, stale leader fenced, bulk Phase 1 adopts in-flight value

**Acceptance:** unit test elects leader, kills old leader, new leader preserves chosen value.

### M3 — Proposer + Replicator + parallel slot pipeline

- `Proposer`: slot counter serialized on a single async task (no shared-state mutex needed), sliding window (configurable, default 16), bounded admission queue, `Busy` backpressure when full
- `Replicator`: per-peer async task; fan-out `Accept` to all peers via `tokio::sync::mpsc` channels, per-slot quorum bitmap, per-peer flow control (cap = window size)
- Background `Repair` async task: gap detection by age threshold, classic Paxos repair (`Prepare`/`Accept` at higher round)
- **Freeze:** consensus message types (`Prepare`, `Promise`, `Accept`, `Accepted`, `Chosen`, `Heartbeat`, `RequestVote`, `Vote`)
- Unit tests: 10 parallel slots chosen out of order, gap repair fills missing slot, safe-slot advances

**Acceptance:** unit test writes 100 ops with window=16, injects artificial delay on one acceptor, repair resolves gap, all ops acked.

### M4 — Learner + Engine trait + leader lease + reads

- `Learner`: applies chosen values to engine, per-key resolved-slot tracking, contiguous-applied frontier
- `Engine` trait + `InMemoryEngine`: `apply(slot, batch)`, `get(k) → Option<(slot, value)>`, `scan(range, limit)`, `snapshot_export/import`, `compare(other) → Diff`
- `Lease`: full deterministic lease state machine driven by `TestTimer` (per [`design-leader-election.md`](design-leader-election.md) §6).
  - Heartbeat round-trip records `lease_grant_until = T_recv + lease_duration` per follower; leader's effective lease is `min(grants) - max_clock_skew`.
  - Linearizable reads on leader: serve locally if effective lease valid; otherwise fall back to `ReadIndex` (quorum heartbeat round-trip).
  - Lease unrenewable for `step_down_threshold` → leader steps down ([`design-leader-election.md`](design-leader-election.md) §8).
- Read modes on leader: `Get`/`Scan` `Linearizable` exercises both lease-valid fast path and ReadIndex fallback path.
- **Freeze:** Engine trait surface (downstream P3 implements additional backends against this exact trait)
- Unit tests: write acked value immediately readable; parallel slots apply out-of-order; per-key resolved-slot wins; lease-valid fast read; lease-expired ReadIndex fallback; lease-unrenewable step-down.

**Acceptance:** unit test: 3-node group, 100 random `Put`/`Get` operations, `compare()` across all learners shows zero divergence; lease state machine passes the three lease-specific tests above.

### M5 — Dedup cache + client idempotency

- Per-`client_id` last-sequence map, last-result cache
- `DedupCheckpoint` entry kind (stubbed log integration — in-memory only)
- Unit tests: retry same `(client_id, seq)` returns same result, different seq advances

**Acceptance:** unit test: client retries 5 times, only one slot assigned, same response each time.

## 2. Module Breakdown

Module: **`consensus`** inside `crowkv`. Engine trait + `InMemoryEngine` live in the sibling module **`engine`** (P1 M4 introduces them; P3 adds backends). Test harness lives in **`crowkv::testkit`** (dev-dep, shared with WAL and other crates).

| Rust path (in `crowkv/src/consensus`) | Responsibility | Lines (est) | Tests (est) |
|---|---|---|---|
| `types.rs` | `PxTerm`, `PxBallot`, `PxSlot`, `PxLogEntry`, `Operation`, `PxGroupConfig` (FROZEN end of P1 M1) | 100 | — |
| `messages.rs` | Consensus message enums (`Prepare`, `Promise`, `Accept`, `Accepted`, `Chosen`, `Heartbeat`, `RequestVote`, `Vote`) (FROZEN end of P1 M3) | 100 | — |
| `error.rs` | Typed `Error` enum used across the crate | 30 | — |
| `paxos/acceptor.rs` | In-memory acceptor state, `Prepare`/`Accept` handlers | 150 | 20 |
| `paxos/proposer.rs` | Slot counter, window, admission queue, quorum bitmap | 250 | 30 |
| `paxos/replicator.rs` | Per-peer Accept fan-out + flow control (in-process channels in P1) | 100 | 15 |
| `paxos/repair.rs` | Gap detection, classic Paxos repair task | 150 | 20 |
| `election/elector.rs` | Raft-style election, `RequestVote`, heartbeat, term tracking | 200 | 25 |
| `election/lease.rs` | Leader lease state machine, ReadIndex fallback, step-down trigger (driven by `TestTimer` in P1; same code path used by P4 with real clock) | 120 | 12 |
| `learner.rs` | Apply to engine, resolved-slot, safe-slot aggregation | 150 | 20 |
| `dedup.rs` | Per-client dedup cache | 80 | 15 |
| `group.rs` | Per-group controller wiring all modules; single-group in P1 | 150 | 10 |

| Crate (sibling) | Responsibility |
|---|---|
| `crowkv::engine` | `Engine` trait (FROZEN end of P1 M4) + `InMemoryEngine`. P3 adds `OrderedFileEngine`, snapshot format, `CrowtreeEngine` placeholder. |
| `crowkv::testkit` | `TestTimer`, `TestRouter`, `TestNode`, fault injection. Dev-dep for every crate. |

## 3. Data Types to Implement

Exact Rust types to be defined in `types.rs` (shape frozen at M1 end):

```rust
pub type PxTerm = u64;
pub type PxSlot = u64;
pub type PxNodeId = u64;
pub type PxGroupId = u64;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct PxBallot { pub round: u64, pub leader_id: PxNodeId }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxLogEntry {
    pub slot: PxSlot,
    pub ballot: PxBallot,
    pub term: PxTerm,
    pub kind: LogEntryKind,
    pub payload: Vec<u8>,
    pub client_id: Option<u64>,
    pub seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEntryKind { Write, NoOp, ConfigChange, DedupCheckpoint }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation { pub key: Vec<u8>, pub op: OpKind, pub value: Option<Vec<u8>> }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpKind { Put, Delete }

// PxGroupConfig per design.md §4.4
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupMember {
    pub node_id: PxNodeId,
    pub endpoint: String,
    pub voting: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupConfig {
    pub group_id: PxGroupId,
    pub members: Vec<PxGroupMember>,
    pub quorum_size: usize,
    pub config_version: u64,
}
```

## 4. In-Process Test Harness

Lives in the **`crowkv::testkit`** crate (not `#[cfg(test)]`-gated; consumed as a `dev-dependency` by every crate). The exact API is shared with `test-design.md` §4 (failure injection taxonomy) and must be kept in sync.

- `TestRouter`: holds `Vec<TestNode>`. Methods (all `async`): `deliver_pending()`, `partition(set_a, set_b)`, `heal()`, `delay(from, to, ms)`, `drop(from, to, pct)`.
- `TestTimer`: deterministic monotonic clock. Methods: `now()`, `advance(ms)`, `skew(node, ms)`. Replaces `tokio::time` for fully deterministic time control.
- `TestNode`: wraps all modules for one group member. Methods (all `async`): `propose(req).await`, `tick().await`, `deliver(msg).await`, `crash()`, `restart().await`, `force_step_down()`.
- `SimDisk`: re-exported from `crowkv::io::backend::sim` so WAL tests share the same surface.

No gRPC, no real timers in Phase 1. All concurrency runs on a single-threaded `tokio` runtime + `LocalSet`:
```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
```
Using `start_paused = true` makes `tokio::time` advances explicit; combined with a `Notify`-based step ticker we get fully deterministic, reproducible interleavings.

**Async-everywhere policy:** every public API in P1 is `async`. No blocking calls anywhere in the consensus core; any future blocking syscall (e.g. fsync in P2) is wrapped in `tokio::task::spawn_blocking`. See `plan.md` §8.

## 5. Freeze Checklist

Freeze gates per milestone (downstream phases reference these):
- [ ] **End of M1:** `PxLogEntry` shape + `PxBallot`/`PxTerm` definitions reviewed (P2 WAL depends on this)
- [ ] **End of M3:** Consensus message types finalized (P4 RPC `.proto` derived from this)
- [ ] **End of M4:** Engine trait surface (`apply`, `get`, `scan`, `snapshot_export/import`, `compare`) finalized (P3 implements additional backends)
- [ ] `test_harness.rs` supports deterministic partition, message delay, message loss, node crash, force step-down
- [ ] G1 milestone passes (3-node linearizable writes + leader change)

After M1 freeze, P2 (WAL) and P3 (Storage) may proceed in parallel with P1 M2–M5. P4 (RPC) waits for all P1 freeze points and G1.
