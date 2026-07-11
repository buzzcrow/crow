# Leader Election Implementation Review Gaps

Reviewed against `doc/design/design-leader-election.md` and the `/review` workflow checklist.

## 1. Comments — doc/ references in code (Checklist #2)

- `crowkv/src/cluster/election.rs:3-4` — module doc references `doc/design/design-leader-election.md` §3/§5/§6/§8.
- `crowkv/src/cluster/local_replica.rs:743` — `handle_step_down` doc comment references `doc/design/design-leader-election.md` §8.

**Rule:** No `doc/`, `plan.md`, etc. references in code comments. Design rationale and step numbers belong in the design doc only; code comments should explain *why* the code does what it does, not where the spec lives.

## 2. Debug — `finish()` vs `finish_non_exhaustive()` (Checklist #11)

- `crowkv/src/cluster/group.rs:88` — manual `Debug` for `PxGroup` uses `.finish()` instead of `.finish_non_exhaustive()`.

All other public structs in the cluster layer (`PxLocalReplica`, `PxRemoteReplica`, `PxElectionConfig`, etc.) already use `finish_non_exhaustive()` or derive `Debug`. `PxGroup` should be consistent.

## 3. `group.leader_id` never updated by election driver (Functional gap)

- `crowkv/src/cluster/group.rs:28` — `leader_id` is a `pub` field.
- `crowkv/src/cluster/election.rs:703` — `finalize_leader` calls `replica.become_leader()` and `group.stamp_proposing_term(term)` but never sets `group.leader_id = replica.id`.
- `crowkv/src/cluster/group.rs:186` — `is_leader()` compares `self.leader_id == self.local_replica.id`.
- `crowkv/src/cluster/group.rs:211` — `leader_endpoint()` branches on `self.is_leader()`.
- `crowkv/src/cluster/group.rs:349` — `report_health()` uses `self.leader_id == self.local_replica.id` to report role.
- `crowkv/src/cluster/group.rs:291` — `snapshot()` includes `leader_id`.

**Impact:** For election-driven groups (non-testkit), `is_leader()`, `leader_endpoint()`, `report_health()`, and `snapshot()` all return stale / incorrect data because `leader_id` remains at its initial value (`0`). The propose gate fortunately uses `replica.role() == Leader`, so writes are not affected, but health/topology/forwarding logic is broken.

**Fix:** Update `leader_id` in `finalize_leader` and clear it on step-down. Alternatively, deprecate the field and drive everything from `replica.role()` + `proposing_term()`.

## 4. Hard-coded default lease duration in vote/heartbeat handlers (Bug)

- `crowkv/src/cluster/local_replica.rs:643` — `handle_request_vote` uses `PxElectionConfig::DEFAULT.lease_duration_ms`.
- `crowkv/src/cluster/local_replica.rs:695` — `handle_heartbeat` uses the same hard-coded default.

**Impact:** If a group sets a custom `lease_duration_ms` (e.g., WAN profile), the follower's `vote_lockout_until` extension and heartbeat lease grant still use the default `4500 ms`. This breaks the lease safety argument for non-default configs.

**Fix:** `PxLocalReplica` needs to know its group's election config, or the config must be passed through the handler call sites (`ReplicaHandler` trait methods currently don't take config).

## 5. Mutex on heartbeat hot path (Hot-path rule)

- `crowkv/src/cluster/local_replica.rs:163` — `lease_state: Mutex<LeaseState>`.
- `crowkv/src/cluster/election.rs:411-422` — `renew_lease` calls `replica.with_lease_state(|s| { ... })` on every heartbeat tick that achieves quorum.
- `crowkv/src/cluster/local_replica.rs:338` — `lease_state_snapshot()` clones the whole `LeaseState` under the mutex.

**Impact:** Every successful heartbeat round-trip takes a mutex to update two `Instant` values. Under high load this contends with concurrent reads (e.g., metrics snapshots, `Get(Linearizable)` lease checks in M5).

**Fix:** Convert `lease_read_until` and `last_quorum_heartbeat_at` to atomic `AtomicU64` storing monotonic millis since process start (or a lazy static anchor). The design doc §6.4 already requires monotonic-only math, so an atomic representation is feasible. Keep the mutex only for complex state transitions.

## 6. Missing module-level doc comment

- `crowkv/src/cluster/remote_replica.rs` — no `//!` module comment. The file is 496 lines and contains the gRPC client adapter, `PeerStream` integration, and `ReplicaClient` impl. A brief `//!` summarizing its role and key work areas is needed per checklist #2.

## 7. Visibility — `PendingLeaderHandoff` could be `pub(crate)`

- `crowkv/src/cluster/group.rs:71` — `pub struct PendingLeaderHandoff` is marked `pub` but is only used inside `crowkv/src/cluster/` (consumed by `election.rs`).

**Fix:** `pub(crate)` is sufficient and reduces the public API surface.

## 8. `t_send_ms_mono` uses wall-clock `SystemTime`, not monotonic (Design doc §6.4 deviation)

- `crowkv/src/cluster/election.rs:348-351` — computes `t_send_ms_mono` from `SystemTime::now()`.
- Design doc §6.4: "All lease math uses the **monotonic clock**, not wall-clock."

**Impact:** The wire timestamp is only used for relative ordering (the comment acknowledges this), but the field name `*_mono` promises monotonicity. If a peer ever uses this value for absolute-time arithmetic, NTP steps could violate the lease invariant.

**Fix:** Rename to `t_send_ms_wall` or compute from a process-start anchor + `StdInstant::elapsed()`. Document the deviation if intentional.

## 9. `update_member_endpoint` out-of-range behavior is confusing

- `crowkv/src/cluster/group.rs:257-272` — when `node_id` is out of range, returns `Some(endpoint)` (the *new* endpoint), making the caller think an old endpoint was replaced.

**Fix:** Return `None` when the replica doesn't exist, or resize the vec and insert a placeholder. This is not leader-election specific but was noticed during review.

---

## Summary Table

| # | Category | File(s) | Severity | Effort |
|---|----------|---------|----------|--------|
| 1 | Comments | `election.rs`, `local_replica.rs` | Low | 5 min |
| 2 | Debug | `group.rs` | Low | 1 min |
| 3 | Functional | `election.rs`, `group.rs` | **High** | 15 min |
| 4 | Config bug | `local_replica.rs` | **High** | 30 min |
| 5 | Hot-path mutex | `local_replica.rs`, `election.rs` | Medium | 1-2 h |
| 6 | Comments | `remote_replica.rs` | Low | 5 min |
| 7 | Visibility | `group.rs` | Low | 1 min |
| 8 | Clock semantics | `election.rs` | Medium | 15 min |
| 9 | API confusion | `group.rs` | Low | 10 min |
