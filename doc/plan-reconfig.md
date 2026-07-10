# CrowKV - Plan: Reconfiguration and Snapshots

Depends on: [`plan.md`](plan.md), [`design-reconfiguration.md`](design-reconfiguration.md), [`plan-rpc.md`](plan-rpc.md), [`plan-wal.md`](plan-wal.md), [`plan-storage.md`](plan-storage.md)
Satisfies: [requirement.md §9.1](requirement.md#91-reconfiguration), [requirement.md §9.2](requirement.md#92-rolling-upgrade)

Phase 5: joint consensus, snapshot install, rolling upgrade. Builds on the snapshot streaming protocol frozen in P4 M2 and the engine snapshot export/import from P3 M3.

## 1. Milestones

### M1 — Snapshot install protocol

- Wire the P4 `SnapshotService` to `Engine::snapshot_export` / `snapshot_import`.
- Resumable chunked transfer with `(snapshot_id, chunk_offset)` checkpointing on the receiver; restart-after-failure picks up at last successful offset ([`design-storage-engine.md`](design-storage-engine.md) §6.3).
- End-to-end CRC verified before activation; throttleable via `chunk_rate_bytes_per_sec` config.
- New-node bootstrap path: receive snapshot at slot S, then catch up via WAL streaming for `[S+1, current_max_chosen]`.

**Acceptance:** new empty node added to running 3-node group, snapshot installs, node catches up, joins quorum, `compare()` equals existing learners.

### M2 — Joint consensus state machine

- Implement `ConfigChange(joint = C_old ∪ C_new)` and `ConfigChange(C_new)` log entries.
- Both-quorum decision rule active while joint config is *applied* (not merely chosen).
- New members join as non-voting catch-up readers during joint phase ([`design-reconfiguration.md`](design-reconfiguration.md) §3, §4).
- Failure recovery: roll back to `C_old` if catch-up exceeds `catchup_timeout`.

**Acceptance:** 3 → 5 single-member add succeeds online; failed catch-up rolls back cleanly to 3.

### M3 — Leader transfer

- `TimeoutNow` RPC instructs target follower to start election immediately at `term + 1`.
- Pre-condition: target's `contiguous_applied == leader's max_chosen`.
- Used during leader-removal reconfig; also exposed as admin RPC for planned maintenance.

**Acceptance:** explicit transfer completes within `leader_transfer_timeout` (default 5 s); old leader steps down; client requests redirected via `NotLeaderHint`.

### M4 — Rolling upgrade

- Version header in `WALRecord`, snapshot, and protobuf messages enforced ([requirement.md §9.2](requirement.md#92-rolling-upgrade)).
- `config_version` in Group-0 prevents older binary from joining a newer-format cluster.
- Operational procedure documented in `README.md`: stop one node → upgrade binary → restart → wait for catch-up → repeat.
- **Upgrade test scope:** consensus protocol compatibility only. WAL + snapshot version compatibility is out of scope for the rolling-upgrade test (no requirement to keep older WAL/snapshot code paths around for testing).

**Acceptance:** mixed-version cluster (one node N+1, two nodes N) serves traffic without divergence; full upgrade completes without write unavailability.

## 2. Module Breakdown

| Rust module | Responsibility |
|---|---|
| `reconfig/joint.rs` | Joint-consensus state machine, both-quorum evaluator |
| `reconfig/transfer.rs` | Leader transfer (`TimeoutNow`) |
| `reconfig/membership.rs` | `PxGroupConfig` mutation, voting/non-voting flags |
| `snapshot/install.rs` | Snapshot install state machine (chunked, resumable, throttled) |
| `snapshot/store.rs` | On-disk snapshot file management, atomic swap |
| `upgrade/version.rs` | Version negotiation, format compatibility checks |

## 3. Freeze Checklist

Release gate:
- [ ] G5 passes: 3 → 5 → 7 online, zero crowbench divergence
- [ ] Rolling binary upgrade 1 version step succeeds in CI
- [ ] Snapshot install resumes correctly after simulated network failure
- [ ] Failed catch-up rolls back cleanly without leaving the cluster in joint mode

## 4. Out-of-Scope for P5

- Group split / merge (out of scope per [requirement.md §2](requirement.md#2-non-goals-out-of-scope))
- Cross-group transactions (out of scope)
- Multi-version compatibility beyond N±1 (out of scope per [requirement.md §9.2](requirement.md#92-rolling-upgrade))
