# CrowKV Ignored / Pending Test Plan

This document tracks `#[ignore]` tests and open issues, organized by the same
layer structure as `test.md`:

```
store      <- crowkv/tests/store.rs
  group    <- crowkv/tests/group.rs
    replica  <- crowkv/tests/replica.rs
      wal / slot <- crowkv/tests/{wal,slot}.rs
        unit <- crowkv/tests/{paxos,kv}.rs
deployment <- crowkv-server/tests/*, crowkv-console/{shared,web}/tests/*
```

## 1. Ignored Tests by Layer

### 1.1 Unit layer (`paxos.rs`, `kv.rs`)

No ignored tests.

### 1.2 WAL / Slot subsystem layer (`wal.rs`, `slot.rs`)

No ignored tests.

### 1.3 Replica layer (`replica.rs`)

No ignored tests.

### 1.4 Group layer (`group.rs`)

| Test | File | Line | Ignore Reason | Analysis / Path to Re-enable |
|------|------|------|---------------|------------------------------|
| `cluster_survives_leader_kill_and_restart_with_no_data_loss` | `tests/group/g2_crash_restart_no_data_loss_test.rs` | 340 | "W10 proposing_term readiness is fixed; this test now fails on data survival after leader restart (read returns None for committed keys). This is a repair / divergence issue after a restart, separate from leadership readiness. Needs further W11-style repair-correctness work before re-enable." | Leadership-readiness fixes (stamping `proposing_term` on single-replica leaders) are complete. The remaining failure is committed keys returning `None` after a leader is killed and restarted, indicating that the new leader's repair / learner state diverges from the previous leader's chosen prefix. This is a W11-style repair-correctness issue. |

### 1.5 Store layer (`store.rs`)

No ignored tests.

### 1.6 Deployment layer (`crowkv-server`, `crowkv-console`)

| Test | File | Line | Ignore Reason | Analysis / Path to Re-enable |
|------|------|------|---------------|------------------------------|
| `e2e_three_node_cluster_kv_put_batch_delete` | `crowkv-server/tests/cluster_e2e_test.rs` | 202 | "test isolation issue: passes individually but fails in full suite" | Needs investigation into shared state / port / process isolation when the whole suite runs together. |
| `e2e_multi_group_isolated_kv` | `crowkv-server/tests/cluster_e2e_test.rs` | 698 | "test isolation issue: passes individually but fails in full suite" | Same class as above: test isolation problem when run in a multi-test process. |
| `e2e_kv_after_dynamic_replica_change` | `crowkv-server/tests/cluster_e2e_test.rs` | 772 | "W9 fixes restart-window quorum=1 self-election; this test still hits a live tenure-cancel race during shrink (post-shrink delete overwritten by stale repair). Needs additional in-flight repair cancellation before re-enable." | A 5→3 replica shrink is followed by a delete on the surviving quorum. The old driver's `tenure_cancel()` is called synchronously, but an in-flight `repair_once` / `run_bulk_phase1` from the prior 5-replica tenure can still land a stale `NoOp` at the delete slot after the delete is chosen but before the new driver stabilises. The test passes with `CROWKV_TEST_LOG=1` (slower scheduling) and fails at full speed. The fix requires (a) W9-style health-gating so stale repairs cannot commit without a reachable configured quorum, and (b) awaiting in-flight repair completion before the new group accepts proposals. This is the same root-cause class as the W11 cluster restart delete regression. |
| `deploy_local_and_observe_topology` | `crowkv-console/shared/tests/lifecycle_e2e_test.rs` | 26 | "flaky: pick_free_port can return same port for mgmt and grpc causing validation failure" | `pick_free_port` binds a transient TCP socket to port 0, reads the port, and immediately drops it. The OS can reuse that port for the next call, so the management and gRPC ports collide and the server rejects the config. Fix by keeping both listener sockets alive until the real server binds them, or by using a single allocator that reserves two distinct ports atomically. |
| `cluster_restart_restores_multistore_groups_and_kv` | `crowkv-console/web/tests/cluster_restart_recovery_test.rs` | 432 | "W8 web-level endpoint refresh is now hardened, but the test fails on a separate crowkv-level data issue: deleted keys (e.g. 12/2/k60) are resurrected after a full cluster restart. Re-enable once the WAL replay / GC delete-survival bug (W11) is fixed." | The W8 web read-routing hardening (monitor-cache refresh, healthy-leader gating, `NotLeader` retry) is complete. The test now fails because a deleted key is visible after the full cluster restart. This is a data-level bug, not a routing bug: the deleted slot is being overwritten by a stale repair or the delete is not surviving replay correctly. Re-enable after W11 is fixed. |

## 2. Open Issues

### 2.1 Live tenure-cancel race (stale repair overwrites committed delete)

A 5→3 replica shrink rebuilds the surviving nodes while the old 5-replica driver's repair task may still be in flight. The synchronous `tenure_cancel()` does not wait for `repair_once` / `run_bulk_phase1` to finish, so a stale repair can land a `NoOp` at the just-chosen delete slot. This is the same quorum=1 / stale-repair class as the fixed W11 cluster restart bug, but triggered by a live tenure change rather than a restart window.

**Fix needed:** (a) ensure the new driver cannot accept proposals until in-flight repairs from the old tenure are cancelled or completed, and (b) rely on W9's health-gated repair so a stale task cannot commit without a reachable configured quorum.

**Blocked tests:** `e2e_kv_after_dynamic_replica_change` (§1.6).

### 2.2 Deleted keys resurrect after full cluster restart

After a full cluster restart, a deleted key is readable again. The W8 web routing layer is not at fault: the console correctly refreshes the monitor cache, gates on a healthy leader, and retries `NotLeader`. The resurrection happens at the crowkv data layer. Possible causes:

- A stale repair overwrites the `Delete` slot with a `NoOp` (same as §2.1).
- The learner / KV engine re-applies an old snapshot or accepted value that predates the delete.
- The durable-commit watermark does not cover the delete slot, and replay applies a stale accepted value above the watermark.

**Blocked tests:** `cluster_restart_restores_multistore_groups_and_kv` (§1.6).

### 2.3 Data loss after leader restart

After the old leader is killed and a new leader takes over, committed keys return `None`. The new leader's learner watermark or accepted log is behind the previous leader's chosen prefix, and the repair process does not correctly backfill the gap.

**Blocked tests:** `cluster_survives_leader_kill_and_restart_with_no_data_loss` (§1.4).

### 2.4 Test infrastructure flakiness

- `pick_free_port` reuse: `pick_free_port` binds a transient TCP socket to port 0, reads the port, and immediately drops it. The OS can reuse that port for the next call, so management and gRPC ports collide. Fix by keeping listener sockets alive until the real server binds them.
- Test isolation in `crowkv-server` cluster E2E: `e2e_three_node_cluster_kv_put_batch_delete` and `e2e_multi_group_isolated_kv` pass individually but fail in full suite. Needs investigation into shared state / port / process isolation.

**Blocked tests:** `deploy_local_and_observe_topology` (§1.6), `e2e_three_node_cluster_kv_put_batch_delete` (§1.6), `e2e_multi_group_isolated_kv` (§1.6).

### 2.5 WAL GC safe slot not integrated

`crowkv/src/wal/gc.rs:61` uses `safe_slot = u64::MAX` because `run_gc_pass` only has access to `WalEngine`. The real safe GC bound should be the group's `contiguous_applied` (or the durable-commit watermark / snapshot slot, whichever is lowest). The GC worker needs to receive the safe slot from the owning group, or `run_gc_pass` needs to be called by the group with its current applied watermark.

### 2.6 W6 multi-node recovery test

A full multi-node cluster recovery test for slots above the durable-commit watermark (recovered by bulk Phase 1 / heartbeat catch-up) is not yet added. The recovery-floor mechanism is implemented (`restore_from_replay` advances `contiguous_chosen` to the watermark, `run_bulk_phase1` uses it as the floor), but the integration test is postponed until §2.1–§2.3 are stable.

## 3. Repro Commands

```bash
# Fast, crowkv-layer (passes):
cargo test -p crowkv --test group full_cluster_restart_keeps_deletes

# Full web e2e (~40s, currently ignored — see §1.6):
cargo test -p crowkv-web --test cluster_restart_recovery_test -- --ignored

# Dynamic replica change (currently ignored — see §1.6):
cargo test -p crowkv-server --test cluster_e2e_test -- --ignored e2e_kv_after_dynamic_replica_change

# Leader kill/restart (currently ignored — see §1.4):
cargo test -p crowkv --test group -- --ignored cluster_survives_leader_kill_and_restart_with_no_data_loss
```

## 4. Suggested Fix Order

1. **Fix `pick_free_port`** and re-enable `deploy_local_and_observe_topology` (pure test infra).
2. **Investigate / fix cluster E2E test isolation** for `e2e_three_node_cluster_kv_put_batch_delete` and `e2e_multi_group_isolated_kv` (test infra).
3. **Close the live tenure-cancel race** so stale repairs cannot overwrite committed deletes.
4. **Re-run and re-enable** `e2e_kv_after_dynamic_replica_change`, `cluster_restart_restores_multistore_groups_and_kv`, and `cluster_survives_leader_kill_and_restart_with_no_data_loss`.
5. **Integrate group `contiguous_applied` into GC safe slot** and add a dedicated GC test for it.
6. **Add the full W6 multi-node recovery test** for slots above the durable-commit watermark.
