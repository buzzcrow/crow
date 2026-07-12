# Bug tracker: restart-recovery (open problems)

The WAL / state-machine **durability design** now lives in
[`design/design-wal.md`](design/design-wal.md) (Model A: one consensus WAL,
derived `KVEngine`, snapshot-based engine persistence; §6 covers replay /
restore / recovery / steady-state apply). **This doc tracks only the open
problems** in the full-cluster restart-recovery work.

## Symptom

Web e2e `crowkv-console/web/tests/cluster_restart_recovery_test`: seed puts,
delete a subset, wait for the deletes to converge on every replica, then restart
**every** node. Historically failed at three points — 425 (pre-restart
convergence), 336 (missing committed put), 343 (resurrected delete).

## Fixed (all 46 `crowkv` cluster tests pass)

- **425 convergence** — followers now apply committed entries on heartbeat
  (`apply_committed_up_to`, design-wal.md §6.5).
- **336 missing-put** — `note_chosen` no longer advances `last_chosen_slot` from
  the payload-less `ChosenNotice`, so election no longer picks a value-missing
  replica (see Open #2 for the regression this introduced).
- **`quorum == 1` restore window** — the election driver is deferred until
  remotes are wired (`PxKvStore::add_group_without_election` + the
  `start_election` flag on `add_group`; web defers for multi-replica groups).

## Open problems

### 1. Resurrection (web e2e line 343) — web layer, not consensus

A previously-deleted key reappears after full restart. The crowkv consensus
layer is **verified clean**: the fast repro
`crowkv/tests/cluster/full_restart_delete_test.rs::full_cluster_restart_keeps_deletes_3node_scaled`
(3 replicas, 60 keys, deletes across the whole range, through the `quorum=1`
restore window) **passes** — *every* replica's local engine converges to deleted
after restart (stricter than the web's forwarded read).

The web read is `read_mode = 0` (linearizable), routed via
`monitor_cache.leader_for` + a `NotLeader` retry (`web/src/kv.rs`). So the
resurrection is the web **resolving / forwarding the read to a node that
transiently serves a stale value** after restart — e.g. monitor-cache
leader / `listen_addr` staleness (per-store gRPC ports are ephemeral and change
on restart), or a node that briefly believes it is leader — **not** a consensus
loss.

**Next step:** investigate the web post-restart leader resolution / read
routing; consider gating reads until the group reports a stable healthy leader,
and hardening cache refresh after restart.

### 2. Election up-to-date check regresses the dynamic-replica test

Neutering `note_chosen` (the 336 fix) makes `crowkv-server`
`e2e_kv_after_dynamic_replica_change` fail (stale read after a membership-change
delete). It passes alone, fails in-suite. **Confirmed not caused by the
deferred-driver change** (that test creates groups via CLI bootstrap and changes
membership via `add_remote_replicas` / `remove_remote_replica`, neither of which
uses `start_election`).

**Proper fix:** base the election log-up-to-date check
(`candidate_log_up_to_date`) on the **durable acceptor log tip** (highest
*accepted* slot + its term) instead of the learner `last_chosen_slot` watermark.
Then `note_chosen` need not be neutered and both tests pass.
`PxAcceptor` exposes `highest_seen_slot` + `accepted_at` but no direct
highest-accepted-slot-with-term accessor yet. *Left failing for now per plan.*

### 3. Recovery floor is unbounded-or-stale (needs P2 watermark)

`bulk_phase1` floors at `contiguous_chosen` (which restore over-claims from
merely-*accepted* slots). `floor = 0` is correct but too slow (~100s e2e,
timeouts). A bounded, correct floor needs the **P2 durable commit watermark**.

### Roadmap (design in `design-wal.md`)

- **P2** — durable commit watermark (fast bounded recovery + truncation; also
  fixes Open #3).
- **P3** — `KVEngine` snapshot + applied-index (`SnapshotMarker`); wire
  `compute_gc_slot`'s `snapshot_slot`.
- **P4** — persist `ConfigChange` membership; health-gate `bulk_phase1` /
  `repair_once` on a reachable quorum of configured voters.

## Repro commands

```bash
# Fast, crowkv-layer:
cargo test -p crowkv --test cluster full_cluster_restart_keeps_deletes

# Full web e2e (~40s, currently flaky across lines 425/343/336):
cargo test -p crowkv-web --test cluster_restart_recovery_test
```
