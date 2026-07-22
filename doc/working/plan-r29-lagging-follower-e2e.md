<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R29 Plan — Lagging-follower e2e for MinSlot fallback

Test-only change. See `design-r29-lagging-follower-e2e.md` for the
mechanism (corrects R29's stated approach: the lagging follower must NOT
be in the leader's accept/notice fan-out, since fan-out ignores `voting`).

## Tasks

- [ ] Generalize `start_two_node_cluster` → `start_three_node_cluster` in
      `e2e_follower_read_test.rs`:
      - A (id 1, Leader, voting, remotes `[B]`, election driver on).
      - B (id 2, Follower, voting, remotes `[A]`, election driver on).
      - C (id 3, Follower, non-voting via `with_voting(false)`, remotes
        `[A]`, election driver off via `add_group_without_election`).
      - Returns `(leader, follower, lagging)` = (A, B, C).
- [ ] Replace `spawn_topology_server(store)` with a variant that serves a
      hand-crafted `StoreStatus`: A's real `status()` with C appended to
      group 1's `remotes` (`RemoteStatus { id: 3, endpoint: C, voting:
      false, ..default() }`). Replica list → `[A, B, C]`.
- [ ] Update module doc comment to describe the 3-node lagging-follower
      topology and why C is not wired on A.
- [ ] Adapt `any_replica_distributes_minslot_reads` →
      `any_replica_distributes_minslot_reads_with_lagging_follower`
      (`min_slot = 0`): expect `Found` (A/B) or `NotFound` (C); assert both
      branches fire; `read_endpoint_distributed >= 6`;
      `read_endpoint_fallback == 0`.
- [ ] Adapt `any_replica_scan_distributes` →
      `any_replica_scan_distributes_with_lagging_follower`
      (`min_slot = 0`): expect 1 item (A/B) or 0 items (C); both branches
      fire; `read_endpoint_distributed >= 6`; `read_endpoint_fallback == 0`.
- [ ] Add `any_replica_falls_back_to_leader_when_follower_lags` (get,
      `min_slot = write.revision`): 6 reads, all `Found`,
      `read_endpoint_distributed >= 6`, `read_endpoint_fallback >= 1`.
- [ ] Add `any_replica_scan_falls_back_when_follower_lags` (scan,
      `min_slot = write.revision`): 6 scans, all 1 item,
      `read_endpoint_distributed >= 6`, `read_endpoint_fallback >= 1`.
- [ ] Update `any_replica_linearizable_still_targets_leader` and
      `leader_policy_unchanged_for_minslot` to use the 3-node helper and
      stop/join all 3 nodes (logic unchanged — reads target A).
- [ ] `follow_scan_not_leader_parser_extracts_endpoint` unchanged (no
      cluster).

## Files

- `crowkv-client/tests/e2e_follower_read_test.rs` — only file changed.

## Test checklist

- [ ] `pixi run test-core` (client e2e tests live under `crowkv-client`;
      run the specific binary: `cargo test -p crowkv-client --test
      e2e_follower_read_test`).
- [ ] All 7 tests in the file pass.
- [ ] `cargo fmt --check` + `cargo clippy -- -D warnings` clean.
