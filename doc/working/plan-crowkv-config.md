<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R40 — Implementation Plan

## Task breakdown

- [ ] **T1: `PxGroup` struct + constructor** — replace `force_classic`,
      `wal_early_ack`, `async_engine_apply`, `election_cfg` fields with
      `config: CrowKVConfig`. Update `PxGroup::new` to init
      `CrowKVConfig { wal_early_ack: false, ..CrowKVConfig::default() }`.
      Import `CrowKVConfig` in `group.rs`.
- [ ] **T2: `PxGroup` setters/getters** — rewrite `set_from_config` to
      `self.config = config.clone()` + delegate calls. Rewrite
      `set_force_classic` / `set_wal_early_ack` / `set_async_engine_apply`
      to delegate to `self.config.*`. Rewrite `set_inflight_config` to
      also sync `self.config.paxos`. Rewrite getters to delegate. Add
      `pub fn config(&self) -> &CrowKVConfig`.
- [ ] **T3: `group_election.rs`** — `election_config()` returns
      `self.config.election`; `set_election_config` sets
      `self.config.election`; update 3 read sites (161, 179, 747) to
      `self.config.election`.
- [ ] **T4: `group_maintenance.rs`** — update 4 `group.election_cfg.*`
      reads to `group.config.election.*`.
- [ ] **T5: `group.rs` internal reads** — update line 878 (struct
      literal), 1232, 1315, 1739 to `self.config.*`.
- [ ] **T6: `mgmt_api` rebuild** — collapse per-flag blocks in
      `rebuild_group_with_same_config` into
      `new_group.set_from_config(group.config())`.
- [ ] **T7: Build + relevant tests** — `pixi run cargo build`, then
      `pixi run test-core` and `pixi run test-server`. Fix failures
      (up to 3 retries).
- [ ] **T8: Commit** — implementation + design + plan docs.
- [ ] **T9: Full test suite** — `pixi run test-suite`.
- [ ] **T10: Merge design** — fold into `design/design.md` (§11 or a
      new config subsection). Delete working docs + R40 backlog entry.
- [ ] **T11: Local CI** — fmt, clippy, test-ct, test-ffi, test-core.

## Files

- `crowkv/src/cluster/group.rs` — struct, constructor, setters,
  getters, internal reads.
- `crowkv/src/cluster/group_election.rs` — trait impl + reads.
- `crowkv/src/cluster/group_maintenance.rs` — reads.
- `crowkv-server/src/mgmt_api.rs` — rebuild collapse.

## Test checklist

- [ ] `pixi run test-core` (paxos, group, store suites)
- [ ] `pixi run test-server` (startup, mgmt-api)
- [ ] `pixi run test-suite` (full)
