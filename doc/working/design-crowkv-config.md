<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R40 — Unified `CrowKVConfig` (design draft)

## Problem

Configuration was scattered across five structs (`WalConfig`,
`PxElectionConfig`, `PaxosConfig`, `ServerConfig`) and three loose
`PxGroup` bool fields (`force_classic`, `wal_early_ack`,
`async_engine_apply`), wired through 4 `create_group_with_wal` call
sites each passing ~14 individual params pulled from
`KvStoreRegistry` fields. The `mgmt_api` rebuild path
(`rebuild_group_with_same_config`) carried each flag individually — a
pattern that grows a new carry block per flag. T1 (`wal_early_ack`)
and R35 (`async_engine_apply`) each need a new carry block; R36
(proposal coalescing) would add another. The scattered-field pattern
does not scale.

## Current state (partially implemented)

A prior commit (`22f11ed`) created the `CrowKVConfig` struct and wired
it into the outer layers:

- `crowkv/src/common/config.rs` — `CrowKVConfig` with `serde` derives,
  `load_from_file`, `for_tests` / `for_e2e`, convenience accessors.
- `crowkv-server/src/store_registry.rs` — `KvStoreRegistry` holds one
  `CrowKVConfig` (via `with_config`).
- `crowkv-server/src/startup.rs` — `create_group_with_wal` takes
  `&CrowKVConfig` (down from ~14 params) and calls
  `group.set_from_config(config)`.
- `crowkv-server/src/main.rs` — `--config` JSON loading + CLI
  overrides.
- `crowkv-server/src/cli.rs` — `--config` arg.

What remains (this design): the inner `PxGroup` layer still holds the
4 fields individually (`force_classic`, `wal_early_ack`,
`async_engine_apply`, `election_cfg`) and the
`mgmt_api` rebuild still carries them per-flag.

## Proposed approach

### `PxGroup` holds one `CrowKVConfig`

Replace the 4 individual struct fields on `PxGroup` —

- `force_classic: bool`
- `wal_early_ack: bool`
- `async_engine_apply: bool`
- `election_cfg: PxElectionConfig`

— with one field:

```rust
pub(crate) config: CrowKVConfig,
```

The constructor initializes it to match the historical `PxGroup::new`
defaults (all three bools `false`, election `PxElectionConfig::DEFAULT`).
`CrowKVConfig::default()` has `wal_early_ack = true` (flipped by T1 for
the production path), so the constructor uses
`CrowKVConfig { wal_early_ack: false, ..CrowKVConfig::default() }` to
preserve the test-path default. The production path
(`create_group_with_wal → set_from_config`) overwrites this with the
real config (where `wal_early_ack` is true post-T1).

### Setters stay as thin delegating wrappers

The individual setters (`set_force_classic`, `set_wal_early_ack`,
`set_async_engine_apply`, `set_election_config`, `set_inflight_config`)
are kept as thin wrappers that delegate to `self.config.*`. R40's
backlog doc called for replacing them with a single `set_config`; this
design keeps them because removing them would force every test call
site to build a full `CrowKVConfig` and call `set_from_config`, which
is all-or-nothing and would silently flip `wal_early_ack` (and the
other fields) to `CrowKVConfig::default()` values — a behavior change
for ~20 test files. Keeping the wrappers preserves surgical
single-field overrides with zero behavior change, while the structural
goal (one config object on the group) is still met.

`set_inflight_config` is updated to also write
`self.config.paxos.{max_inflight_proposals, inflight_queues,
inflight_admission}` so the held config stays the source of truth for
the admission parameters (the `InflightAdmission` semaphores remain
runtime state, reconstructed from the config params).

### `set_from_config` becomes the bulk setter

```rust
pub fn set_from_config(&mut self, config: &CrowKVConfig) {
    self.config = config.clone();
    self.set_election_config(config.election);
    self.set_inflight_config(
        config.paxos.max_inflight_proposals,
        config.paxos.inflight_queues,
        config.paxos.inflight_admission,
    );
}
```

`set_election_config` mirrors `lease_duration_ms` onto the local
replica (unchanged behavior). `set_inflight_config` reconstructs the
semaphores and syncs `config.paxos`.

### `config()` getter

A new `pub fn config(&self) -> &CrowKVConfig` exposes the held config
by borrow, so the `mgmt_api` rebuild can copy it as one unit.

### `mgmt_api` rebuild collapse

`rebuild_group_with_same_config` replaces the per-flag carry blocks —

```rust
new_group.set_election_config(group.election_config());
if group.force_classic() { new_group.set_force_classic(true); }
if group.wal_early_ack() { new_group.set_wal_early_ack(true); }
if group.async_engine_apply() { new_group.set_async_engine_apply(true); }
new_group.set_inflight_config(...);
```

— with one line:

```rust
new_group.set_from_config(group.config());
```

`proposing_term`, `membership_epoch`, `config_store`, and
`node_config_store` remain separate carries (they are runtime state,
not `CrowKVConfig` fields).

### Read-site updates

Internal field reads change from `self.force_classic` /
`self.wal_early_ack` / `self.async_engine_apply` / `self.election_cfg`
to `self.config.force_classic` / `self.config.wal_early_ack` /
`self.config.async_engine_apply` / `self.config.election`. Affected
files: `group.rs` (4 read sites + struct literal), `group_election.rs`
(5 sites), `group_maintenance.rs` (4 sites). The public getters
(`force_classic()`, `wal_early_ack()`, `async_engine_apply()`,
`election_config()`) remain and delegate to `self.config.*`, so
external callers (tests, mgmt_api, client) are unchanged.

## Alternatives considered

- **Remove the individual setters entirely (full R40 backlog vision).**
  Rejected: `set_from_config` is all-or-nothing; migrating ~20 test
  files that today call `set_election_config(for_tests())` or
  `set_force_classic(true)` would silently set `wal_early_ack` to
  `CrowKVConfig::default()`'s `true` (post-T1), changing test
  semantics for multi-node clusters where early-ack affects the
  write-ack contract. Keeping thin wrappers preserves surgical
  overrides with zero behavior change while still unifying the
  storage into one config object.
- **Drop `wal_backend` / `crowtree_backend` params from
  `create_group_with_wal` (acceptance criterion says 4 identity
  params).** Deferred: `IoBackend::MemBlock` / `BlockDevice` carry
  device state that is shared from the registry's single `Arc`;
  re-deriving per call would give each group a fresh device, a
  behavior change for `mem-block` tests. The 7-param form (down from
  14) already achieves the param-reduction goal.

## Acceptance test plan

- `cargo test -p crowkv` (paxos + group + store suites) — all pass
  unchanged (setters still work, no test edits needed).
- `cargo test -p crowkv-server` (startup + mgmt-api) — all pass.
- `rebuild_group_with_same_config` carries one config object
  (verified by reading the diff: no per-flag blocks).
- `PxGroup` has one `config: CrowKVConfig` field; the 4 individual
  fields are gone.
