<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R40: Unified `CrowKVConfig` (merge all sub-configs, JSON file loading)

**Problem**: Configuration is scattered across five structs and three
loose `PxGroup` bool fields, wired through 4 `create_group_with_wal`
call sites each passing ~14 individual params pulled from
`KvStoreRegistry` fields. The `mgmt_api` rebuild path
(`rebuild_group_with_config`, L1565) carries each flag individually
(`force_classic` block at L1572, `election_config()` at L1557,
`inflight_config` at L1582) — a pattern that grows a new block per
flag. T1 (`wal_early_ack`) and R35 (`async_engine_apply`) each need a
new carry block; R36 (proposal coalescing) will add another. The
scattered-field pattern does not scale.

Current config landscape:
- `WalConfig` (`config.rs` L96) — 12 fields, per-group WAL tunables.
- `PxElectionConfig` (`config.rs` L188) — 12 fields, per-group
  election/heartbeat/lease/maintenance tunables, with `DEFAULT` /
  `for_tests` / `for_e2e` profiles.
- `PaxosConfig` (`config.rs` L49) — 6 fields, global Paxos retry +
  admission tunables.
- `ServerConfig` (`config.rs` L81) — 1 field, shutdown timeout.
- `PxGroup` internal flags (`group.rs` L68/L73/L79) — `force_classic`,
  `wal_early_ack`, `async_engine_apply`, each with its own
  setter/getter and `mgmt_api` carry block.
- `KvStoreRegistry` (`store_registry.rs` L38) — holds `election_cfg`,
  `wal_root`, `config_root`, `wal_backend`, `data_root`,
  `crowtree_backend`, `wal_skip_fsync`, `max_inflight`,
  `inflight_queues` as individual fields, set via builder methods from
  CLI args in `main.rs` L108–117.

No config file exists today. All config is programmatic: `main.rs`
parses CLI args (`cli.rs`) and populates `KvStoreRegistry` via builder
methods; `mgmt_api` reads `KvStoreRegistry` fields; tests construct
`PxElectionConfig::for_tests()` and call `set_election_config` /
`set_inflight_config` / `set_force_classic` individually.

**Approach**: Merge all sub-configs into one `CrowKVConfig` with `serde`
derives, loaded from a JSON config file. CLI args override individual
fields after loading.

### `CrowKVConfig` struct

```rust
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CrowKVConfig {
    pub server: ServerConfig,
    pub paxos: PaxosConfig,
    pub election: PxElectionConfig,
    pub wal: WalConfig,
    // Former PxGroup internal flags — now first-class config fields.
    pub force_classic: bool,
    pub wal_early_ack: bool,
    pub async_engine_apply: bool,
    // Runtime paths / backends (not serde-serialized — set from CLI).
    #[serde(skip)]
    pub wal_root: PathBuf,
    #[serde(skip)]
    pub config_root: PathBuf,
    #[serde(skip)]
    pub data_root: PathBuf,
    #[serde(skip)]
    pub wal_backend: WalBackend,
    #[serde(skip)]
    pub crowtree_backend: CrowtreeBackend,
    #[serde(skip)]
    pub wal_skip_fsync: bool,
}
```

Sub-structs (`WalConfig`, `PxElectionConfig`, `PaxosConfig`,
`ServerConfig`) gain `serde::Deserialize`/`Serialize` derives and
`#[serde(default)]` on fields so a partial JSON file only overrides the
listed keys. The sub-structs remain as nested types (not flattened) so
the JSON file has a clear hierarchical structure:

```json
{
  "server":    { "shutdown_timeout_ms": 10000 },
  "paxos":     { "max_inflight_proposals": 32, "inflight_admission": "queue" },
  "election":  { "heartbeat_interval_ms": 150, "lease_duration_ms": 3000 },
  "wal":       { "wal_segment_size": 67108864, "wal_flush_coalesce_us": 0 },
  "force_classic": false,
  "wal_early_ack": true,
  "async_engine_apply": false
}
```

### Config file format: JSON

**Chosen: JSON.** Zero new dependencies on either side:
- Rust: `serde_json` is already a dep in `crowkv` and `crowkv-server`
  (`Cargo.toml`).
- C++: `boost::json` is available in the pixi env
  (`.pixi/envs/default/include/boost/json.hpp`) if standalone C++ tools
  ever need to read/write the config. The C++ engine itself does not
  parse config files today — it receives `crowtree::Options` via FFI
  from Rust — but `boost::json` is ready if needed.

**Alternatives considered:**
- **TOML.** Nicer for hand-editing (comments, less punctuation), but
  requires adding `toml` crate to Rust and `toml++` to C++ — new deps
  on both sides for a file that's largely machine-managed. Rejected.
- **YAML.** Same dep cost as TOML, plus YAML's ambiguity issues.
  Rejected.

### Loading + CLI override

1. `main.rs` adds `--config <path>` CLI arg (optional). When present,
   `CrowKVConfig::load_from_file(path)` deserializes the JSON file.
   When absent, `CrowKVConfig::default()` is used (same defaults as
   today).
2. CLI args override individual fields after loading — e.g.
   `--max-inflight 64` sets `config.paxos.max_inflight_proposals = 64`,
   `--no-fsync` sets `config.wal_skip_fsync = true`,
   `--election-profile test` sets
   `config.election = PxElectionConfig::for_tests()`. This preserves
   the existing CLI ergonomics; the config file is the base, CLI is the
   override.
3. `KvStoreRegistry` holds one `CrowKVConfig` instead of ~9 individual
   fields. The builder methods (`with_data_root`, etc.) become
   `config.wal_root = …` assignments or are replaced by
   `config.override_from_cli(&args)`.

### `create_group_with_wal` simplification

Today: 14 params. After R40: the function takes `&CrowKVConfig` (plus
`store_id`, `group_id`, `replica_id`, `initial_role` — the per-call
identity params). The 4 call sites in `main.rs` + `mgmt_api.rs` shrink
from ~14 lines each to ~5.

### `mgmt_api` rebuild-carry simplification

Today (`rebuild_group_with_config`, L1565): carries `membership_epoch`,
`force_classic`, `config_store`, `node_config_store`, `inflight_config`
individually. After R40: the full `CrowKVConfig` is one object on
`PxGroup` (or held by the registry and passed through); rebuild copies
it as one unit. The per-flag carry blocks collapse into one
`new_group.set_config(group.config().clone())`.

### `PxGroup` changes

The three bool flags (`force_classic`, `wal_early_ack`,
`async_engine_apply`) move from `PxGroup` struct fields into the
`CrowKVConfig` held by the group. The individual setters
(`set_force_classic`, `set_wal_early_ack`, `set_async_engine_apply`)
are replaced by `set_config(&mut self, cfg: CrowKVConfig)`. The
individual getters become `self.config.force_classic` etc. The
`election_cfg` field is replaced by `self.config.election`.

The `test-util` `readindex_round_gate` stays on `PxGroup` (it's a
test-only runtime gate, not config).

### Test profile constructors

`PxElectionConfig::for_tests()` / `for_e2e()` remain as constructors
on `PxElectionConfig`. `CrowKVConfig` gains
`CrowKVConfig::for_tests()` / `for_e2e()` that build the full config
with the right election profile and test-appropriate defaults for the
other sub-structs. Tests that today call
`group.set_election_config(PxElectionConfig::for_tests())` call
`group.set_config(CrowKVConfig::for_tests())` instead.

### Backward compatibility

- No persisted config format changes. The `GroupConfigStore` persists
  group topology (members, endpoints, epochs), not tunables — unchanged.
- CLI args remain the same (no breaking changes to `cli.rs`); the
  `--config` arg is additive.
- Existing tests that call `set_election_config` / `set_inflight_config`
  / `set_force_classic` individually are updated to use
  `set_config(CrowKVConfig { … })` or the `for_tests()` constructor.

**Dependencies**: T1 (`wal_early_ack` default flip) and R35
(`async_engine_apply` carry) are gated on R40. R36 (proposal
coalescing) will benefit from R40's `coalesce_window_us` field but is
not blocked by it.

**Priority**: Medium-high — unblocks T1 and R35; eliminates the
per-flag carry-block pattern that scales poorly.

**Complexity**: Medium — the merge is mechanical (move fields, add
serde derives, update call sites). The JSON loading is straightforward
(`serde_json::from_reader`). The risk is in the test updates (~20 test
files call `set_election_config` / `set_inflight_config` /
`set_force_classic`); each is a mechanical replacement.

**Files**:
- `crowkv/src/common/config.rs` — `CrowKVConfig` struct, serde derives,
  `load_from_file`, `for_tests` / `for_e2e` / `default`.
- `crowkv/src/cluster/group.rs` — replace 3 bool flags + `election_cfg`
  with one `CrowKVConfig`; replace individual setters with `set_config`.
- `crowkv-server/src/store_registry.rs` — replace ~9 individual fields
  with one `CrowKVConfig`.
- `crowkv-server/src/startup.rs` — `create_group_with_wal` takes
  `&CrowKVConfig` instead of 14 params.
- `crowkv-server/src/main.rs` — `--config` arg, load + CLI override
  logic.
- `crowkv-server/src/mgmt_api.rs` — 4 `create_group_with_wal` call
  sites simplified; `rebuild_group_with_config` carry collapsed.
- `crowkv-server/src/cli.rs` — `--config` arg.
- `crowkv/tests/` — ~20 test files updated to use `set_config` /
  `CrowKVConfig::for_tests()`.

**Acceptance**:
- `CrowKVConfig` holds all fields from `WalConfig`, `PxElectionConfig`,
  `PaxosConfig`, `ServerConfig`, and the 3 former `PxGroup` flags.
- `--config <path>` loads a JSON file; CLI args override individual
  fields. Omitting `--config` uses `CrowKVConfig::default()` (same
  defaults as today).
- `create_group_with_wal` takes `&CrowKVConfig` + 4 identity params
  (down from 14).
- `rebuild_group_with_config` carries one config object (no per-flag
  blocks).
- All existing tests pass (mechanical `set_config` replacement).
- `wal_early_ack` defaults to `false` in `CrowKVConfig::default()`
  (T1 flips it to `true` after crash tests pass).
- A partial JSON file (only some keys) works — missing keys use
  sub-struct defaults via `#[serde(default)]`.
