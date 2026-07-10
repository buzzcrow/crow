# crowkv

A high-concurrency distributed key-value engine built on Multi-Paxos with static multi-group sharding, written in Rust.

## Workspace layout

This is a Cargo workspace. One library crate (`crowkv`) holds all core logic as modules, plus two binary crates (`crowkv-server`, `crowkv-bench`).

```
crowkv/            Core library: consensus, engine, wal, io, rpc, reconfig
                   (modules populated phase by phase)
crowkv-server/     Server binary: single-group, multi-group, cluster (P4)
crowkv-bench/      Benchmark / load test binary                     (P4)
```

Test utilities (`TestTimer`, `TestRouter`, `TestNode`, `SimDisk`) live in `crowkv/src/testkit.rs` (gated `#[cfg(test)]`).
Integration tests live in `crowkv/tests/`.

Dependency rule: `crowkv-server` and `crowkv-bench` depend on `crowkv`. Full design and phasing in [`doc/`](doc/).

## Documentation

See [`doc/requirement.md`](doc/requirement.md) for product requirements, [`doc/design.md`](doc/design.md) for the system design, and [`doc/plan.md`](doc/plan.md) for the implementation phasing.
