<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW

[![CI](https://github.com/buzzcrow/crow/actions/workflows/ci.yml/badge.svg)](https://github.com/buzzcrow/crow/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

CROW is a storage platform — a foundation layer for building storage systems where you own the hot path all the way down to the metal. The first component is **crow-kv**, a distributed key-value cluster that takes Multi-Paxos seriously: not as a textbook exercise, but as a deliberate engineering choice to eliminate Raft's sequential commit bottleneck on the write hot path.

## Why This Project

A high-performance distributed KV store is the natural foundation layer: put one underneath a larger storage system and its overall architecture simplifies considerably. But that only works with full control over the KV itself — the freedom to make targeted optimizations as workloads demand. Off-the-shelf components don't offer that, so CROW builds one from scratch as its first component.

And since future performance gains are increasingly tied to hardware (io_uring, NVMe, CPU cache behavior), the platform is Rust + C++, keeping the hot paths close to the metal.

> **Project started July 10, 2026.** Built by a single developer working with AI.

### Demos

**Cluster Lifecycle** — bootstrap a 3-node cluster from scratch: add a rack, register nodes in the physical view, then switch to the logical view to create a store and a Paxos group with replicas on the selected target nodes. The topology canvas updates in real time as replicas come online and elect a leader.

<video src="doc/assets/demo-cluster.mp4" autoplay muted loop></video>

**KV Operations** — the KV Operator panel auto-loads demo keys, then demonstrates put, get, and delete against a specific group. The scan list updates live after each mutation, and demo keys can be bulk-deleted in one click.

<video src="doc/assets/demo-kv.mp4" autoplay muted loop></video>

**Failover & Replica Management** — add a 4th node and expand the group from 3 to 4 replicas, then remove the leader replica. The remaining nodes re-elect a new leader and KV operations continue uninterrupted. The removed replica is added back afterward. Note: even-numbered replica counts fall back to an odd quorum and function correctly, but odd counts are recommended for production deployments.

<video src="doc/assets/demo-failover.mp4" autoplay muted loop></video>

## Why Multi-Paxos?

Raft's log is contiguous by construction: a leader cannot acknowledge slot N+1 until slot N is committed. Under high concurrency this becomes a sequential bottleneck.

Multi-Paxos treats each slot as an independent Paxos instance. Slots can be **decided and applied out of order**, turning a sequential wait into a fully pipelined commit path. crow-kv pays for this with extra complexity around gap repair and a slightly more conservative read frontier — a tradeoff documented in detail in the [design doc](doc/design/kv/design-crow-kv.md#1-overview).

## Architecture at a Glance

```
                         crow-kv Cluster
  ┌──────────────────────────────────────────────────────────┐
  │   Node A                Node B                Node C     │
  │   ┌─────────┐          ┌─────────┐          ┌─────────┐  │
  │   │Group-1 L│ ◄──────► │Group-1 F│ ◄──────► │Group-1 F│  │
  │   │Group-2 F│          │Group-2 L│          │Group-2 F│  │
  │   └─────────┘          └─────────┘          └─────────┘  │
  └───────▲──────────────────────────────────────────────────┘
          │  HTTP /topology + per-group gRPC reads/writes
     ┌────┴────┐
     │ Client  │
     └─────────┘
```

- **Multi-group sharding** — each node hosts multiple Paxos groups; routing is by explicit `group_id`. Group membership and key ranges are operator-defined.
- **Pluggable storage** — the WAL is the source of truth; the key-value engine is a derived projection. An in-memory engine and `crow-tree` (a custom B+tree with delta-chain encoding, io_uring async I/O, and epoch-safe lock-free reads) are both implemented behind a unified `KVEngine` trait.
- **Raft where it doesn't matter, Paxos where it does** — leader election, leases, snapshot install, and reconfiguration follow settled Raft designs. Only the write hot path diverges into Multi-Paxos.
- **Console** — a web UI and CLI for cluster lifecycle management (bootstrap, rolling upgrade, replica add/remove, health monitoring).

## Crates

| Crate | What it is |
| --- | --- |
| `crow-kv` | Core library: Multi-Paxos consensus, WAL, storage engine trait, RPC, reconfiguration |
| `crow-kv-server` | Server binary: hosts groups, serves gRPC + HTTP management API |
| `crow-kv-client` | Client library: topology cache, retry, idempotency |
| `crow-tree` | Custom storage engine (C++ core): B+tree, delta chains, io_uring reactor, buffer pool |
| `crow-tree-ffi` | Rust FFI bindings to `crow-tree` — exposes the C++ engine as a `KVEngine` trait impl |
| `crow-common` | Shared C++/Rust utilities: async logging (spdlog), compressing file sink |
| `crow-console-shared` | Console core library: cluster config, lifecycle (deploy/stop), SSH, topology model |
| `crow-web` | Web console (Axum + React): cluster lifecycle UI, KV operator panel, Swagger |
| `crow-cli` | CLI console (`clap`): same management surface as the web console |

<details>
<summary><b>Getting Started</b></summary>

CROW uses [Pixi](https://pixi.sh) for environment management — it pins the C++ toolchain, Rust compiler, and all native dependencies (cmake, gtest, lz4, folly, protobuf, etc.) in a single lockfile. The Linux build targets glibc 2.17 (CentOS 7 / Ubuntu 16.04 era), so binaries built once run on virtually any modern Linux distribution.

```bash
# Install pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | sh

# Build everything (crow-tree C++ + Rust workspace + web UI)
pixi run build

# Run all tests (C++ ctest + Rust unit/integration + web + Playwright e2e)
pixi run test-suite
```

</details> 

## Performance

crow-kv's hot path is built around a few core design choices:

- **Pipelined inflight window** — Multi-Paxos decides slots out of order, so the leader admits many proposals in parallel instead of serializing one at a time like Raft.
- **Server-side proposal coalescing** — concurrent single-key client ops are batched into one slot and one quorum round, amortizing the consensus RPC cost across the whole batch.
- **Queue-based admission** — when the window is full, proposals block on a semaphore instead of being rejected as `Busy`; no client-side retry storms.
- **Off-critical-path durability & apply** — the leader's local fsync (early-ack) and engine apply (async, fenced) both run off the per-proposal critical path, leaving only the quorum RPC round-trip.
- **Zero-copy hot path** — `Bytes` ref-counting, vectored `writev` for WAL flush, FFI batch-apply slices, and `PinnedValue`-backed reads that hand a `Bytes` straight from the C++ frame with no copy.
- **Lease fast-path reads** — linearizable reads cost ~0 barrier when the leader's lease is valid; MinSlot reads spread across all replicas.

**Write Performance** — 3-node cluster, in-memory WAL + storage, write-only, 512-byte values, 1M key space, 10-second duration, `coalesce_max_keys=32`, `max_inflight=32`, admission policy = `Queue`. AMD Ryzen 9 5950X (16 cores / 32 threads, Linux). Zero errors across all configurations.

| Threads | Connections | Throughput (ops/s) | Avg Latency | p99 Latency | Busy Rejections |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 3,029 | 327 µs | 428 µs | 0 |
| 4 | 2 | 12,681 | 313 µs | 496 µs | 0 |
| 16 | 4 | 32,935 | 483 µs | 804 µs | 0 |
| 32 | 16 | 52,688 | 604 µs | 1,180 µs | 0 |
| 64 | 32 | 75,280 | 846 µs | 1,850 µs | 0 |
| 128 | 32 | 105,779 | 1,204 µs | 2,592 µs | 0 |
| 256 | 32 | 123,745 | 2,058 µs | 4,392 µs | 0 |

Peak **123K ops/s** at 256 threads — 4.3× the non-coalesced ceiling (~29K) from batching single-key ops into one slot/round. The per-proposal critical path is the quorum RPC round-trip only; the leader's fsync and engine apply both run off it. **Zero `Busy` rejections** across all configs — queue admission backpressures instead of rejecting.

**Read Performance** — same setup, read-only, 200K key space pre-populated. All three replicas run on one machine, so MinSlot's 3-replica parallelism is bounded by the same CPU as Linearizable — they converge at peak. In a multi-node deployment, MinSlot would scale across separate machines. Zero errors across all configurations.

| Threads | Connections | Linearizable ops/s | Linearizable p99 | MinSlot ops/s | MinSlot p99 |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 5,876 | 233 µs | 5,876 | 233 µs |
| 6 | 6 | 47,366 | 200 µs | 45,563 | 183 µs |
| 24 | 24 | 120,494 | 403 µs | 112,172 | 444 µs |
| 48 | 48 | 144,486 | 828 µs | 135,928 | 884 µs |

Peak **145K ops/s** — ~1.17× the coalesced write peak (124K). Reads skip the consensus critical path entirely (no WAL, no quorum RPC); the lease barrier costs ~0 when valid, so a linearizable read is just engine get + gRPC RTT.

## Documentation

The full design lives in [`doc/`](doc/). Start with:

- [**Design**](doc/design/kv/design-crow-kv.md) — what crow-kv is, why key choices were made, and how the system is structured
- [**User Guide**](doc/user-manual/user-guide.md) — quick start, KV operations, cluster management, and API reference
- [**Doc Index**](doc/doc_index.md) — a navigable map to every design doc and sub-topic

## Notes on AI-Assisted Development

The code in this project was written with AI assistance. But the shape of the software is personal. Architecture, naming, module boundaries, the tradeoffs that matter. Those are human choices. AI is the compiler. The intent is mine.

## License

See [LICENSE](LICENSE).
