<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

[![CI](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml/badge.svg)](https://github.com/buzzcrow/crowdb/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

CROWDB is a high-performance distributed storage platform — a foundation layer for building storage systems where you own the hot path all the way down to the metal.

The foundation is **crowdb-kv**, a distributed key-value cluster built on multi-group Multi-Paxos. Where Raft serializes every commit through a single log, Multi-Paxos decides slots in parallel, turning the write hot path into a fully parallel commit. That is the consensus core. On top of it comes a chunk-based common storage layer, and on top of that, distributed data structures like KV and streams that scale out far beyond any single node's local limitation, with no node owning the data it serves.

## Why This Project

Storage systems are usually assembled from off-the-shelf parts: a consensus library here, a KV engine there, a log component somewhere else. Each of those arrives with an architecture already decided, and once you build on top of one you live inside that decision. When it turns into the bottleneck, all you can do is work around it. CROWDB owns the whole flow instead — consensus, WAL, storage engine, I/O path — so every layer is ours to move.

That control matters because the ground under storage keeps shifting. io_uring and NVMe moved the bottleneck once already, and GPUDirect Storage, DMA paths that skip the CPU, high-bandwidth fabrics, and offload to accelerator cards will move it again. Adopting any of them is never a patch to a single module; it reaches through consensus, durability, and the data path at the same time, which only works if all three are yours.

Workloads keep shifting too. Storage is no longer a short list of fixed protocols: AI training and inference ask for things that file and object were never shaped around, and what they ask for is still changing. A foundation layer either answers quickly or every system above it inherits the delay.

None of this pays off without a base worth building on, so that is what comes first — a design simple enough to reason about, efficient enough to be worth the trouble, and stable enough to carry what comes later. Rust and C++, close to the metal. The hardware work follows. This is an early start, and most of the modern-hardware story is still ahead of us.

Not because the off-the-shelf parts are bad, but because a foundation that can't be redesigned isn't a foundation.

### Demos

**Cluster Lifecycle** — bootstrap a 3-node cluster from scratch: add a rack, register nodes in the physical view, then switch to the logical view to create a store and a Paxos group with replicas on the selected target nodes. The topology canvas updates in real time as replicas come online and elect a leader.

<video src="https://github.com/user-attachments/assets/974d4a44-2446-462e-a9d0-9d9a82d07146" autoplay muted loop></video>

**KV Operations** — the KV Operator panel auto-loads demo keys, then demonstrates put, get, and delete against a specific group. The scan list updates live after each mutation, and demo keys can be bulk-deleted in one click.

<video src="https://github.com/user-attachments/assets/1fbdcf4e-255a-47e6-b4db-6a1fa0fb0df8" autoplay muted loop></video>

**Failover & Replica Management** — add a 4th node and expand the group from 3 to 4 replicas, then remove the leader replica. The remaining nodes re-elect a new leader and KV operations continue uninterrupted. The removed replica is added back afterward. Note: even-numbered replica counts fall back to an odd quorum and function correctly, but odd counts are recommended for production deployments.

<video src="https://github.com/user-attachments/assets/63298646-6eaa-4253-b96a-6f5eb420ad91" autoplay muted loop></video>

## Why Multi-Paxos?

Raft's log is contiguous by construction: a leader cannot acknowledge slot N+1 until slot N is committed. Under high concurrency this becomes a sequential bottleneck.

Multi-Paxos treats each slot as an independent Paxos instance. Slots can be **decided and applied out of order**, turning a sequential wait into a fully pipelined commit path. crowdb-kv pays for this with extra complexity around gap repair and a slightly more conservative read frontier — a tradeoff documented in detail in the [design doc](doc/design/kv/design-crowdb-kv.md#1-overview).

## Architecture at a Glance

```
                         crowdb-kv Cluster
  ┌──────────────────────────────────────────────────────────┐
  │   Node A                Node B                Node C     │
  │   ┌─────────┐          ┌─────────┐          ┌─────────┐  │
  │   │Group-1 L│ ◄──────► │Group-1 F│ ◄──────► │Group-1 F│  │
  │   │Group-2 F│          │Group-2 L│          │Group-2 F│  │
  │   └─────────┘          └─────────┘          └─────────┘  │
  └───────▲──────────────────────────────────────────────────┘
          │  HTTP /topology + per-group crowdb-rpc reads/writes
     ┌────┴────┐
     │ Client  │
     └─────────┘
```

- **Multi-group sharding** — each node hosts multiple Paxos groups; routing is by explicit `group_id`. Group membership and key ranges are operator-defined.
- **Pluggable storage** — the WAL is the source of truth; the key-value engine is a derived projection. An in-memory engine and `crowdb-tree` (a custom B+tree with delta-chain encoding, io_uring async I/O, and epoch-safe lock-free reads) are both implemented behind a unified `KVEngine` trait.
- **Raft where it doesn't matter, Paxos where it does** — leader election, leases, snapshot install, and reconfiguration follow settled Raft designs. Only the write hot path diverges into Multi-Paxos.
- **Console** — a web UI and CLI for cluster lifecycle management (bootstrap, rolling upgrade, replica add/remove, health monitoring).

## Crates

| Crate | What it is |
| --- | --- |
| `crowdb-kv` | Core library: Multi-Paxos consensus, WAL, storage engine trait, RPC, reconfiguration |
| `crowdb-kv-server` | Server binary: hosts groups, serves crowdb-rpc + HTTP management API |
| `crowdb-kv-client` | Client library: topology cache, retry, idempotency |
| `crowdb-tree` | Custom storage engine (C++ core): B+tree, delta chains, io_uring reactor, buffer pool |
| `crowdb-tree-ffi` | Rust FFI bindings to `crowdb-tree` — exposes the C++ engine as a `KVEngine` trait impl |
| `crowdb-common` | Shared C++/Rust utilities: async logging (spdlog), compressing file sink |
| `crowdb-console-shared` | Console core library: cluster config, lifecycle (deploy/stop), SSH, topology model |
| `crowdb-web` | Web console (Axum + React): cluster lifecycle UI, KV operator panel, Swagger |
| `crowdb-cli` | CLI console (`clap`): same management surface as the web console |

<details>
<summary><b>Getting Started</b></summary>

CROWDB uses [Pixi](https://pixi.sh) for environment management — it pins the C++ toolchain, Rust compiler, and all native dependencies (cmake, gtest, lz4, folly, flatbuffers, etc.) in a single lockfile. The Linux build targets glibc 2.17 (CentOS 7 / Ubuntu 16.04 era), so binaries built once run on virtually any modern Linux distribution.

```bash
# Install pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | sh

# Build everything (crowdb-tree C++ + Rust workspace + web UI)
pixi run build

# Run all tests (C++ ctest + Rust unit/integration + web + Playwright e2e)
pixi run test-suite
```

</details> 

## Performance

crowdb-kv's hot path is built around a few core design choices:

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

Peak **145K ops/s** — ~1.17× the coalesced write peak (124K). Reads skip the consensus critical path entirely (no WAL, no quorum RPC); the lease barrier costs ~0 when valid, so a linearizable read is just engine get + crowdb-rpc RTT.

## Documentation

The full design lives in [`doc/`](doc/). Start with:

- [**Design**](doc/design/kv/design-crowdb-kv.md) — what crowdb-kv is, why key choices were made, and how the system is structured
- [**User Guide**](doc/user-manual/user-guide.md) — quick start, KV operations, cluster management, and API reference
- [**Doc Index**](doc/doc_index.md) — a navigable map to every design doc and sub-topic

## Notes on AI-Assisted Development

The code in this project was written with AI assistance. But the shape of the software is personal. Architecture, naming, module boundaries, the tradeoffs that matter. Those are human choices. AI is the compiler. The intent is mine.

## License

See [LICENSE](LICENSE).
