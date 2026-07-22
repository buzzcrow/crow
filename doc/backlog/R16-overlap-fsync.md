<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R16: Overlap local WAL fsync with remote RPC fan-out

**Problem**: In `run_accept_phase`, the leader calls
`replica.on_accept(entry.clone()).await` first, which internally awaits
`wal.append(&record).await` (an `fdatasync` round-trip) before
returning `PxAcceptReply::Accepted`. Only after this local fsync
completes does the leader begin sending `send_accept` RPCs to remote
replicas. The leader's local disk fsync is therefore fully serial with
the remote RPC fan-out, adding ~10-100 µs (NVMe) to ~1-10 ms (SSD/HDD)
of disk latency to the critical path before any network I/O starts.

With R14 (concurrent fan-out), the remote RPCs overlap with each other
but still wait for the local fsync to finish first.

**Approach**: Start the local WAL append and the remote accept RPCs
concurrently. The local acceptor logic (`inner_accept`) is synchronous
and completes instantly — only the WAL persist (`fdatasync`) is slow.
Split `on_accept` into two phases:
1. **Accept logic** — run `inner_accept`, get the `PxAcceptReply`.
2. **WAL persist** — `wal.append(&record).await`.

The proposer would:
1. Call the accept logic on the local replica (instant).
2. Concurrently: (a) await the local WAL persist, (b) fan out
   `send_accept` RPCs to all remote replicas (R14 makes these
   concurrent).
3. After all futures resolve, fold results and check quorum.

**Concept change (highlighted)**: This weakens the W6 ack contract for
the *local* replica. Today, `on_accept` guarantees the Accepted record
is durably persisted before returning `Accepted`. With this change, the
local replica's `Accepted` reply is returned before the WAL flush
completes — the proposer tracks local persist separately. If the node
crashes between the accept reply and the WAL flush, the accepted value
may be lost, which is safe in Paxos (the value was not yet chosen) but
means the leader may re-propose a slot it had already accepted. This is
correctness-safe but changes the durability ordering.

**Feature flag**: Gate behind `wal_overlap_local_persist` (default
off). When enabled, the proposer overlaps; when disabled, the current
serial behavior is preserved.

**Testing**:
- Crash-recovery tests: kill the leader after local accept but before
  WAL flush; verify the slot is re-proposed and converges.
- Quorum tests: verify the proposer still waits for all replicas before
  declaring chosen.
- Benchmark: measure fsync latency hidden behind RPC round-trips.

**Priority**: Medium — hides fsync latency behind network latency,
which is the single largest non-network bottleneck in the write path.

**Complexity**: High — splits `on_accept`, changes the proposer's
local-replica handling from a simple `.await` to a concurrent join,
weakens W6 contract, requires feature flag and crash-recovery tests.

**Files**: `crowkv/src/cluster/local_replica.rs` (`on_accept` split),
`crowkv/src/cluster/group.rs` (`run_accept_phase` concurrent local +
remote), `crowkv/src/wal/wal_engine.rs` (no change to `append` itself).

**Acceptance**:
- With feature flag off: all existing tests pass, behavior unchanged.
- With feature flag on: Paxos election and group propose tests pass.
- Crash-recovery test: leader crash after accept, before WAL flush →
  slot re-proposed and converges to a single chosen value.
- Benchmark shows reduced per-proposal latency when fsync is the
  bottleneck (non-loopback or slow disk).
