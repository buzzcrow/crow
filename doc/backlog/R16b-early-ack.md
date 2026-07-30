<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R16b: Early ack before local WAL persist (gated)

**Problem**: Even after R16a (concurrent fan-out), the proposer still
waits for the local `on_accept` — and thus the local `fdatasync` — to
resolve before it can declare `Chosen`. On a slow disk or a busy
writer, the local fsync can be the long pole even when remote RPCs
have already reached quorum.

**Approach**: Return `Chosen` as soon as quorum is confirmed by
*remote* accepts, tracking the local WAL persist as a separate
outstanding future. The local acceptor logic (`inner_accept`) still
runs synchronously (CAS on the slot node); only the WAL flush is
deferred. This requires splitting `on_accept` into:

1. **Accept logic** — run `inner_accept`, get the `PxAcceptReply`
   (synchronous, instant).
2. **WAL persist** — `wal.append(&record).await` (deferred, tracked
   separately).

The proposer would:
1. Call the accept logic on the local replica (instant).
2. Concurrently (R16a): (a) await the local WAL persist, (b) fan out
   `send_accept` RPCs to all remote replicas.
3. Declare `Chosen` as soon as remote quorum is met — *without* waiting
   for the local WAL persist to finish.
4. Track the local persist future to completion in the background; if
   it fails, surface a durability-error (the value is already chosen,
   so this is a data-integrity alert, not a consensus rollback).

**Concept change (highlighted)**: This weakens the W6 ack contract for
the *local* replica. Today, `on_accept` guarantees the Accepted record
is durably persisted before returning `Accepted`. With this change, the
local replica's `Accepted` reply is returned before the WAL flush
completes. If the node crashes between the accept reply and the WAL
flush, the accepted value may be lost — safe in Paxos (the value was
not yet chosen at the time of the local reply, and re-election will
re-propose), but it changes the durability ordering and means the
leader may re-propose a slot it had already accepted.

**Feature flag**: Gate behind `wal_early_ack` (default off). When
enabled, the proposer declares chosen on remote quorum without waiting
for local persist; when disabled, the R16a behavior (wait for local
persist before declaring chosen) is preserved.

**Testing**:
- Crash-recovery tests: kill the leader after local accept reply but
  before WAL flush; verify the slot is re-proposed and converges to a
  single chosen value.
- Quorum tests: verify the proposer still requires remote quorum
  (plus the local acceptor CAS success) before declaring chosen.
- Durability-error path: inject WAL flush failure; verify the
  background persist future surfaces the error without rolling back
  the chosen value.
- Benchmark: measure additional per-proposal latency hidden when local
  fsync is the long pole (slow disk / high writer contention).

**Priority**: Low-medium — only matters when local fsync is slower
than the remote RPC RTT (slow disk, or fast network with many
replicas). On NVMe + loopback the gain over R16a is small. Do after
R16a and only if profiling shows local fsync still on the critical
path.

**Complexity**: High — splits `on_accept`, changes the proposer's
local-replica handling to track persist separately from quorum,
weakens W6 contract, requires feature flag and crash-recovery tests.

**Files**:
- `crowkv/src/cluster/local_replica.rs` — `on_accept` split (accept
  logic vs WAL persist)
- `crowkv/src/cluster/group.rs` — `run_accept_phase` (declare chosen
  on remote quorum; track local persist separately)
- `crowkv/src/wal/wal_engine.rs` — no change to `append` itself
- `crowkv/src/common/config.rs` — `wal_early_ack` flag

**Acceptance**:
- With feature flag off: all existing tests pass, behavior matches
  R16a.
- With feature flag on: Paxos election and group propose tests pass.
- Crash-recovery test: leader crash after accept reply, before WAL
  flush → slot re-proposed and converges to a single chosen value.
- Benchmark shows reduced per-proposal latency when local fsync is
  the long pole (slow disk / high writer contention).

**Split from**: R16 (original). The safe concurrent-fan-out part is
now R16a; this item is the contract-weakening early-ack part.
**Depends on**: R16a (concurrent fan-out is a prerequisite — early
ack builds on the same concurrent local + remote join).
