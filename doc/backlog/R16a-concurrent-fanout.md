<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R16a: Concurrent local + remote fan-out (overlap local fsync with RPC RTT)

**Problem**: In `run_accept_phase` and `run_prepare_phase`, the leader
`await`s the local `on_accept` / `on_prepare` (which runs acceptor CAS
**+ WAL append + `fdatasync`**) *before* issuing any remote RPC. The
leader's local fsync (~10-100 µs on NVMe, more under load) sits on the
critical path ahead of the network RTT. With R14 (concurrent fan-out),
the remote RPCs overlap with each other but still wait for the local
fsync to finish first.

**Approach**: Issue the local handler and the remote RPCs concurrently
— fold the local `on_accept` / `on_prepare` call into the same
`join_all` as the remote RPCs (or spawn-then-join equivalently). The
quorum check still awaits the local reply before counting it.

**Why this is safe (no contract change)**: W6 only forbids the local
replica replying `Accepted` to the proposer before the Accepted record
is durably persisted. `on_accept` still does not return `Accepted`
until after `wal.append` resolves — that internal ordering is
unchanged. The only thing that changes is *when the proposer issues
the remote RPCs* relative to the local persist: they go out
concurrently instead of after. The proposer still waits for the local
`Accepted` reply (and for quorum) before declaring chosen. No crash-
recovery semantics change: if the leader crashes mid-flight, the
outcome is identical to today (the local accept either persisted or
didn't; remote accepts either arrived or didn't).

This is the part of the original R16 that is a pure win. The
contract-weakening part (returning `Chosen` before local persist
completes) is split out into R16b.

**No feature flag needed**: the behavior is observably identical to
the serial path except for issue ordering, which is not a correctness
observable.

**Testing**:
- All existing Paxos election and group propose tests pass unchanged.
- Quorum tests: verify the proposer still waits for all replicas
  (including local) before declaring chosen.
- Benchmark: measure fsync latency hidden behind RPC round-trips;
  expect reduced per-proposal latency at high inflight (the ~1.7 ms
  ceiling at 48 inflight in the 2026-07-24 sweep).

**Priority**: Medium-high — biggest structural win on the consensus
critical path that carries no correctness risk. Should be done before
R16b and before R17.

**Complexity**: Medium — changes `run_accept_phase` and
`run_prepare_phase` from "local await, then `join_all` remote" to a
single `join_all` over local + remote. No `on_accept` / `on_prepare`
split needed (unlike R16b). No WAL changes.

**Files**:
- `crowkv/src/cluster/group.rs` — `run_accept_phase`, `run_prepare_phase`
  (concurrent local + remote join)
- `crowkv/src/cluster/local_replica.rs` — no change

**Acceptance**:
- All existing consensus tests pass with no behavioral change.
- Benchmark at 48T:48C:MI=64 shows reduced per-proposal latency
  (avg µs drops; throughput ceiling rises above 29K if fsync was a
  contributor).
- No new feature flag; no new config knob.

**Split from**: R16 (original). The contract-weakening early-ack
variant is now R16b.
