<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R26: Follower read distribution for MinSlot

**Problem**: `CrowkvClient::get` / `scan` always call
`resolve_leader(store_id, group_id)` to pick the endpoint, regardless
of `read_mode`. For MinSlot reads this is wasteful — the server serves
MinSlot reads from the local replica without forwarding, so any
follower could serve them. All stale-read load is concentrated on the
leader, leaving follower capacity unused and making the leader a
read bottleneck under read-heavy workloads.

Full gap analysis: G8 in
[`read-flow-analysis.md`](../working/read-flow-analysis.md).

**Approach**: Client-side change only — the server already serves
MinSlot reads locally without forwarding, so no server-side work is
needed.

- Topology cache exposes all replica endpoints for a group, not just
  the leader (it already stores the full replica list; the selector
  just needs to use it).
- New read-endpoint selector for MinSlot reads. Initial policy:
  round-robin across replica endpoints. Future policies
  (least-connection, latency-based) can plug into the same seam.
- New config flag `read_endpoint_policy = leader | any_replica`
  (default `leader` for backward compat and to keep linearizable reads
  correct — linearizable reads must still target the leader).
- Linearizable reads are unaffected — they always resolve to the
  leader.
- On `NotLeader` redirect (a follower that hasn't caught up to
  `min_slot`), the client falls back to the leader for that request,
  mirroring the existing retry path.

**Priority**: Medium — read scaling matters under read-heavy
workloads, but only after R19 lands so the distribution effect is
measurable (forward counter must drop, per-mode latency on the leader
must drop).

**Complexity**: Medium — endpoint selector, config flag, topology
cache already has the data. No protocol change. The subtle part is
the fallback path when a chosen follower hasn't applied `min_slot`
yet.

**Dependencies**: R19 (Read performance profiling and metrics) —
need `kv.get_forwarded.c`, `read.minslot_fallback.c`, and per-mode
latency to validate that distribution actually reduces leader load
without increasing fallback storms.

**Files**: `crowkv-client/src/client.rs` (`get` / `scan` — branch on
`read_endpoint_policy` for MinSlot, new endpoint selector),
`crowkv-client/src/topology.rs` (expose replica endpoints to the
selector), `crowkv-client/src/config.rs` (`read_endpoint_policy`).

**Acceptance**:
- With `read_endpoint_policy = any_replica`, MinSlot reads are
  distributed across replicas (round-robin); linearizable reads still
  go to the leader.
- `kv.get_forwarded.c` drops toward zero for MinSlot reads (they are
  no longer routed to the leader and then forwarded back).
- `read.minslot_fallback.c` stays low — followers keep up with the
  client's write watermark; fallback to leader is rare.
- Per-mode latency on the leader (`kv.get.linearizable.lh`) drops
  under read-heavy mixed workloads because the leader no longer
  serves MinSlot reads.
- No regression in linearizable read correctness (still leader-only).
