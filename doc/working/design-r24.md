<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R24 — Simplify Read Modes to Linearizable + MinSlot

## Problem

The current read API exposes four modes (`Linearizable`, `ReadYourWrites`,
`BoundedStale`, `BestEffort`) but the server-side routing for the three
non-linearizable modes is nearly identical:

- `ReadYourWrites` — check `contiguous_applied >= client_slot`, serve or
  redirect.
- `BoundedStale` — always serve locally, report `safe_slot`.
- `BestEffort` — always serve locally.

The only real knob is *how fresh* the read must be. `ReadYourWrites` expresses
"at least as fresh as my last write"; `BoundedStale` expresses "at least as
fresh as the last known safe-slot"; `BestEffort` expresses "any staleness".
All three are subsumed by a single parameter: `min_slot`.

## Proposed Approach

Collapse the enum to two variants:

- **`Linearizable` (0)** — unchanged. Leader-served with lease/ReadIndex
  fencing. Non-leader replicas forward to the leader.
- **`MinSlot` (1)** — client carries `min_slot`. Replica checks
  `contiguous_applied >= min_slot`; if true, serves locally; if false,
  redirects to leader via `NotLeader` response.

The client chooses the freshness policy by setting `min_slot`:

- `0` — accept any staleness (was `BestEffort`).
- write watermark — read-your-writes (was `ReadYourWrites`).
- last known `safe_slot` — bounded stale (was `BoundedStale`).

### Proto changes

- `ReadMode` enum: `LINEARIZABLE = 0`, `MIN_SLOT = 1`. Remove
  `READ_YOUR_WRITES`, `BOUNDED_STALE`, `BEST_EFFORT`.
- `KvGetRequest.client_slot` (field 7) renamed to `min_slot`.
- `KvScanRequest`: add `min_slot` field (field 9) for symmetry.

### Server changes

`resolve_read_point` in `px_kv_store.rs` — replace the 4-way match with
2-way:

- `Linearizable` — unchanged (lease/ReadIndex barrier on leader, redirect
  on non-leader).
- `MinSlot` — if `contiguous_applied >= min_slot`, serve locally at
  `contiguous_applied`; otherwise `NotLeader` redirect.

`kv_service.rs` — forwarding logic unchanged: only `Linearizable` triggers
server-side forwarding. `MinSlot` is never forwarded (it is served locally
or the response carries a `NotLeader` hint that the client follows).

### Client changes

`resolve_client_slot` → `resolve_min_slot`: for `MinSlot` mode, auto-attach
the write watermark if the caller did not supply a `min_slot` (preserves
read-your-writes behavior). For `Linearizable`, `min_slot` is ignored.

`get` signature: `client_slot: Option<u64>` → `min_slot: Option<u64>`.
`scan` signature: add `min_slot: Option<u64>` parameter.

### Alternatives considered

- **Keep 4 modes, add `MinSlot` as a 5th** — rejected: adds complexity
  without benefit. The 3 non-linearizable modes are subsumed.
- **Remove the enum entirely, use only `min_slot`** — rejected:
  `Linearizable` requires a fundamentally different code path (leader
  barrier) that cannot be expressed as a slot threshold.

## Acceptance Test Plan

- `node_test.rs::read_modes_serve_value_with_slots_on_single_leader` —
  updated to test `Linearizable` and `MinSlot` (with `min_slot = 0` and
  `min_slot = write_watermark`).
- `e2e_single_node_test.rs` — `BestEffort` → `MinSlot` with `min_slot = 0`;
  `ReadYourWrites` → `MinSlot` with auto watermark.
- `snapshot_join_e2e_test.rs` — `read_mode: 3` → `read_mode: 1` (MinSlot).
- `full_restart_delete_test.rs` — `read_mode: 3` → `read_mode: 1` (MinSlot).
- All existing `read_mode: 0` (Linearizable) tests unchanged.
- `cargo fmt --check`, `cargo clippy -- -D warnings` pass.
- `pixi run test-core`, `pixi run test-server` pass.
