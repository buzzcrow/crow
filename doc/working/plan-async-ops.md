<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R12 Plan — Async Operation API + Cluster Readiness

## Task 1: Operation Registry

- [ ] Define `OperationKind` enum: `StepDown`, `RemoveReplica`,
  `AddReplica`, `StopServer`
- [ ] Define `OperationStatus` enum: `Pending`, `Running`, `Completed`,
  `Failed`
- [ ] Define `Operation` struct: `id`, `kind`, `status`, `target`,
  `started_at`, `completed_at`, `error`
- [ ] Implement `OperationRegistry` with `DashMap<u64, Operation>`,
  `create()`, `get()`, `update_status()`, auto-increment ID
- [ ] Add TTL cleanup task (remove completed ops after 5 min)
- [ ] Unit tests: create → get → update → cleanup

Files: `crowkv-server/src/operation_registry.rs` (new),
`crowkv-server/src/mgmt_api.rs` (wire into router state)

## Task 2: Cluster Readiness API

- [ ] Implement `group_readiness()` on `PxGroup`: check leader_id != 0,
  all voting replicas reachable, lag <= threshold
- [ ] Add `GET /stores/:sid/groups/:gid/ready` endpoint
- [ ] Define `ReadinessResponse` JSON type: `ready`, `leader_id`, `term`,
  `voting_replicas`, `reachable_replicas`, `max_applied_slot`,
  `min_applied_slot`, `lag`, `reason` (when not ready)
- [ ] Return `200` when ready, `503` when not ready
- [ ] Add OpenAPI annotation
- [ ] Tests: ready group returns 200, no-leader group returns 503,
  lagging replica returns 503 with lag info

Files: `crowkv/src/cluster/status.rs` (readiness logic),
`crowkv-server/src/mgmt_api.rs` (endpoint)

## Task 3: Async Step-Down

- [ ] Modify `POST /stores/:sid/groups/:gid/step-down`:
  - Default: create operation, spawn task, return `202 {operation_id}`
  - `?sync=true`: preserve existing synchronous behavior
- [ ] Async task: call `step_down_if_leader`, then poll group status until
  new leader appears (timeout 10 s), update operation status
- [ ] Add `GET /operations/:id` endpoint
- [ ] Define `OperationResponse` JSON type
- [ ] Tests: async step-down returns 202, operation completes with new
  leader, sync mode preserves old behavior

Files: `crowkv-server/src/mgmt_api.rs`

## Task 4: Async Remove Replica

- [ ] Modify `DELETE /stores/:sid/groups/:gid/remotes/:rid`:
  - If removing the leader: async (create operation, return 202)
  - If removing non-leader: synchronous (existing behavior)
  - `?sync=true`: always synchronous
- [ ] Async task: remove replica, poll group status until new leader
  appears (timeout 10 s), update operation status
- [ ] Tests: remove leader returns 202 + new leader eventually elected,
  remove non-leader returns 200 synchronously

Files: `crowkv-server/src/mgmt_api.rs`

## Task 5: Async Add Replica (Catch-Up Wait)

- [ ] Modify `POST /stores/:sid/groups/:gid/remotes`:
  - Default: add replicas, create operation, return `202 {operation_id}`
  - `?sync=true`: preserve existing synchronous behavior
- [ ] Async task: poll `/ready` until new replica catches up (lag <=
  threshold, timeout 30 s), update operation status
- [ ] Tests: add replica returns 202, operation completes when replica
  catches up, data visible on new replica

Files: `crowkv-server/src/mgmt_api.rs`

## Task 6: GUI Integration

- [ ] Add operation polling helper in console shared core
- [ ] Modify UI to handle `202` responses: show spinner, poll
  `/operations/:id`, then poll `/groups/:gid/ready`
- [ ] Show success toast on completion, error toast on failure
- [ ] Timeout handling: show "Operation timed out" after 30 s
- [ ] E2E test: trigger step-down via UI, verify spinner appears, verify
  topology refreshes after completion

Files: `crowkv-console/shared/src/`, `crowkv-console/web/ui/src/`

## Task 7: Update Existing Tests

- [ ] Add `?sync=true` to existing deployment tests that call step-down,
  remove replica, add replica — preserves current behavior
- [ ] Add new async-mode tests: verify 202 response, poll operation
  status, verify completion
- [ ] Add readiness API tests: ready/not-ready/lagging scenarios

Files: `crowkv-server/tests/*`

## Task 8: Integration Verification

- [ ] Run `pixi run test-server` — all pass
- [ ] Run `pixi run test-ui` — all pass (with async GUI flow)
- [ ] Run `pixi run test-core` — no regressions
- [ ] Run `pixi run test-ct` — no regressions
- [ ] Verify `?sync=true` backward compat: existing tests unchanged
