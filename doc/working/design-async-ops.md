<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R12 Design — Async Operation API + Cluster Readiness

## Problem

Management API operations that trigger cluster state changes (step-down,
remove replica, add replica, stop server) execute synchronously. When the
GUI deletes a leader, the HTTP call blocks until the operation completes —
which may take seconds during leader re-election. There is no way for the
caller to trigger an operation and poll for completion asynchronously.

After a reconfig operation (e.g. adding a replica), there is no API to
check whether the new replica has caught up — the caller must guess or poll
`/health` and infer convergence from status levels.

## Proposed Approach

### 1. Operation Registry

An in-memory `OperationRegistry` tracks async operations. Each operation
has:

- **`operation_id`**: `u64`, monotonically increasing, process-unique.
- **`kind`**: enum — `StepDown`, `RemoveReplica`, `AddReplica`,
  `StopServer`.
- **`status`**: enum — `Pending`, `Running`, `Completed`, `Failed`.
- **`target`**: `{store_id, group_id, replica_id?}` for status display.
- **`started_at`**, **`completed_at`**: timestamps for TTL cleanup.
- **`error`**: optional error message on `Failed`.

The registry is a `DashMap<u64, Operation>` wrapped in `Arc`, shared
between the HTTP handler and the async task. A background cleanup task
removes completed operations after 5 minutes.

### 2. Async Operation Flow

```
Client                    Server
  |--- POST /step-down --->|
  |                        | create Operation(Pending)
  |                        | spawn tokio task:
  |                        |   set status=Running
  |                        |   execute step_down logic
  |                        |   set status=Completed/Failed
  |<-- 202 {operation_id} -|
  |                        |
  |--- GET /operations/123 -->|
  |<-- 200 {status, ...} ----|
  |                        |
  | (poll until Completed)   |
```

The HTTP handler creates the operation, spawns a tokio task to execute the
actual logic, and returns `202 Accepted` with the operation ID immediately.

### 3. Which Operations Become Async

Only operations that may take >1 s or involve cluster state transitions:

- **Step-down**: leader steps down, cluster re-elects. The step-down itself
  is fast, but the subsequent election takes time. The operation is marked
  `Completed` when a new leader is observed (via polling group status).
- **Remove replica (leader)**: removing the leader triggers re-election.
  Same as step-down — `Completed` when new leader appears.
- **Remove replica (non-leader)**: fast, remains synchronous. No state
  transition.
- **Add replica**: the add itself is fast, but the new replica needs to
  catch up. The operation is marked `Completed` when the new replica's
  `contiguous_applied` reaches the group's max applied slot (within a
  threshold). Timeout: 30 s.
- **Stop server**: graceful drain. `Completed` when the server process is
  confirmed stopped.

Simple operations (add rack, add node, list stores, deploy server, ping)
remain synchronous.

### 4. Cluster Readiness API

`GET /stores/:sid/groups/:gid/ready` — lightweight convergence check.

**Response (200 OK):**
```json
{
  "ready": true,
  "leader_id": 2,
  "term": 5,
  "voting_replicas": 3,
  "reachable_replicas": 3,
  "max_applied_slot": 42,
  "min_applied_slot": 42,
  "lag": 0
}
```

**Response (503 Service Unavailable):**
```json
{
  "ready": false,
  "reason": "no leader elected",
  "leader_id": 0,
  "voting_replicas": 3,
  "reachable_replicas": 2,
  "max_applied_slot": 42,
  "min_applied_slot": 10,
  "lag": 32
}
```

**Readiness criteria:**
- `leader_id != 0` (a leader has been elected)
- All voting replicas are reachable (status != `Unreachable`)
- `lag <= threshold` (default threshold: 5 slots). Lag = max_applied -
  min_applied across all voting replicas.

This is a fast, non-blocking check — it reads in-memory state only, no
network probes. The `reachable` status is based on the last known RPC
result (heartbeat timestamp within election timeout).

### 5. API Endpoints

New routes added to `mgmt_api.rs`:

- `GET /operations/:id` — poll operation status
- `GET /stores/:sid/groups/:gid/ready` — cluster readiness check

Modified routes (return `202` + operation_id for async cases):

- `POST /stores/:sid/groups/:gid/step-down` — now async
- `DELETE /stores/:sid/groups/:gid/remotes/:rid` — async when removing
  the leader, synchronous otherwise
- `POST /stores/:sid/groups/:gid/remotes` — async (waits for catch-up)

### 6. Backward Compatibility

The existing synchronous behavior is preserved by adding a `?sync=true`
query parameter. When `sync=true`, the endpoint blocks until the operation
completes and returns the original response format. This allows existing
tests and CLI commands to work without changes.

Default behavior changes to async (`202 Accepted`). The GUI and new tests
use the async pattern.

### 7. GUI Integration

The UI flow for an async operation:

1. User triggers action (e.g. click "Delete Node" on a leader)
2. UI sends the request, gets `202 {operation_id}`
3. UI shows a spinner/toast: "Operation in progress..."
4. UI polls `GET /operations/:id` every 1 s
5. On `Completed`: UI polls `GET /groups/:gid/ready` to verify convergence
6. On `ready=true`: UI refreshes topology, shows success toast
7. On `Failed`: UI shows error toast with the error message
8. Timeout: if not completed within 30 s, UI shows "Operation timed out"

## Alternatives Considered

- **SSE/WebSocket streaming**: push updates instead of polling. Rejected
  for v1 — adds complexity, and polling at 1 s interval is sufficient for
  management operations that take seconds.

- **Full async for all operations**: make every management operation async.
  Rejected — simple operations (add rack, list stores) are fast and benefit
  from synchronous simplicity.

- **Readiness via existing `/health`**: the `/health` endpoint returns
  `StatusLevel` which is too coarse for convergence checking. A dedicated
  `/ready` endpoint with lag metrics is more useful.

## Acceptance Criteria

- `POST /step-down` returns `202 {operation_id}` immediately
- `GET /operations/:id` returns correct status at each phase
- `GET /groups/:gid/ready` returns `200` when ready, `503` when not
- `?sync=true` preserves backward-compatible synchronous behavior
- Async step-down operation completes when new leader is elected
- Async add-replica operation completes when new replica catches up
- GUI shows operation progress instead of blocking
- Tests can poll `/ready` to verify convergence after reconfig
- Existing tests pass with `?sync=true` (no behavior change)
