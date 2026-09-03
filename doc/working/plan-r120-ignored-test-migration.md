# Revive crowdb-rpc migrated ignored test: malformed Accept rejection (R120) Plan

Design: `doc/working/design-r120-ignored-test-migration.md`.
Backlog: `doc/backlog/R120-kv-ignored-test-migration.md`.
Goal: implement the one remaining `#[ignore]`d test —
`malformed_accept_request_is_rejected_by_rpc_boundary` — and un-ignore
it so `pixi run test-kv-core` reports `0 ignored`.

## Test infrastructure

- [x] **Add `send_raw_request` to `PxRpcTransport`**: `#[cfg(feature =
  "test-util")]` method that sends arbitrary control bytes with a given
  `msg_type` via `RpcClient::call` and returns the raw `Response`.
  Files: `lib/crowdb-kv/src/rpc/px_rpc_transport.rs`.
- [x] **Add `send_raw_accept` to `TestPxClient`**: `#[cfg(feature =
  "test-util")]` facade that calls `send_raw_request` with
  `EAcceptRequest` msg_type and returns the raw response control bytes.
  Files: `lib/crowdb-kv/tests/common/test_client.rs`.

## Test implementation

- [x] **Implement `malformed_accept_request_is_rejected_by_rpc_boundary`**:
  write the test body (wrong-table-type `FBPrepareRequest` + missing-value
  `FBAcceptRequest`), remove `#[ignore]`, update the comment. Files:
  `lib/crowdb-kv/tests/group_test/paxos_error_test.rs`.

## Verification

- [x] **Run affected tests**: `pixi run test-kv-core` — confirmed `0
  ignored`, all tests pass (98 group_test + 559 other tests).
- [x] **fmt + clippy**: `pixi run cargo fmt --all -- --check` +
  `pixi run cargo clippy --all-targets -- -D warnings` — both pass.

## File list

- `lib/crowdb-kv/src/rpc/px_rpc_transport.rs` — add `send_raw_request`
  (test-util gated).
- `lib/crowdb-kv/tests/common/test_client.rs` — add `send_raw_accept`
  on `TestPxClient` (test-util gated).
- `lib/crowdb-kv/tests/group_test/paxos_error_test.rs` — implement
  test body, remove `#[ignore]`, update comment.

## Test checklist

- [x] `malformed_accept_request_is_rejected_by_rpc_boundary` —
  wrong-table-type `FBPrepareRequest` sent as `EAcceptRequest` →
  server returns error response (not `Accepted`), no panic. Integration
  test.
- [x] Missing-value variant — `FBAcceptRequest` with `value = None` →
  same: error response, no panic. Integration test (same function,
  second block).
- [x] `pixi run test-kv-core` reports `0 ignored`.
