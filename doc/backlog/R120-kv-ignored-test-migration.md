<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R120: kv — revive crowdb-rpc migrated ignored tests

One `#[ignore]`d test stub remains in `lib/crowdb-kv/tests/group_test/`
after the wire transport migrated from tonic to crowdb-rpc. It is an
empty function body (`{}`) with an ignore reason pointing at the missing
migration path. The underlying crowdb-rpc infrastructure now exists —
the test just needs to be written.

The sibling stub (`forwarded_request_does_not_re_forward` in
`kv_forward_test.rs`) was already implemented: it uses the existing
`TestKvClient::get_with_forwarded` → `KvRpcTransport::send_get_with_forwarded`
path and asserts the loop-guard contract (a follower receiving
`forwarded = true` returns a not-leader hint instead of re-forwarding).

**Current behavior + impact:** `group_test.rs` reports `1 ignored`
(after the sibling cleanup deleted the 5 watch-notify stubs and the
forwarded-loop-guard stub was implemented). The remaining stub covers a
real contract that has no other test coverage at the `crowdb-kv` layer:
- Malformed `Accept` rejection at the crowdb-rpc flatbuffer boundary
  (a corrupted or truncated `EAcceptRequest` frame must be rejected
  with `InvalidArgument`, no panic or state corruption).

**Design pointers:** `design/kv/design-crowdb-kv-rpc.md` §4
(Server-Side Handler — `EAcceptRequest` → `handle_accept`, flatbuffer
deserialization guard that returns `FBKvRetCode::InvalidArgument` on
parse failure). `design/kv/design-crowdb-kv-rpc-client.md` §6
(Transparent Leader-Forwarding — the `forwarded: bool` loop-guard
field, already covered by the implemented test).

**Use scenarios:**
- A proposer sends a malformed `Accept` frame (corrupted flatbuffer
  bytes that fail `flatbuffers::root::<FBAcceptRequest>` deserialization)
  over the unary `EAcceptRequest` RPC to a running server. Expected:
  the server rejects the frame with `InvalidArgument` and returns an
  error response, no panic or state corruption.
- A proposer sends an `Accept` frame with a truncated payload (control
  buffer shorter than the flatbuffer header requires). Expected: same
  — deserialization fails, `InvalidArgument` returned, no panic.

## Solution

**One-line summary:** Write the one remaining ignored test body against
the existing crowdb-rpc unary Accept path and flatbuffer deserialization
guard.

1. **`malformed_accept_request_is_rejected_by_rpc_boundary`** —
   `lib/crowdb-kv/tests/group_test/paxos_error_test.rs`. Start a
   cluster via `start_cluster`, open a `px_client` to a node, and send
   a raw crowdb-rpc frame with `msg_type = EAcceptRequest` but a
   corrupted control buffer (e.g. random bytes or a truncated
   flatbuffer). Assert the response carries `FBKvRetCode::InvalidArgument`
   (or the `PxReplicaError::Internal` mapping thereof) and the server
   did not panic. The rejection happens in `PxRpcService::handle_accept`
   at the `flatbuffers::root::<FBAcceptRequest>(req.control())` guard
   — before any consensus logic runs. Requires a test helper to build
   and send a raw `EAcceptRequest` frame with arbitrary control bytes;
   the existing `send_accept` helper in `rpc_migration_test.rs` builds
   a valid frame, so a low-level variant that accepts raw control
   bytes is needed (or use the `px_client` transport's raw call path).

**Edge cases at a glance:**
- Corrupted control buffer (random bytes) → deserialization fails →
  `InvalidArgument` returned, no panic.
- Truncated control buffer (too short for flatbuffer header) → same.
- Valid flatbuffer with slot = 0 → NOT rejected as malformed; slot 0
  is a valid Paxos slot, processed normally by `on_accept_inner`.
- Valid flatbuffer with ballot (0, 0) → NOT rejected as malformed;
  it is the minimum ballot, accepted or rejected by the acceptor CAS
  based on the current promised ballot.

## Dependencies

- None — all infrastructure (crowdb-rpc transport, `PxRpcService`
  handler with flatbuffer deserialization guard, unary Accept path)
  is already landed.

## Acceptance

**Malformed Accept rejection:**
- Corrupted `EAcceptRequest` control buffer (random bytes) sent to a
  running server → response carries `InvalidArgument` (or mapped
  `PxReplicaError::Internal`), server does not panic. Integration
  test.
- Truncated `EAcceptRequest` control buffer (empty or too short) sent
  to a running server → same: `InvalidArgument`, no panic. Integration
  test.

**Already-satisfied (forwarded loop-guard — no new work):**
- `forwarded = true` Get to follower → follower returns not-leader
  hint, does not re-forward. Integration test (already covered by
  `forwarded_request_does_not_re_forward` in `kv_forward_test.rs`).
- `forwarded = false` Get to follower → follower forwards to leader,
  returns value. Integration test (already covered by
  `follower_get_forwards_to_leader_after_local_clear`).

**Final gate:**
- `pixi run test-kv-core` reports `0 ignored` (after the remaining
  test is un-ignored and passing).
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
