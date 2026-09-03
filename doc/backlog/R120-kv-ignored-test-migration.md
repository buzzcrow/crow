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
- A proposer sends a malformed `Accept` frame (a valid flatbuffer of
  the wrong table type, e.g. `FBPrepareRequest` sent with `msg_type =
  EAcceptRequest`) over the unary RPC to a running server. Expected:
  the server's `flatbuffers::root::<FBAcceptRequest>` verification
  fails, it rejects the frame with `InvalidArgument` and returns an
  error response, no panic or state corruption.
- A proposer sends an `Accept` frame with a missing required `value`
  field (valid `FBAcceptRequest` flatbuffer but `value = None`).
  Expected: the server parses the flatbuffer but rejects it at the
  `fb_req.value()` guard with `InvalidArgument`, no panic.

## Solution

**One-line summary:** Write the one remaining ignored test body against
the existing crowdb-rpc unary Accept path and flatbuffer deserialization
guard.

1. **`malformed_accept_request_is_rejected_by_rpc_boundary`** —
   `lib/crowdb-kv/tests/group_test/paxos_error_test.rs`. Start a
   cluster via `start_cluster`, open a `px_client` to a node, and send
   two malformed `EAcceptRequest` frames: (a) a valid `FBPrepareRequest`
   flatbuffer sent with `msg_type = EAcceptRequest` (wrong table type —
   `flatbuffers::root::<FBAcceptRequest>` verification fails), and
   (b) a valid `FBAcceptRequest` with `value = None` (passes root
   parse but rejected at the `fb_req.value()` guard). Assert both
   responses indicate an error (not `Success`), server does not panic.
   Requires a `test-util`-gated `send_raw_request` on `PxRpcTransport`
   and a `send_raw_accept` facade on `TestPxClient` to send arbitrary
   control bytes with `msg_type = EAcceptRequest`.

**Edge cases at a glance:**
- Wrong table type (`FBPrepareRequest` as `EAcceptRequest`) →
  `flatbuffers::root` verification fails → `InvalidArgument`, no panic.
- Missing `value` field → handler's `value()` guard rejects →
  `InvalidArgument`, no panic.
- Completely corrupted bytes (e.g. `[0xDE, 0xAD, 0xBE, 0xEF]`) → NOT
  tested: the C++ RPC layer does not deliver frames with invalid
  control buffers to the handler (request times out). The wrong-table-
  type approach achieves the same `flatbuffers::root` failure while
  passing C++ frame validation.
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
- Wrong table type (`FBPrepareRequest` sent as `EAcceptRequest`) to a
  running server → response is invalid as `FBAcceptedResponseRef` or
  has `ret_code != Success`, server does not panic. Integration test.
- Missing `value` field (`FBAcceptRequest` with `value = None`) to a
  running server → same: error response, no panic. Integration test.

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
