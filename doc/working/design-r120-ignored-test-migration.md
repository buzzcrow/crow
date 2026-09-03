<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Revive crowdb-rpc migrated ignored test: malformed Accept rejection (R120)

Design draft for R120 — implementing the one remaining `#[ignore]`d
test stub in `lib/crowdb-kv/tests/group_test/paxos_error_test.rs`.

- Backlog: `doc/backlog/R120-kv-ignored-test-migration.md`
- Root design: `doc/design/kv/design-crowdb-kv-rpc.md` §4 (Server-Side
  Handler — `PxRpcService::handle_accept` flatbuffer deserialization
  guard).
- Already landed: the crowdb-rpc transport (`PxRpcTransport`), the
  server handler (`PxRpcService::handle_accept`), the flatbuffer
  deserialization guard that returns `FBKvRetCode::InvalidArgument` on
  parse failure, and the `test-util` feature on `crowdb-kv`.

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. Raw-frame test helper on `PxRpcTransport`

### 1.1 Why

The existing `PxRpcTransport::send_accept` builds a *valid*
`FBAcceptRequest` flatbuffer from a `PxLogEntry` — there is no way to
inject corrupted control bytes through the normal API. The test needs
to send a frame with `msg_type = EAcceptRequest` but arbitrary
(garbage) control bytes to verify the server's deserialization guard
rejects it. A `test-util`-gated method on `PxRpcTransport` is the
minimal entry point: it reuses the existing connection pool, request
ID allocation, and `RpcClient::call` path, only skipping the
flatbuffer build step.

### 1.2 Method signature

```rust
#[cfg(feature = "test-util")]
pub async fn send_raw_request(
    &self,
    rpc_endpoint: &str,
    msg_type: u16,
    control: Buffer,
) -> Result<Response, PxReplicaError>
```

a. Allocate a request ID via `self.next_id()`.
b. Get a connection via `self.conn_for(rpc_endpoint)`.
c. Call `self.rpc.call(&self.server, &conn, req_id, control, None, msg_type)`.
d. Await the `CallFuture`, map errors via `self.map_rpc_err`.
e. Return the raw `Response` — the test inspects `resp.control` bytes.

Edge cases:
- Empty control buffer (`buf.len() < 4`) → server's
  `flatbuffers::root::<FBAcceptRequest>` fails → `InvalidArgument`
  response returned, no panic.
- Random bytes → same: deserialization fails, `InvalidArgument`.
- Transport error (server down) → `PxReplicaError::Internal` from
  `map_rpc_err`, test should not reach the assertion.

## 2. Test client facade method

### 2.1 `TestPxClient::send_raw_accept`

```rust
#[cfg(feature = "test-util")]
pub async fn send_raw_accept(
    &self,
    control: Vec<u8>,
) -> Result<Vec<u8>, TestRpcStatus>
```

a. Call `self.transport.send_raw_request(&self.endpoint,
   FBMsgType::EAcceptRequest.0 as u16, Buffer::from_bytes(control))`.
b. On success, return `resp.control.bytes().to_vec()` (the raw
   response control buffer for the test to inspect).
c. On error, map to `TestRpcStatus`.

This keeps `TestPxClient` as the single test entry point — the test
does not touch `PxRpcTransport` directly.

## 3. Test: `malformed_accept_request_is_rejected_by_rpc_boundary`

### 3.1 Test body

a. `start_cluster(&[0, 1], 0)` — 2-node cluster, node 0 is leader.
b. `let client = cluster.px_client(leader).await` — get a `TestPxClient`
   to the leader.
c. **Wrong table type**: build a valid `FBPrepareRequest` flatbuffer
   and send it via `send_raw_accept` (which sets `msg_type =
   EAcceptRequest`). The handler's
   `flatbuffers::root::<FBAcceptRequest>` verification fails because
   the vtable layout doesn't match.
d. Assert the RPC completes (server did not panic): the result is
   `Ok(resp_bytes)`.
e. Parse the response as `FBAcceptedResponseRef` and assert
   `!valid()` OR `ret_code() != Success` — the server rejected the
   frame.
f. **Missing required field**: build a valid `FBAcceptRequest` with
   `value = None`. The handler parses it successfully but rejects it
   at the `fb_req.value()` guard with `InvalidArgument`.
g. Same assertion: `!valid()` OR `ret_code() != Success`.
h. `cluster.shutdown().await`.

### 3.2 Why two rejection paths

The `handle_accept` handler has two `InvalidArgument` rejection guards:
(1) `flatbuffers::root::<FBAcceptRequest>` fails (wrong table type or
corrupted bytes), and (2) `fb_req.value()` is `None` (valid flatbuffer
but missing required field). Both produce `submit_error` responses. The
test exercises both to cover the full rejection surface.

Sending completely corrupted bytes (e.g. `[0xDE, 0xAD, 0xBE, 0xEF]`)
was attempted first but the C++ RPC layer does not deliver frames with
invalid control buffers to the handler — the request times out at the
client. Using a valid flatbuffer of the wrong table type achieves the
same `flatbuffers::root` verification failure while passing C++ frame
validation.

### 3.3 Why parse as `FBAcceptedResponseRef`

The `send_accept` client path parses Accept responses as
`FBAcceptedResponseRef`. The `submit_error` helper builds a
`FBPromiseResponse` buffer (shared by all error paths) and submits it
with `msg_type = EAcceptedResponse`. Since `FBPromiseResponse` and
`FBAcceptedResponse` have different vtable layouts (field 8:
`previously_accepted` vs `rejected`), `parse_root::<FBAcceptedResponse>`
fails verification → `valid()` returns `false`. This is exactly what
the production `send_accept` path observes: it returns
`PxReplicaError::Internal("accept response malformed")`. The test
asserts the same invariant from the raw response.

Edge cases:
- Wrong table type (`FBPrepareRequest` as `EAcceptRequest`) →
  `flatbuffers::root` fails, `InvalidArgument` response, `valid()` false.
- Missing `value` field → handler's `value()` guard rejects,
  `InvalidArgument` response, `valid()` false.
- Valid `FBAcceptRequest` with slot=0 → NOT tested here (slot 0 is a
  valid Paxos slot, not malformed — covered by the edge-case note in
  the backlog doc).

## Scope

- `lib/crowdb-kv/src/rpc/px_rpc_transport.rs` — add
  `#[cfg(feature = "test-util")] pub async fn send_raw_request`.
- `lib/crowdb-kv/tests/common/test_client.rs` — add
  `#[cfg(feature = "test-util")] pub async fn send_raw_accept` on
  `TestPxClient`.
- `lib/crowdb-kv/tests/group_test/paxos_error_test.rs` — implement
  `malformed_accept_request_is_rejected_by_rpc_boundary`, remove
  `#[ignore]`, update the comment.

## Complexity

Low. No production logic changes — the rejection path
(`handle_accept` → `flatbuffers::root` guard → `submit_error`) already
exists. The only new code is a `test-util`-gated raw-frame send method
(~15 lines) and the test body (~30 lines). The main challenge is
understanding the response shape: `submit_error` builds a
`FBPromiseResponse` buffer for all error paths, so the test must
account for the cross-type parse failure rather than checking
`ret_code` directly.

## Test Design

### Unit tests (UT)

None — no new pure logic.

### End-to-end tests (E2E)

- **`malformed_accept_request_is_rejected_by_rpc_boundary`** —
  `start_cluster(&[0, 1], 0)` → `px_client(leader)` →
  `send_raw_accept(FBPrepareRequest bytes)` (wrong table type) →
  assert response is invalid as `FBAcceptedResponseRef` or has
  `ret_code != Success` → server did not panic. Integration test.
- **Missing-value variant** — same setup →
  `send_raw_accept(FBAcceptRequest with value=None)` → same
  assertion. Integration test (same test function, second block).

## Module Structure

```
lib/crowdb-kv/src/rpc/px_rpc_transport.rs    # +send_raw_request (test-util)
lib/crowdb-kv/tests/common/test_client.rs     # +send_raw_accept (test-util)
lib/crowdb-kv/tests/group_test/paxos_error_test.rs  # implement test, un-ignore
```

## Config Extensions

None.

## Server Wiring

None — no server changes.
