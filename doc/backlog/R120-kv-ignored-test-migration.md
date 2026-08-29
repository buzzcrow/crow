<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R120: kv — revive crowdb-rpc migrated ignored tests

Two `#[ignore]`d test stubs in `lib/crowdb-kv/tests/group_test/` were left
behind when the wire transport migrated from tonic to crowdb-rpc. Both are
empty function bodies (`{}`) with an ignore reason pointing at the missing
migration path. The underlying crowdb-rpc infrastructure now exists — the
tests just need to be written.

**Current behavior + impact:** `group_test.rs` reports `7 ignored` (after
R120's sibling cleanup deleted the 5 watch-notify stubs, this drops to
`2 ignored`). The two remaining stubs cover real contracts that have no
other test coverage at the `crowdb-kv` layer:
- The forwarded-flag loop guard on `Get`/`Scan` (a follower must not
  re-forward an already-forwarded request).
- Malformed `Accept` rejection on the LearnerStream bidi path.

**Design pointers:** `design/kv/design-crowdb-kv-rpc.md` §3 (crowdb-rpc
flatbuffer framing) and §4 (KvService request/response schema, including
the `forwarded` flag). `design/kv/design-crowdb-kv-consensus.md` §6
(LearnerStream accept validation).

**Use scenarios:**
- A client sends a `Get` to a follower; the follower forwards it to the
  leader with `forwarded = true`. The leader processes it and returns the
  value — it must not re-forward (loop guard). Expected: the client
  receives the value, no infinite forward loop.
- A proposer sends a malformed `Accept` frame (bad slot, bad ballot, or
  truncated payload) over the LearnerStream bidi path. Expected: the
  server rejects the frame and closes the stream, no panic or state
  corruption.

## Solution

**One-line summary:** Write the two ignored test bodies against the
existing crowdb-rpc flatbuffer and LearnerStream APIs.

1. **`forwarded_request_does_not_re_forward`** —
   `lib/crowdb-kv/tests/group_test/kv_forward_test.rs`. Use the existing
   `start_cluster` + `cluster.kv_client` harness. Write a key through the
   leader, wait for Paxos propagation to the follower, then send a `Get`
   to the follower with `forwarded = true` set in the flatbuffer request.
   Assert the follower returns `not_found` (it does not re-forward).
   Requires extending `TestKvClient::get` (or adding a `get_forwarded`
   variant) to pass the `forwarded` flag through `KvRpcTransport::send_get`.

2. **`malformed_accept_request_is_rejected_by_rpc_boundary`** —
   `lib/crowdb-kv/tests/group_test/paxos_error_test.rs`. Construct a raw
   crowdb-rpc frame with an invalid `Accept` payload (e.g. slot = 0 or
   ballot with `round = 0, leader_id = 0`) and send it over a
   `PxRpcTransport` bidi stream to a running server. Assert the stream
   is closed with an error, no panic. Requires a test helper to build
   and send raw LearnerStream frames (may already exist in
   `rpc_migration_test.rs` — check `send_accept` helpers).

**Edge cases at a glance:**
- `forwarded = true` on a leader (not a follower) → leader processes
  normally, no forward needed → returns value.
- `forwarded = true` on a follower with empty local store → returns
  `not_found`, does not forward.
- Malformed Accept with valid slot but invalid ballot → rejected.
- Malformed Accept with truncated payload → stream closed, no panic.

## Dependencies

- None — all infrastructure (crowdb-rpc transport, flatbuffer `forwarded`
  flag, LearnerStream bidi path) is already landed.

## Acceptance

- `forwarded = true` Get to follower with value in leader only → follower
  returns `not_found`, does not forward. Integration test.
- `forwarded = true` Get to leader → leader returns the value normally.
  Integration test.
- `forwarded = false` Get to follower (existing behavior) → follower
  forwards to leader, returns value. Integration test (already covered by
  `follower_get_forwards_to_leader_after_local_clear`).
- Malformed Accept (slot = 0) over LearnerStream → stream rejected, no
  panic. Integration test.
- Malformed Accept (truncated payload) over LearnerStream → stream
  closed, no panic. Integration test.
- `pixi run test-kv-core` reports `0 ignored` (after both tests are
  un-ignored and passing).
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`
