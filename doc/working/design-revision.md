<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: Track Revision for Reads (R1)

## Problem

`kv_get` discards the per-key slot returned by `engine_get`. The `revision`
field in `KvResponse` is set to `0` for all read paths (`ok_value`,
`not_found`). Clients cannot determine which logical version of a key they
read, limiting:

- Linearizable read correctness verification (no way to compare the read
  revision against the leader's current frontier).
- Revision-based snapshot reads (a client cannot use the read revision as a
  `client_slot` for a subsequent `READ_YOUR_WRITES` read).

## Current Behavior

`px_kv_store.rs` `kv_get` calls `engine_get(key)` which returns
`Option<(SlotIndex, Vec<u8>)>`. The slot is explicitly discarded:

```rust
.map(|(_slot, v)| v);
```

The response is then built via `KvResponse::ok_value(v, ...)` or
`KvResponse::not_found(...)`, both of which hardcode `revision: 0`.

For writes, `propose_and_respond` already sets `revision` to the chosen Paxos
slot via `KvResponse::ok_chosen(slot, ...)`.

## Proposed Approach

Stamp the per-key slot from `engine_get` into the `revision` field of the read
response. This makes `revision` consistent between reads and writes: it is
always the Paxos slot at which the returned value was last written.

Changes:

- `px_kv_store.rs` `kv_get`: stop discarding the slot from `engine_get`; pass
  it through to the response builder.
- `kv_response.rs`: add `ok_value_with_revision(value, revision, ...)` and
  `not_found_with_revision(revision, ...)` constructors (or extend the existing
  ones) so the revision is propagated.
- `not_found` responses get `revision = 0` (key absent → no write slot).

No proto changes needed — `revision` (field 3) already exists.

## Alternatives Considered

- **Use `read_slot` as revision**: rejected. `read_slot` is the replica's
  applied frontier, not the per-key write slot. A key last written at slot 5
  read on a replica at frontier 100 would report `revision=100`, which is
  misleading — the key hasn't changed since slot 5.
- **Add a new proto field `key_slot`**: rejected. `revision` already exists and
  its proto comment says "logical version / LSN"; the per-key slot is exactly
  that.

## Acceptance Test Plan

- Unit test: put a key at slot N, then `kv_get` returns `revision == N`.
- Unit test: overwrite the key at slot M > N, then `kv_get` returns
  `revision == M`.
- Unit test: `kv_get` on a missing key returns `revision == 0`.
- Unit test: `kv_get` after delete returns `revision == 0` (key absent).
- Existing tests pass (the `revision` field was already `0` for reads, so no
  consumer can break by receiving the actual slot).
