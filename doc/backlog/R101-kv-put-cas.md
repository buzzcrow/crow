<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R101: kv — Compare-and-Set on Put

## Problem

**Current behavior + impact**

`KvSetRequest` (`lib/crow-kv/src/rpc/proto/kv.proto:29`) is a blind
overwrite — it has no `expected_revision` field. The KV server's `put`
handler (`lib/crow-kv/src/rpc/kv_service.rs:217`) calls `kv_put` →
`propose_and_respond` → paxos propose, with no precondition check. The
last writer wins, silently overwriting whatever was there before.

This is safe under R99's chunkdb ownership model (one instance owns a
chunk, so the in-process mutex in R100 serializes all writes). But it
leaves no defense-in-depth: if R99's ownership is ever bypassed (bug in
routing, misconfigured client, manual admin operation targeting the wrong
instance), two writers can race on the same key with no detection.

More broadly, the KV layer itself has no optimistic-concurrency primitive.
Any caller that does read-modify-write (chunkdb, future diskio, future
console tooling) must rely on external serialization (R100's lock) rather
than a KV-level guard. A CAS primitive at the KV layer would benefit all
read-modify-write callers, not just chunkdb.

**Design pointers**

- `doc/design/kv/design-crow-kv-group0.md` §2.1 — `crow-kv-client` is the
  single sysdata API surface; the CAS method would live here.
- `doc/design/chunkdb/design-crow-chunkdb.md` §9 (Chunk Lifecycle) —
  "Concurrency: KV CAS or state machine guards prevent conflicting
  transitions." R101 implements the KV CAS half of this statement (R100
  implements the state-machine-guard half via the in-process mutex).
- `lib/crow-kv/src/rpc/proto/kv.proto:29` — `KvSetRequest` message to
  extend.
- `lib/crow-kv/src/cluster/px_kv_store.rs:488` — `propose_and_respond`,
  the propose path where the CAS check would be inserted (read-before-
  propose, lease-protected).
- `lib/crow-kv/src/paxos/learner.rs:548` — `apply_entry`, the apply path
  (no change needed — CAS is checked at propose time, not apply time).
- `lib/crow-kv-client/src/client.rs:500` — `put` method to extend with
  an optional `expected_revision`.

**Use scenarios**

- **chunkdb read-modify-write with CAS** — `append_chunk` reads the chunk
  (gets revision N), mutates it, calls `put_chunk` with
  `expected_revision=N`. If another writer (bug, bypassed routing) wrote
  the same key between the read and the put, the CAS fails with
  `RevisionMismatch` → `LifecycleError::StateConflict` → the caller
  retries the read-modify-write. Defense-in-depth on top of R100's lock.
- **Console admin tooling** — a management tool reads a chunk record,
  modifies a field, writes it back with `expected_revision`. If the chunk
  was modified between read and write, the CAS fails and the tool
  re-reads and retries. No external lock needed.
- **Future diskio component** — a diskio-like component doing
  read-modify-write on block metadata uses CAS instead of an external
  lock, since it may not have a per-key mutex like chunkdb's.

## Solution

**One-line summary**

Add an optional `expected_revision` field to `KvSetRequest`; the KV
server checks the key's current revision (via a linearizable read) before
proposing; on mismatch, returns a CAS-failed response. The leader's lease
guarantees the read-then-propose sequence is atomic with respect to other
leaders.

**Why read-before-propose (not apply-time check)**

The KV server sends the response at propose-choose time
(`propose_and_respond` returns `ok_chosen(slot)` as soon as paxos chooses
the entry, `px_kv_store.rs:502`), NOT at apply time (apply is async,
`learner.rs:548`). An apply-time CAS check would mean the client gets
`ok` even when the CAS fails — requiring a second round-trip to discover
the failure. Read-before-propose avoids this: the leader reads the key's
current revision, checks it against `expected_revision`, and returns
CAS-failed immediately if it doesn't match — all before proposing. The
leader's lease (used for linearizable reads, design §"lease fast-path")
guarantees no other leader can interleave a write between the read and
the propose, so the check is authoritative.

**Numbered work items**

- **`KvSetRequest` proto extension** (`lib/crow-kv/src/rpc/proto/kv.proto`)
  — add `optional uint64 expected_revision = 10;` to `KvSetRequest`
  (field number 10, next available after the existing fields 1-9). When
  absent (0 / None), behavior is unchanged (blind overwrite, the default).
  When present, the server checks the key's current revision before
  proposing.
- **CAS check in the `put` handler**
  (`lib/crow-kv/src/rpc/kv_service.rs:217`) — before calling
  `store.kv_put`, if `req.expected_revision != 0`:
  - Do a linearizable read of the key (via the existing `kv_get` path or
    an internal `get_revision(group_id, key)` helper) to get the key's
    current revision (the paxos slot at which it was last written, or 0
    if the key does not exist).
  - If `current_revision != expected_revision`, return a CAS-failed
    `KvResponse` with a new error code `KV_ERROR_CAS_FAILED` and the
    current revision (so the client can retry with the correct
    revision without a separate read).
  - If `current_revision == expected_revision`, proceed with
    `store.kv_put` as today. The lease guarantees no interleaving.
- **`KV_ERROR_CAS_FAILED` error code** (`kv.proto` `KvErrorCode` enum)
  — new error code. The `KvResponse` on CAS failure includes the
  current revision in the `revision` field (normally the write's slot;
  on CAS failure, it's the key's current revision for client retry).
- **`put_cas` client method** (`lib/crow-kv-client/src/client.rs`) —
  new method `put_cas(store_id, group_id, key, value, expected_revision,
  ids) -> Result<WriteOutcome>`. Like `put` but passes
  `expected_revision` in the request. On `KV_ERROR_CAS_FAILED`, returns
  a new `Error::CasFailed { current_revision }` so the caller can retry.
  Alternatively, extend the existing `put` method with an optional
  `expected_revision: Option<u64>` parameter (preferred — one method,
  fewer code paths).
- **`ChunkStore::put_chunk_cas`** (`app/crow-chunkdb/src/storage.rs:51`)
  — new method or extend `put_chunk` with an optional
  `expected_revision: Option<u64>`. Passes the revision from the
  `get_chunk` read to the `put_cas` call. On `CasFailed`, returns
  `StoreError::CasFailed` → `LifecycleError::StateConflict` (the
  variant already exists at `lifecycle.rs:39`, currently unreachable).
  The lifecycle methods (append/seal/delete) can optionally retry the
  read-modify-write on `StateConflict` (bounded retries, e.g. 3).
- **`GetOutcome` revision propagation** — `GetOutcome::Found { value,
  revision }` already returns the key's revision
  (`client.rs:40`). `ChunkStore::get_chunk` must propagate this revision
  to the caller so it can be passed to `put_chunk_cas`. Currently
  `get_chunk` returns `Chunk` only; add a `get_chunk_with_revision`
  variant or return `(Chunk, u64)`.

**Flow diagram**

```
Client calls put_cas(key, value, expected_revision=N)
        │
        ▼
  KV server: put handler
        │
        ├─ expected_revision == 0? ──yes──► blind put (existing path)
        │                              no
        │                              ▼
        │                    linearizable read of key
        │                    (lease-protected)
        │                              │
        │                    current_revision == N?
        │                              │
        │              yes ┌───────────┴───────────┐ no
        │                  ▼                         ▼
        │            propose(value)           return CAS_FAILED
        │                  │                   (with current_revision)
        │                  ▼
        │            paxos chooses
        │                  │
        │                  ▼
        │            return ok(slot)
        │
        ▼
  Client: on ok → done (revision = slot)
          on CAS_FAILED → retry with current_revision
```

**Edge cases at a glance**

- `expected_revision == 0` and key does not exist → CAS passes (creating
  a new key with expected_revision=0 is the "create-if-absent" pattern).
- `expected_revision == 0` and key exists → CAS fails (key already
  exists). This gives chunkdb's `allocate_chunk` a KV-level existence
  check without a separate `get_chunk`.
- `expected_revision == N` and key was deleted after the read → the
  key's revision is still N (delete is a write at slot N); CAS passes
  and the put resurrects the key. If the caller wants "fail if deleted",
  they must check the value, not just the revision. (For chunkdb, the
  state check inside the lock handles this — CAS is defense-in-depth,
  not the primary guard.)
- Leader loses lease mid-read → the linearizable read may be stale; the
  propose will fail with `NotLeader` (existing path). The client retries
  against the new leader. No correctness issue — CAS is never applied on
  a stale leader.
- Key never written (revision=0) and `expected_revision=0` → CAS passes.
  This is the "create-if-absent" case.
- Batch write with CAS — `KvBatchWriteRequest` does not get CAS in this
  R-number (batch CAS is more complex — per-key expected revisions).
  Future extension if needed.

## Dependencies

- Depends on: existing KV server `put` path (`kv_service.rs:217`,
  `px_kv_store.rs:488`), existing `GetOutcome::Found { revision }`
  (`client.rs:40`), existing leader lease (linearizable read path).
- No proto-breaking changes — `expected_revision` is an optional field
  (field number 10); existing clients that don't set it get blind-overwrite
  behavior (backward compatible).
- **R100** (chunkdb lifecycle lock) — not a dependency. R101 is
  defense-in-depth on top of R100 + R99. R100's `StateConflict` variant
  (`lifecycle.rs:39`) is already defined and currently unreachable; R101
  wires it to the CAS-failure path.
- **R99** (dynamic range binding) — not a dependency. R99's ownership
  model is the primary correctness boundary; R101 is defense-in-depth if
  R99's ownership is ever bypassed.

## Acceptance

**CAS correctness**:

- `put_cas` with `expected_revision=N` on a key whose current revision
  is N → succeeds, returns `WriteOutcome { revision = new_slot }`. Unit
  test.
- `put_cas` with `expected_revision=N` on a key whose current revision
  is M (M != N) → fails with `CasFailed { current_revision = M }`. Unit
  test.
- `put_cas` with `expected_revision=0` on a non-existent key → succeeds
  (create-if-absent). Unit test.
- `put_cas` with `expected_revision=0` on an existing key → fails with
  `CasFailed { current_revision = current }`. Unit test.
- `put` (no `expected_revision`) on any key → succeeds (blind overwrite,
  backward compatible). Unit test.

**CAS under concurrency**:

- Two concurrent `put_cas` on the same key, both with
  `expected_revision=N` → exactly one succeeds, the other fails with
  `CasFailed`. Integration test.
- `put_cas` with `expected_revision=N` succeeds; a second `put_cas` with
  `expected_revision=N` (same stale revision) → fails (the first put
  advanced the revision). Integration test.

**Client retry**:

- `put_cas` fails with `CasFailed { current_revision = M }` → client
  re-reads, gets revision M, retries `put_cas` with
  `expected_revision=M` → succeeds. Integration test.

**chunkdb integration**:

- `ChunkStore::put_chunk_cas` with `expected_revision=N` on a chunk
  whose current revision is N → succeeds. Unit test.
- `ChunkStore::put_chunk_cas` with `expected_revision=N` on a chunk
  whose current revision is M → returns `StoreError::CasFailed` →
  `LifecycleError::StateConflict`. Unit test.
- `append_chunk` with CAS retry: CAS fails on first `put_chunk_cas` →
  `StateConflict` → re-read + re-mutate + retry `put_chunk_cas` →
  succeeds (within bounded retries). Integration test.
- `append_chunk` exhausts CAS retries (3 consecutive failures) → returns
  `StateConflict` to the caller. Integration test.

**Backward compatibility**:

- Existing `put` calls (no `expected_revision`) behave identically
  before and after R101. Unit test.
- Existing clients built against the old proto can still send `put`
  requests to a server upgraded with R101 (optional field). Integration
  test.

**Error mapping**:

- `CasFailed` maps to gRPC `ABORTED` with the current revision in the
  error detail (so the client can retry without a separate read). Unit
  test.

**Test commands**:

- `pixi run cargo test -p crow-kv --test cas_test` (new test file)
- `pixi run cargo test -p crow-kv-client --test put_cas_test`
- `pixi run cargo test -p crow-chunkdb --test lifecycle_test`
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy --all-targets -- -D warnings`

## Open Questions

- **CAS check location: read-before-propose vs apply-time.** This doc
  proposes read-before-propose (the leader reads the key's revision,
  checks it, then proposes). This relies on the leader's lease being
  valid for the duration of the read + propose. If the lease expires
  mid-sequence, the propose will fail with `NotLeader` (safe — the CAS
  is never applied on a stale leader). Alternative: apply-time check
  (encode `expected_revision` in the paxos payload; check at apply time
  in `learner.rs:548`; the client discovers the result via a second
  read). Apply-time is more complex (requires a second round-trip to
  discover failure) but doesn't depend on the lease. Default to
  read-before-propose — it's simpler, lower-latency, and the lease
  guarantee is already relied upon for linearizable reads. Revisit if
  lease reliability becomes a concern.
- **Extend `put` vs new `put_cas` method.** Two options: (a) add an
  optional `expected_revision: Option<u64>` parameter to the existing
  `put` method (one method, fewer code paths, but changes the signature
  for all callers), or (b) add a separate `put_cas` method (existing
  `put` unchanged, but two methods to maintain). Default to (a) —
  `Option<u64>` is backward-compatible and avoids duplicating the retry
  / not-leader / metrics logic. Revisit if the `put` signature becomes
  too unwieldy.
- **Batch CAS.** `KvBatchWriteRequest` does not get CAS in this
  R-number. If needed, it would require per-key `expected_revision`
  fields (a list parallel to the batch items). Deferred — no current
  caller needs batch CAS. File a follow-up if a use case emerges.
