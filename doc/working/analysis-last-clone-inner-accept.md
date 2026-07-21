<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Analysis: Eliminate the Last Clone in `inner_accept`

## Current State (Post-R15)

R15 changed `Acceptor::accept` and `ReplicaHandler::on_accept` to take
`&PxLogEntry`, eliminating all redundant clones in the accept path. The
only remaining clone is in `inner_accept` (`acceptor.rs:124`):

```rust
if node.cas_accepted(accepted_ptr, entry.clone()).is_ok() {
```

`cas_accepted` takes `new: PxLogEntry` (owned) because it
`Box::into_raw(Box::new(new))` stores the entry in the slot node. Since
`inner_accept` holds `&PxLogEntry`, it must clone to produce an owned
value.

## Why the Clone Exists

- `cas_accepted` does `Box::into_raw(Box::new(new))` — the slot node
  must own its `PxLogEntry` (stored behind a raw pointer in an
  `AtomicPtr`).
- `inner_accept` receives `&PxLogEntry` from `accept`, which receives
  `&PxLogEntry` from `on_accept`.
- The callers (`on_accept` in `local_replica.rs`, `handle_accept_inner`
  in `px_service.rs`, `run_accept_phase` in `group.rs`) all need `entry`
  after the `accept` call — for WAL encode, `learn_chosen`, or
  `send_accept`. So `on_accept` cannot take owned `PxLogEntry` and move
  it into the acceptor without changing these post-accept uses.

## Two Paths to the Clone

### Path 1: Follower receives Accept RPC

```
gRPC deserialization → owned PxLogEntry (entry)
  → on_accept(&entry)
    → acceptor.accept(&entry)
      → inner_accept(&entry)
        → entry.clone() → cas_accepted  [CLONE]
  → learn_chosen(&entry)  [uses entry after accept]
```

The gRPC layer already deserialized into an owned `PxLogEntry`. In
principle, this owned value could be moved into the slot node with zero
clones. The blocker: `handle_accept_inner` (`px_service.rs:547`) calls
`learn_chosen(&entry)` after `on_accept` returns. If `entry` were moved
into the acceptor, it would no longer be available.

**Possible fix**: After `on_accept` succeeds, read the accepted entry
back from the slot node (via `acceptor.accepted_at(slot)`) and pass that
to `learn_chosen`. This adds one `accepted_cloned()` call (which clones
from the slot node), but that clone already happens in other code paths
(e.g., `learn_chosen` itself calls `entry.clone()` for the learner).
Net: same number of clones, but the clone moves from `inner_accept` to
`learn_chosen` — no savings.

**Alternative fix**: Change `on_accept` to return the owned `PxLogEntry`
on success (move it through), and let the caller pass it to
`learn_chosen`. This requires `accept` to return the entry, which means
`inner_accept` must not move it into the slot node — contradiction.

**Real fix**: Change `cas_accepted` to take `&PxLogEntry` and clone
internally. This just moves the clone from `inner_accept` to
`cas_accepted` — same number of clones, no savings. But it does
eliminate the clone-on-race-retry: if `cas_accepted` clones internally
only when the CAS succeeds, the retry loop avoids cloning on every
failed attempt.

### Path 2: Leader local accept

```
base_entry() → owned PxLogEntry (constructed)
  → run_accept_phase(&entry)
    → on_accept(&entry)
      → acceptor.accept(&entry)
        → inner_accept(&entry)
          → entry.clone() → cas_accepted  [CLONE]
    → send_accept(&entry, ...)  [uses entry after accept, borrows only]
```

The leader constructs the entry and needs it for `send_accept` after
local accept. `send_accept` only borrows `entry` (reads fields, clones
`payload` for the protobuf `AcceptRequest`). So the leader cannot move
`entry` into the acceptor either.

## The Retry Loop Problem

`inner_accept` has a CAS retry loop. On each iteration, `entry.clone()`
is called. If `cas_accepted` fails (race), the cloned `PxLogEntry` is
dropped (`Box::from_raw` + drop in the `Err` arm). On the next
iteration, `entry.clone()` is called again.

With `Bytes` payload, each clone is an O(1) ref-count bump and each
drop is an O(1) ref-count decrement. The cost is negligible. But if
payloads were ever not `Bytes`-backed, this would be O(n) per retry
iteration.

**Optimization**: Move the clone outside the loop, or make
`cas_accepted` clone internally only on success:

```rust
// Option A: clone once before the loop
let owned = entry.clone();
loop {
    // ...
    if node.cas_accepted(accepted_ptr, owned).is_ok() {  // move on success
        return Some(PxAcceptResult::Accepted { slot, ballot });
    }
    // On failure, cas_accepted drops the Box but we've lost `owned`.
    // Need to re-clone for the next iteration — same as today.
}
```

This doesn't work because `cas_accepted` takes ownership and drops on
failure. We'd need to re-clone every iteration anyway.

**Option B**: Change `cas_accepted` to take `&PxLogEntry` and clone
internally only when the CAS succeeds:

```rust
pub fn cas_accepted(
    &self,
    expected: *mut PxLogEntry,
    new: &PxLogEntry,
) -> Result<*mut PxLogEntry, *mut PxLogEntry> {
    let new_ptr = Box::into_raw(Box::new(new.clone()));  // clone only here
    match self.accepted.compare_exchange(...) {
        Ok(old) => { ... Ok(new_ptr) }
        Err(actual) => { drop(Box::from_raw(new_ptr)); Err(actual) }
    }
}
```

This still clones on every attempt (success or failure), because the
clone must happen before the CAS. No savings.

**Option C**: Double-buffered CAS — clone once, retry with the same
owned value:

```rust
pub fn cas_accepted_retry(
    &self,
    expected_ptr: &mut *mut PxLogEntry,
    new: PxLogEntry,  // owned, reused across retries
) -> Result<(), *mut PxLogEntry> {
    loop {
        let new_ptr = Box::into_raw(Box::new(/* move new? can't, need it for retry */));
        // Problem: Box::into_raw consumes `new`. Can't reuse.
    }
}
```

This doesn't work because `Box::into_raw(Box::new(new))` consumes
`new`. To retry, we'd need to clone it back out of the Box on failure,
which is the same as cloning before the Box.

**Fundamental constraint**: `Box::into_raw` consumes the value. To
retry, we need either:
- Clone before each attempt (current approach).
- Clone once, and on failure, take the value back out of the Box before
  it's dropped, then re-Box on the next attempt. This is possible but
  adds unsafe code complexity for zero benefit with `Bytes` payloads.

## Conclusion

### Can we eliminate the clone?

**No, not without moving it elsewhere.** The clone is fundamental:
the slot node must own a `PxLogEntry`, and the caller cannot move it
because the entry is borrowed (`&PxLogEntry`) and needed after the
accept call.

### Can we reduce the number of clones in the retry loop?

**Theoretically yes, but not worth it.** We could restructure
`cas_accepted` to avoid cloning on failed CAS attempts, but the
complexity (unsafe pointer juggling) provides zero practical benefit
with `Bytes` payloads (O(1) ref-count ops).

### Recommendation

**Do not implement.** The current design is optimal:
- One clone per successful accept (unavoidable — slot node must own).
- O(1) cost with `Bytes` payloads (ref-count bump).
- Clone on CAS failure is also O(1) and rare (only under concurrent
  writers to the same slot, which is itself rare).
- Any restructuring would either move the clone elsewhere (no net
  savings) or add unsafe complexity for no measurable benefit.

The R15 work already achieved the goal: zero redundant clones in the
accept path, with the single unavoidable clone isolated to
`cas_accepted`.

## Payload Copy Audit

The real concern is not ref-count bumps (O(1)) but O(n) heap allocate +
memcpy of payload data. Below is a full audit of every point in the
accept → learn → WAL → engine flow where payload bytes are touched.

### Accept path (entry.payload: Bytes)

- **`inner_accept` clone** (`acceptor.rs:124`): `entry.clone()` →
  `Bytes::clone` = ref-count bump. **No payload copy.**
- **`send_accept`** (`remote_replica.rs:187`): `entry.payload.clone()` →
  ref-count bump for protobuf `AcceptRequest`. **No payload copy.**
- **gRPC deserialization** (`px_service.rs:509`): `payload: value.payload`
  — move from protobuf message into `PxLogEntry`. **No payload copy.**
- **`learn_chosen`** (`local_replica.rs:1153`): `entry.clone()` →
  ref-count bump, then `apply_entry(slot, payload.as_ref())` passes
  `&[u8]`. **No payload copy.**

### WAL persist path

- **`WALRecord::from_accepted`** (`record.rs:408`): calls
  `encode_accepted_payload(entry)` which does
  `entry.payload.to_vec()` (`record.rs:487`).
  **O(n) heap allocate + memcpy.** This is a real payload copy.
- **`WALRecord.encode_frame`** (`record.rs:199`):
  `payload: self.payload.clone()` → ref-count bump for the
  `RecordFrame`. **No payload copy.**
- **Vectored write** (`record.rs:152`): `IoSlice::new(&self.payload)`
  borrows the frame's `Bytes`. **No payload copy.**
- **`segment::append`** (`segment.rs:144`): `record.encode()` →
  `Vec::with_capacity` + `extend_from_slice` for all frame parts
  including payload. **O(n) heap allocate + memcpy.** But this path
  is only used for `TextLine` format; the binary pipeline uses
  `encode_frame` + vectored write (zero-copy).

### WAL replay path

- **`WALRecord::to_log_entry`** (`record.rs:451`): calls
  `decode_accepted_payload(rec)` which does
  `Bytes::copy_from_slice(&rec.payload[..])` (`record.rs:491`).
  **O(n) heap allocate + memcpy.** This is a real payload copy.
  (Unavoidable — the WAL record's `Bytes` is a different allocation
  from the original; replay must reconstruct the `PxLogEntry`.)

### Engine apply path

- **`Batch::decode`** (`op.rs:54`): `payload.get(..).to_vec()` for
  each key and value (`op.rs:70,75`).
  **O(n) heap allocate + memcpy per key/value.** This is a real
  payload copy — the batch ops own their key/value as `Vec<u8>`.
- **`encode_batch`** (`crowtree/ffi/src/lib.rs:415`): packs ops into
  a `Vec<u8>` for the C++ FFI `ct_apply_batch`.
  **O(n) heap allocate + memcpy.** This is a real payload copy —
  the C++ engine needs a contiguous packed buffer.

### Summary

Points with O(n) payload allocate+memcpy:

- `encode_accepted_payload` — `entry.payload.to_vec()` in WAL encode.
- `decode_accepted_payload` — `Bytes::copy_from_slice` in WAL replay.
- `Batch::decode` — `to_vec()` per key/value in engine apply.
- `encode_batch` — pack into `Vec<u8>` for FFI.

Points that are O(1) ref-count bumps only:

- `inner_accept` clone (the R15 residual clone).
- `send_accept` payload clone for protobuf.
- `learn_chosen` entry clone.
- `encode_frame` payload clone for vectored write.

### Verdict

**The accept path itself has zero O(n) payload copies.** All clones in
the accept path are `Bytes` ref-count bumps. The O(n) copies are in the
WAL encode/replay and engine apply paths — these are structurally
unavoidable (WAL needs a contiguous byte buffer for disk I/O; the C++
engine needs a packed FFI buffer). The `Batch::decode` copies could
theoretically be eliminated by having `Batch` borrow from the `Bytes`
payload instead of owning `Vec<u8>`, but that would require a lifetime
parameter on `Batch` and ripple through the engine trait — a significant
refactor for marginal benefit.
