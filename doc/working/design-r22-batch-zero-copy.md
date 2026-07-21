<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R22 Design: Zero-copy Batch decode

## Problem

`Batch::decode` (`op.rs:54`) decodes the Paxos payload into
`Vec<BatchOp>` where each `BatchOp` owns `key: Vec<u8>` and
`Op::Put(Vec<u8>)`. Each `to_vec()` is an O(n) heap allocate + memcpy.
For a batch with K keys and total payload size N, this is K+1
allocations and N bytes of memcpy — on every `learn_chosen` call,
i.e. on every write.

The `PxLogEntry.payload` is already `Bytes` (ref-counted, shared
across the accept path via R15). `Batch::decode` could use
`Bytes::slice(range)` to create zero-copy views into the same
allocation, eliminating all `to_vec()` calls.

## Current behavior

```rust
pub enum Op {
    Put(Vec<u8>),
    Delete,
}

pub struct BatchOp {
    pub key: Vec<u8>,
    pub op: Op,
}

impl Batch {
    pub fn decode(payload: &[u8]) -> Self {
        // ...
        let key = payload.get(offset..offset + key_len).unwrap_or(&[]).to_vec();
        let value = payload.get(offset..offset + value_len).unwrap_or(&[]).to_vec();
        // ...
    }
}
```

Call site in `learner.rs:327-328`:
```rust
async fn apply_entry(&self, slot: SlotIndex, payload: &[u8]) {
    let batch = Batch::decode(payload);
```

Called from `learner.rs:346`:
```rust
self.apply_entry(entry.slot, entry.payload.as_ref()).await;
```

## Proposed approach

Change `Op` and `BatchOp` from `Vec<u8>` to `Bytes`:

```rust
pub enum Op {
    Put(Bytes),
    Delete,
}

pub struct BatchOp {
    pub key: Bytes,
    pub op: Op,
}
```

Change `Batch::decode` to accept `&Bytes` and use `slice`:

```rust
impl Batch {
    pub fn decode(payload: &Bytes) -> Self {
        // ...
        let key = payload.slice(offset..offset + key_len);
        let value = payload.slice(offset..offset + value_len);
        // ...
    }
}
```

Change `apply_entry` to accept `&Bytes`:
```rust
async fn apply_entry(&self, slot: SlotIndex, payload: &Bytes) {
    let batch = Batch::decode(payload);
```

Call site becomes:
```rust
self.apply_entry(entry.slot, &entry.payload).await;
```

Engine apply paths use `.as_ref()` to get `&[u8]` for FFI:
```rust
Op::Put(v) => CtBatchOp::Put { key: b.key.as_ref(), value: v.as_ref() },
```

## What does NOT change

- `KVEngine` trait — still `fn apply(&self, slot: u64, batch: &Batch)`.
  `Batch` is still an owned struct, just with `Bytes` fields instead of
  `Vec<u8>`.
- `Cell` — stays `Vec<u8>`. Engine-internal storage, separate concern.
- `EngineDiff` — stays `Vec<u8>`. Comparison type, separate concern.
- `encode_kv_payload` / `encode_kv_batch_items` — still produce
  `Vec<u8>`, which is converted to `Bytes` at `propose` entry. No
  change needed.
- WAL encode/decode — operates on `WALRecord.payload: Bytes`, not
  `Batch`. No change needed.

## Alternatives considered

- **Lifetime parameter on `Batch`** (`Batch<'a>`): would ripple through
  `KVEngine` trait, all engine impls, and all callers. High complexity,
  no benefit over `Bytes` (which is owned + zero-copy).
- **Borrowed slices (`&[u8]`)**: same lifetime problem. `Bytes` is the
  standard solution for owned + zero-copy in Rust.

## Acceptance test plan

- All existing `op_codec_test` tests pass (round-trip, truncation,
  empty payload, multi-op, large keys/values, boundary conditions).
- All existing `mem_kv_test` tests pass (highest-slot-wins, tombstone,
  scan, live_key_count, snapshot import/export).
- All existing `replay_tests` pass (WAL replay with Batch decode).
- All existing conformance tests pass for both InMemKV and
  CrowtreeEngine.
- `cargo clippy -- -D warnings` passes.
- `cargo fmt --check` passes.
- Code inspection confirms no `to_vec()` in `Batch::decode`.
