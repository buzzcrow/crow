<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R25 Design: Eliminate client-side batch copy via `Bytes`

## Problem

`CrowkvClient::batch_write` (`crowkv-client/src/client.rs:440`) has two
avoidable O(n) copies:

1. **BatchOp → KvBatchItem clone** (`client.rs:442-456`): The client
   `BatchOp` owns `Vec<u8>` for key and value. Mapping to the protobuf
   `KvBatchItem` calls `key.clone()` and `value.clone()` per op —
   O(n) heap allocate + memcpy of every key and value in the batch.
2. **items.clone() per retry** (`client.rs:463`): The retry loop
   clones the entire `Vec<KvBatchItem>` on every attempt — O(n) copy
   of all keys and values per retry.

The same pattern affects single-key `put` (`key.to_vec()`,
`value.to_vec()`) and `delete` (`key.to_vec()`), though those are
one-key copies, not batch-scaled.

## Root cause

`prost-build` in `build.rs:12` maps only `AcceptedValue.payload` to
`bytes::Bytes`. All other `bytes` proto fields (KV `key`, `value`,
`prefix`, `start_after`, etc.) use the default `Vec<u8>` mapping.
The client `BatchOp` (`client.rs:51-53`) also uses `Vec<u8>`. Every
clone of a `Vec<u8>` is an O(n) heap allocate + memcpy; every clone
of a `Bytes` is an O(1) atomic ref-count bump.

## Proposed approach

Extend `prost-build` `.bytes([...])` in `crowkv/build.rs` to map all
KV `bytes` fields to `bytes::Bytes`:

- `KvSetRequest.key`, `KvSetRequest.value`
- `KvGetRequest.key`
- `KvDeleteRequest.key`
- `KvBatchItem.key`, `KvBatchItem.value`
- `KvResponse.value`
- `KvScanRequest.prefix`, `KvScanRequest.start_after`
- `KvScanItem.key`, `KvScanItem.value`

Change client `BatchOp` from `Vec<u8>` to `bytes::Bytes`.

Update `batch_write` to construct `KvBatchItem` with `Bytes::clone`
(O(1)) instead of `Vec<u8>::clone` (O(n)). The retry loop's
`items.clone()` also becomes O(1) per item.

Update all call sites that construct or destructure the affected
proto types: `kv_response.rs` constructors, `px_kv_store.rs`
(`encode_kv_batch_items`, `KvScanItem` construction), client
`put`/`delete`/`get`/`scan`, and all test files.

The server-side `encode_kv_batch_items` still flattens into a
`Vec<u8>` consensus payload — that copy remains (eliminating it
requires changing the consensus payload contract, out of scope).

## Alternatives considered

- **Borrowed slices in client API** (`&[(&[u8], Option<&[u8]>)]`):
  eliminates copies but changes the public API ergonomics and
  complicates retry (borrowed data must outlive the retry loop).
  `Bytes` achieves the same zero-copy-with-ergonomics via ref-counting.
- **Server-side payload encoding elimination**: skip
  `encode_kv_batch_items` and pass `KvBatchItem` directly as the
  Paxos payload. Requires changing `Batch::decode` to match the
  protobuf wire format. High complexity, ripples through consensus +
  WAL + replay. Separate requirement.

## Acceptance criteria

- All existing client and server tests pass unchanged (test code
  updated for type changes, but test logic unchanged).
- `CrowkvClient::batch_write` does not call `Vec<u8>::clone` for
  key/value data — `items.clone()` in the retry loop is O(1) per
  item (ref-count bump).
- `pixi run cargo clippy -- -D warnings` passes.
- `pixi run cargo fmt --check` passes.

## Files

- `crowkv/build.rs` — extend `.bytes([...])`
- `crowkv/src/rpc/kv_response.rs` — `Vec<u8>` → `Bytes` in constructors
- `crowkv/src/cluster/px_kv_store.rs` — `KvScanItem` construction,
  `encode_kv_batch_items` field access
- `crowkv-client/Cargo.toml` — add `bytes = "1"` dependency
- `crowkv-client/src/client.rs` — `BatchOp` type, `batch_write`,
  `put`, `delete`, `get`, `scan`
- `crowkv/tests/**` — test helpers that construct KV requests
- `crowkv-server/tests/**` — test helpers that construct KV requests
- `crowkv-client/tests/**` — test helpers that construct KV requests
