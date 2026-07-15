<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Track Revision for Reads (R1)

## Tasks

- [x] Add `ok_value_with_revision` constructor to `kv_response.rs`
- [x] Update `kv_get` in `px_kv_store.rs` to pass through the per-key slot
- [x] Add unit test: put then get returns correct revision
- [x] Add unit test: overwrite then get returns updated revision
- [x] Add unit test: get missing key returns revision 0
- [x] Add unit test: get after delete returns revision 0
- [x] Run `pixi run test-core` to verify

## Files

- `crowkv/src/rpc/kv_response.rs` — new constructor
- `crowkv/src/cluster/px_kv_store.rs` — stop discarding slot from `engine_get`
- `crowkv/tests/kv.rs` — new test cases

## Test Checklist

- [x] revision == slot after put + get
- [x] revision == newer slot after overwrite + get
- [x] revision == 0 for missing key
- [x] revision == 0 for deleted key
- [x] existing kv tests still pass
