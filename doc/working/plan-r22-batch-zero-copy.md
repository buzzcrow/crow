<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R22 Plan: Zero-copy Batch decode

## Tasks

- [ ] 1. Change `Op`, `BatchOp` from `Vec<u8>` to `Bytes` in `op.rs`
- [ ] 2. Rewrite `Batch::decode` to use `Bytes::slice` instead of `to_vec()`
- [ ] 3. Change `apply_entry` in `learner.rs` to accept `&Bytes`
- [ ] 4. Update `CrowtreeEngine::apply` to use `.as_ref()` for FFI mapping
- [ ] 5. Update `InMemKV::apply` in `mem_kv_impl.rs` (test engine)
- [ ] 6. Update test helpers in `conformance.rs` (`put`/`del`)
- [ ] 7. Update test helpers in `op_codec_test.rs` (`put`/`del`)
- [ ] 8. Update `Batch::decode` call sites in `mem_kv_test.rs` and `replay_tests.rs`
- [ ] 9. Run `cargo fmt --check` + `cargo clippy -- -D warnings`
- [ ] 10. Run relevant tests: `op_codec_test`, `mem_kv_test`, `replay_tests`, conformance

## Files

- `crowkv/src/kv/op.rs` — `Op`, `BatchOp`, `Batch::decode`
- `crowkv/src/paxos/learner.rs` — `apply_entry` signature + call site
- `crowkv/src/kv/crowtree_engine.rs` — `apply` FFI mapping
- `crowkv/tests/kv/mem_kv_impl.rs` — `InMemKV::apply`
- `crowkv/tests/kv/conformance.rs` — `put`/`del` helpers
- `crowkv/tests/kv/op_codec_test.rs` — `put`/`del` helpers + assertions
- `crowkv/tests/kv/mem_kv_test.rs` — `Batch::decode` call sites
- `crowkv/tests/wal/replay_tests.rs` — `Batch::decode` call sites

## Test checklist

- [ ] op_codec_test: all decode round-trip + truncation tests
- [ ] mem_kv_test: highest-slot-wins, tombstone, scan, live_key_count
- [ ] replay_tests: WAL replay with Batch decode
- [ ] conformance: InMemKV + CrowtreeEngine apply
