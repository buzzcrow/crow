// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Storage-engine integration tests.
//!
//! Entrypoint for `kv/` submodules covering the `KVEngine` trait surface,
//! shared across the `InMemKV` and `CrowdbTreeEngine` implementations (apply
//! idempotency, per-key highest-slot-wins, tombstones, prefix scan,
//! `compare`), plus cross-engine parity.

#[path = "kv_test/mem_kv_impl_test.rs"]
mod mem_kv;

#[path = "kv_test/conformance_test.rs"]
mod conformance;

#[path = "common/test_util.rs"]
mod test_util;

#[path = "kv_test/mem_kv_test.rs"]
mod mem_kv_tests;

#[path = "kv_test/crowdb_tree_engine_test.rs"]
mod crowdb_tree_engine;

#[path = "kv_test/op_codec_test.rs"]
mod op_codec;

#[path = "kv_test/kv_future_test.rs"]
mod kv_future;
