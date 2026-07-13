//! Storage-engine integration tests.
//!
//! Entrypoint for `kv/` submodules covering the `KVEngine` trait surface,
//! shared across the `InMemKV` and `CrowtreeEngine` implementations (apply
//! idempotency, per-key highest-slot-wins, tombstones, prefix scan,
//! `compare`), plus cross-engine parity.

#[path = "kv/conformance.rs"]
mod conformance;

#[path = "kv/mem_kv_test.rs"]
mod mem_kv;

#[path = "kv/crowtree_engine_test.rs"]
mod crowtree_engine;

#[path = "kv/op_codec_test.rs"]
mod op_codec;

#[path = "kv/kv_future_test.rs"]
mod kv_future;
