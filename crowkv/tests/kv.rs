//! Storage-engine integration tests.
//!
//! Entrypoint for `kv/` submodules covering the `KVEngine` trait surface
//! and the `InMemKV` implementation (apply idempotency, per-key
//! highest-slot-wins, tombstones, prefix scan, `compare`).

#[path = "kv/mem_kv_test.rs"]
mod mem_kv;

#[path = "kv/op_codec_test.rs"]
mod op_codec;
