//! Storage-engine integration tests.
//!
//! Entrypoint for `kv/` submodules covering the `KVEngine` trait surface
//! and the `InMemKV` implementation (apply idempotency, per-key
//! highest-slot-wins, tombstones, prefix scan, `compare`).

#[path = "kv/in_memory_test.rs"]
mod in_memory;
