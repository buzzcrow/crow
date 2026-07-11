//! Storage-engine integration tests.
//!
//! Entrypoint for `engine/` submodules covering the `Engine` trait surface
//! and the `InMemoryEngine` implementation (apply idempotency, per-key
//! highest-slot-wins, tombstones, prefix scan, `compare`).

#[path = "engine/in_memory_test.rs"]
mod in_memory;
