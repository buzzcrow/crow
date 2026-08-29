// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowDB` storage engine.
//!
//! The engine is the sole consumer of consensus output: it owns the
//! materialized key-value state and serves all reads. The WAL is the durable
//! log; the engine is the materialized projection. It knows nothing about
//! Paxos, terms, ballots, leaders, or the network — its entire write contract
//! is `apply(slot, batch)`.
//!
//! Single-version per key: every live key maps to `(resolved_slot, Cell)`
//! where `Cell` is a value or a tombstone. `apply` is atomic and idempotent —
//! an op is skipped when its `slot <= resolved_slot(key)`, which makes replays
//! and out-of-order parallel-slot applies naturally correct (highest slot
//! wins).
//!
//! Key work: `KVEngine` trait, payload `Batch` decode, cross-learner
//! `compare`. The crowdb-tree engine and streamable snapshot import/export land
//! in later phases. `InMemKV` lives in `tests/` as a test-only reference
//! implementation.

mod crowdb_tree_engine;
mod kv_engine;
mod kv_future;
mod op;

pub use crowdb_tree_engine::{CrowdbTreeBackend, CrowdbTreeEngine, CrowdbTreeOptions, CrowdbTreeStats};
pub use kv_engine::{KVEngine, SnapshotViewEntry};
pub use kv_future::KVFuture;
pub use op::{Batch, BatchOp, Cell, EngineDiff, Op};
