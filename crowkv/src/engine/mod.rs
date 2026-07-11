//! `CrowKV` storage engine.
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
//! Key work: `Engine` trait, in-memory implementation, payload `Batch` decode,
//! cross-learner `compare`. Ordered-file / crowtree engines and streamable
//! snapshot import/export land in later phases.

mod mem_engine;
mod op;
mod store_engine;

pub use mem_engine::InMemoryEngine;
pub use op::{Batch, BatchOp, Cell, EngineDiff, Op};
pub use store_engine::Engine;
