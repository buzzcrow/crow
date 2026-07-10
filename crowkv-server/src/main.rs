//! `crowkv-server` — `CrowKV` daemon entry point.
//!
//! Supports three deployment modes:
//! - Single group (single node or replicated group).
//! - Multi-group on a single node (shared process, separate consensus groups).
//! - Full cluster across different nodes (or containers).
//!
//! Real content lands in P4 (G4 milestone): wires the gRPC server
//! from `crowkv::rpc`, the WAL from `crowkv::wal`, and the engine
//! from `crowkv::engine` into a runnable process. See `doc/plan/plan-rpc.md`.

fn main() {
    eprintln!("crowkv-server: not yet implemented (lands in P4)");
    std::process::exit(1);
}
