//! `CrowKV` core library.
//!
//! All business logic lives here as modules. Binaries (`crowkv-server`, `crowkv-bench`)
//! are thin wrappers that wire configuration and CLI parsing.

pub mod cluster;
pub mod common;
pub mod engine;
pub mod io;
pub mod paxos;
pub mod reconfig;
pub mod rpc;
pub mod wal;
