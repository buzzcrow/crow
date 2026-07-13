//! `CrowKV` core library.
//!
//! All business logic lives here as modules. Binaries (`crowkv-server`)
//! are thin wrappers that wire configuration and CLI parsing.

pub mod cluster;
pub mod common;
pub mod kv;
pub mod paxos;
pub mod rpc;
pub mod wal;
