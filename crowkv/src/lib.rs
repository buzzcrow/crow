//! `CrowKV` core library.
//!
//! All business logic lives here as modules. Binaries (`crowkv-server`, `crowkv-bench`)
//! are thin wrappers that wire configuration and CLI parsing.

pub mod engine;
pub mod group;
pub mod io;
pub mod kv;
pub mod node;
pub mod paxos;
pub mod reconfig;
pub mod rpc;
pub mod testkit;
pub mod wal;
