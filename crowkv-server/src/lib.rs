//! `crowkv-server` library — exposes the binary's modules so integration
//! tests under `tests/` can exercise CLI parsing, the management router,
//! and the registry without spawning a process.
//!
//! The binary entry (`main.rs`) imports from this lib via
//! `use crowkv_server::{cli, management, state};`.

pub mod cli;
pub mod management;
pub mod state;
