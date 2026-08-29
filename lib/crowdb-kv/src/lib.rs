// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowDB` core library.
//!
//! All business logic lives here as modules. Binaries (`crowdb-kv-server`)
//! are thin wrappers that wire configuration and CLI parsing.

#![allow(clippy::mod_module_files)]

pub mod cluster;
pub mod common;
pub mod kv;
pub mod metrics;
pub mod paxos;
pub mod rpc;
pub mod wal;
