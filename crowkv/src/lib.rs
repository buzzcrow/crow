// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowKV` core library.
//!
//! All business logic lives here as modules. Binaries (`crowkv-server`)
//! are thin wrappers that wire configuration and CLI parsing.

pub mod cluster;
pub mod common;
pub mod kv;
pub mod metrics;
pub mod paxos;
pub mod rpc;
pub mod wal;
