// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` — distributed disk-block allocator.
//!
//! Lightweight, stateless server that owns disk-groups, allocates
//! disk blocks, and persists all state to CROW KV (group 0 for
//! sysdata, paxos data groups for zone journals). No local WAL.
//!
//! This is the skeleton — functionality is filled in by follow-up
//! requirements (R70–R77). See `doc/design/diskdb/design-crow-diskdb.md`.

pub mod config;
pub mod types;

pub use config::DiskdbConfig;
