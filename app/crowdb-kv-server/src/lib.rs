// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-kv-server` library — exposes the binary's modules so integration
//! tests under `tests/` can exercise CLI parsing, the management router,
//! and the registry without spawning a process.
//!
//! The binary entry (`main.rs`) imports from this lib via
//! `use crowdb_kv_server::{cli, mgmt, startup, store_registry};`.

pub mod binding_monitor_wiring;
pub mod cli;
pub mod engine_collector;
pub(crate) mod group_rebuild;
pub mod keepalive;
pub mod mgmt;
pub mod operation_registry;
pub mod reconcile;
pub mod restore;
pub mod startup;
pub mod store_registry;
