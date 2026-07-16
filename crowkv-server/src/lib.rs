// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crowkv-server` library — exposes the binary's modules so integration
//! tests under `tests/` can exercise CLI parsing, the management router,
//! and the registry without spawning a process.
//!
//! The binary entry (`main.rs`) imports from this lib via
//! `use crowkv_server::{cli, mgmt_api, startup, store_registry};`.

pub mod cli;
pub mod mgmt_api;
pub mod operation_registry;
pub mod startup;
pub mod store_registry;
