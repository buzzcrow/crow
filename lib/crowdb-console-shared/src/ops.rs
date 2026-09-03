// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Operations layer for the console CLI (R126).
//!
//! Each submodule wraps a domain area (hardware, KV logical, KV server,
//! KV data-plane, cluster, chunk/diskdb, bench) as free functions that
//! take an [`OpContext`] — the shared connection to group-0 sysdata +
//! the local TOML config. The CLI command handlers are thin wrappers
//! that parse args, call these functions, and render the result.

pub mod bench;
pub mod chunk;
pub mod cluster;
pub mod context;
pub mod hardware;
pub mod kv_data;
pub mod kv_logical;
pub mod kv_server;

pub use context::OpContext;
