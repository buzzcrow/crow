// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! crowdb-rpc service module — one file per service.

pub mod diskdb_rpc_service;
pub mod diskdb_service;
mod mutation_gate;

pub use diskdb_rpc_service::DiskdbRpcService;
