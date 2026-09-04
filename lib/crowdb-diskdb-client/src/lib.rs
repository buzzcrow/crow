// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROWDB diskdb.
//!
//! Provides allocate/free/query APIs with retry and endpoint caching,
//! mirroring `crowdb-kv-client`'s pattern. The client discovers diskdb
//! instances via the service registry (group 0), caches
//! `disk_group_id -> rpc_endpoint`, and lazily refreshes on cache
//! miss or `Unavailable`.

pub mod client;
pub mod rpc_transport;

pub use client::{normalize_endpoint, DiskdbClient, RetryConfig};
pub use rpc_transport::DiskdbRpcTransport;

use thiserror::Error;

/// Error type for diskdb client operations.
#[derive(Debug, Error)]
pub enum DiskdbClientError {
    #[error("diskdb has no space: {0}")]
    NoSpace(String),
    #[error("diskdb server unreachable: {0}")]
    Unreachable(String),
    #[error("diskdb server does not own the requested resource: {0}")]
    NotOwner(String),
    #[error("diskdb RPC error: {0}")]
    Rpc(String),
}

pub type Result<T> = std::result::Result<T, DiskdbClientError>;
