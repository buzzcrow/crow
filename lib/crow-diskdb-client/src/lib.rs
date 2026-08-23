// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROW diskdb.
//!
//! Provides allocate/free/query APIs with retry and endpoint caching,
//! mirroring `crow-kv-client`'s pattern. The client discovers diskdb
//! instances via the service registry (group 0), caches
//! `disk_group_id -> grpc_endpoint`, and lazily refreshes on cache
//! miss or `Unavailable`.

pub mod client;

pub use client::{normalize_endpoint, DiskdbClient, RetryConfig};

use thiserror::Error;

/// Error type for diskdb client operations.
#[derive(Debug, Error)]
pub enum DiskdbClientError {
    #[error("diskdb server unreachable: {0}")]
    Unreachable(String),
    #[error("diskdb RPC error: {0}")]
    Rpc(String),
}

pub type Result<T> = std::result::Result<T, DiskdbClientError>;
