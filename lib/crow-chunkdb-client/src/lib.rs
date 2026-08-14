// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROW chunkdb.
//!
//! Provides allocate/append/seal/delete/query APIs with endpoint
//! caching, mirroring `crow-diskdb-client`'s pattern. The client
//! discovers chunkdb instances via the service registry (group 0),
//! caches `instance_id -> grpc_endpoint`, and lazily refreshes on
//! cache miss. Retry logic lands in R90.

pub mod client;

pub use client::ChunkdbClient;

use thiserror::Error;

/// Error type for chunkdb client operations.
#[derive(Debug, Error)]
pub enum ChunkdbClientError {
    #[error("chunkdb server unreachable: {0}")]
    Unreachable(String),
    #[error("chunkdb RPC error: {0}")]
    Rpc(String),
}

pub type Result<T> = std::result::Result<T, ChunkdbClientError>;
