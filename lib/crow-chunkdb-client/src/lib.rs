// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROW chunkdb.
//!
//! Provides allocate/append/seal/delete/query APIs with endpoint
//! caching, mirroring `crow-diskdb-client`'s pattern. The client
//! discovers chunkdb instances via the service registry (group 0),
//! caches `instance_id -> rpc_endpoint`, and lazily refreshes on
//! cache miss. Retry on transient errors with exponential backoff.

#![allow(clippy::must_use_candidate, clippy::missing_errors_doc)]

pub mod client;
pub mod rpc_transport;

pub use client::{ChunkdbClient, RetryConfig};
pub use rpc_transport::ChunkdbRpcTransport;

// Re-export RangeBindingClient so callers can construct it without a
// direct crow-kv-client dependency for this one type.
pub use crow_kv_client::RangeBindingClient;

use thiserror::Error;

/// Error type for chunkdb client operations.
#[derive(Debug, Error)]
pub enum ChunkdbClientError {
    #[error("chunkdb server unreachable: {0}")]
    Unreachable(String),
    #[error("chunkdb server unavailable (transient): {0}")]
    Unavailable(String),
    #[error("chunk not found: {0}")]
    NotFound(String),
    #[error("chunk already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid state transition: {0}")]
    FailedPrecondition(String),
    #[error("state conflict (concurrent modification): {0}")]
    Aborted(String),
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("chunk bucket not in owned ranges: {0}")]
    NotMyRange(String),
    #[error("RPC error: {0}")]
    Rpc(String),
}

impl ChunkdbClientError {
    /// Check if this error is transient (worth retrying).
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_) | Self::DeadlineExceeded(_) | Self::Unreachable(_) | Self::NotMyRange(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, ChunkdbClientError>;
