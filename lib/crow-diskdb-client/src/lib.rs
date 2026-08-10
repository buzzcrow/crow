// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROW diskdb.
//!
//! Provides allocate/free/query APIs with retry and topology caching,
//! mirroring `crow-kv-client`'s pattern. Skeleton — functionality
//! filled in by follow-up requirements.

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
