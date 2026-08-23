// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Error types for the chunk data path.

use thiserror::Error;

/// Error type for chunk IO operations.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("chunk allocation failed: {0}")]
    AllocationFailed(String),
    #[error("disk write failed: {0}")]
    WriteFailed(String),
    #[error("EC encode failed: {0}")]
    EcEncodeFailed(String),
    #[error("memory budget exhausted")]
    MemoryBudgetExhausted,
    #[error("writer already finished")]
    Finished,
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, IoError>;

impl From<crow_chunkdb_client::ChunkdbClientError> for IoError {
    fn from(e: crow_chunkdb_client::ChunkdbClientError) -> Self {
        Self::AllocationFailed(e.to_string())
    }
}

impl From<crow_diskio_client::DiskioError> for IoError {
    fn from(e: crow_diskio_client::DiskioError) -> Self {
        Self::WriteFailed(e.to_string())
    }
}

impl From<crow_common::ec::EcError> for IoError {
    fn from(e: crow_common::ec::EcError) -> Self {
        Self::EcEncodeFailed(e.to_string())
    }
}
