// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `HashWorker` — whole-object digest compute (placeholder stub).
//!
//! Separate struct from `EcWorker` — no shared `Worker` trait. Lives
//! at the object level (owned by `LargeObjectWriter` /
//! `SmallObjectWriter`), not the strip level. Different queue length
//! and capability than `EcWorker`. Filled in by R-later.

use bytes::Bytes;

use crate::{IoError, Result};

/// Whole-object digest worker (MD5/SHA-256). Placeholder — methods
/// return `IoError::Internal` until R-later fills in the impl.
pub struct HashWorker {
    _algorithm: HashAlgorithm,
}

/// Digest algorithm selector.
#[derive(Debug, Clone, Copy)]
pub enum HashAlgorithm {
    Md5,
    Sha256,
}

impl HashWorker {
    /// Construct a new worker for the given algorithm.
    #[must_use]
    pub fn new(algorithm: HashAlgorithm) -> Self {
        Self {
            _algorithm: algorithm,
        }
    }

    /// Feed one data buffer. Computes incrementally.
    pub fn push(&mut self, _buffer: &Bytes) -> Result<()> {
        Err(IoError::Internal("HashWorker not yet implemented".into()))
    }

    /// Finalize: return the digest bytes.
    pub fn finish(&mut self) -> Result<Vec<u8>> {
        Err(IoError::Internal("HashWorker not yet implemented".into()))
    }

    /// Reset to accept a new object.
    pub fn reset(&mut self) {
        // No-op until implemented.
    }
}
