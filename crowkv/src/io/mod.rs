// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowKV` async I/O facade.
//!
//! All local disk operations route through [`AsyncFile`]. Backend selection
//! happens once at startup via [`IoBackend::detect`]:
//! - **Fallback** — `tokio::fs` + `spawn_blocking` for fdatasync. Works everywhere.
//! - **`SimDisk`** — in-memory, deterministic, failure-injectable (test-util only).
//! - **`io_uring`** — deferred to V2; capability probe stub always selects fallback.
//!
//! Callers never branch on backend. The WAL, engine, and snapshot layers all
//! go through `IoBackend::open` → `AsyncFile`.

pub mod backend;
mod fallback;
#[cfg(feature = "test-util")]
pub mod sim;

use std::io;

pub use backend::{IoBackend, OpenOptions};
#[cfg(feature = "test-util")]
pub use sim::{SimDisk, SimDiskController};

/// Backend-agnostic async file handle.
///
/// Wraps the chosen backend's file type and dispatches all operations.
pub struct AsyncFile {
    pub(crate) inner: AsyncFileInner,
}

pub(crate) enum AsyncFileInner {
    Fallback(fallback::FallbackFile),
    #[cfg(feature = "test-util")]
    Sim(sim::SimFile),
}

impl AsyncFile {
    /// Write `data` at byte `offset`. Returns bytes written.
    ///
    /// # Errors
    /// Returns IO error if the write fails.
    pub async fn write_at(&mut self, data: &[u8], offset: u64) -> io::Result<usize> {
        match &mut self.inner {
            AsyncFileInner::Fallback(f) => f.write_at(data, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.write_at(data, offset),
        }
    }

    /// Read into `buf` starting at byte `offset`. Returns bytes read.
    ///
    /// # Errors
    /// Returns IO error if the read fails.
    pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        match &mut self.inner {
            AsyncFileInner::Fallback(f) => f.read_at(buf, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.read_at(buf, offset),
        }
    }

    /// Read exactly `buf.len()` bytes at `offset`, or return `UnexpectedEof`.
    ///
    /// # Errors
    /// Returns IO error if the read fails or returns unexpected EOF.
    pub async fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        match &mut self.inner {
            AsyncFileInner::Fallback(f) => f.read_exact_at(buf, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.read_exact_at(buf, offset),
        }
    }

    /// Flush file data to disk (fdatasync).
    ///
    /// # Errors
    /// Returns IO error if the sync fails.
    pub async fn fdatasync(&self) -> io::Result<()> {
        match &self.inner {
            AsyncFileInner::Fallback(f) => f.fdatasync().await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.fdatasync(),
        }
    }

    /// Flush file data + metadata to disk (fsync).
    ///
    /// # Errors
    /// Returns IO error if the sync fails.
    pub async fn fsync(&self) -> io::Result<()> {
        match &self.inner {
            AsyncFileInner::Fallback(f) => f.fsync().await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.fsync(),
        }
    }

    /// Current file size in bytes.
    ///
    /// # Errors
    /// Returns IO error if the file size cannot be determined.
    pub async fn len(&mut self) -> io::Result<u64> {
        match &mut self.inner {
            AsyncFileInner::Fallback(f) => f.len().await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.len(),
        }
    }

    /// Truncate file to `len` bytes.
    ///
    /// # Errors
    /// Returns IO error if truncation fails.
    pub async fn truncate(&self, len: u64) -> io::Result<()> {
        match &self.inner {
            AsyncFileInner::Fallback(f) => f.truncate(len).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Sim(f) => f.truncate(len),
        }
    }
}
