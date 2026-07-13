// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Backend-agnostic async WAL file handle.
//!
//! `WalFile` wraps the chosen backend's file type (`FileBackendFile` or
//! `BlockSegment`) and dispatches all operations through a uniform async API.
//! This is the single I/O abstraction used by the segment and pipeline layers,
//! so they are unaware of whether the underlying storage is a real file or a
//! simulated block device.

use std::io;

#[cfg(feature = "test-util")]
use super::block_backend;
use super::file_backend;

/// Backend-agnostic async WAL file handle.
///
/// Wraps the chosen backend's file type and dispatches all operations.
pub struct WalFile {
    pub(crate) inner: WalFileInner,
}

pub(crate) enum WalFileInner {
    File(file_backend::FileBackendFile),
    #[cfg(feature = "test-util")]
    Block(block_backend::BlockSegment),
}

impl WalFile {
    /// Write `data` at byte `offset`. Returns bytes written.
    ///
    /// # Errors
    /// Returns IO error if the write fails.
    pub async fn write_at(&mut self, data: &[u8], offset: u64) -> io::Result<usize> {
        match &mut self.inner {
            WalFileInner::File(f) => f.write_at(data, offset).await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.write_at(data, offset),
        }
    }

    /// Write multiple non-contiguous buffers at byte `offset` in a single
    /// vectored system call. Returns the total number of bytes written.
    ///
    /// # Errors
    /// Returns an IO error if the write fails or if fewer than the total bytes
    /// could be written.
    pub async fn write_vectored_at(
        &mut self,
        bufs: &[std::io::IoSlice<'_>],
        offset: u64,
    ) -> io::Result<usize> {
        match &mut self.inner {
            WalFileInner::File(f) => f.write_vectored_at(bufs, offset).await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.write_vectored_at(bufs, offset),
        }
    }

    /// Read into `buf` starting at byte `offset`. Returns bytes read.
    ///
    /// # Errors
    /// Returns IO error if the read fails.
    pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        match &mut self.inner {
            WalFileInner::File(f) => f.read_at(buf, offset).await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.read_at(buf, offset),
        }
    }

    /// Read exactly `buf.len()` bytes at `offset`, or return `UnexpectedEof`.
    ///
    /// # Errors
    /// Returns IO error if the read fails or returns unexpected EOF.
    pub async fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        match &mut self.inner {
            WalFileInner::File(f) => f.read_exact_at(buf, offset).await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.read_exact_at(buf, offset),
        }
    }

    /// Flush file data to durable storage.
    ///
    /// # Errors
    /// Returns IO error if the sync fails.
    pub async fn fdatasync(&self) -> io::Result<()> {
        match &self.inner {
            WalFileInner::File(f) => f.fdatasync().await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.fdatasync(),
        }
    }

    /// Flush file data + metadata to durable storage.
    ///
    /// # Errors
    /// Returns IO error if the sync fails.
    pub async fn fsync(&self) -> io::Result<()> {
        match &self.inner {
            WalFileInner::File(f) => f.fsync().await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.fsync(),
        }
    }

    /// Current file size in bytes.
    ///
    /// # Errors
    /// Returns IO error if the file size cannot be determined.
    pub async fn len(&mut self) -> io::Result<u64> {
        match &mut self.inner {
            WalFileInner::File(f) => f.len().await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.len(),
        }
    }

    /// Truncate file to `len` bytes.
    ///
    /// # Errors
    /// Returns IO error if truncation fails.
    pub async fn truncate(&self, len: u64) -> io::Result<()> {
        match &self.inner {
            WalFileInner::File(f) => f.truncate(len).await,
            #[cfg(feature = "test-util")]
            WalFileInner::Block(f) => f.truncate(len),
        }
    }
}
