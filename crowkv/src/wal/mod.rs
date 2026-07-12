//! `CrowKV` write-ahead log.
//!
//! Multi-disk segmented WAL with batched durable flush, ack contract, replay, and GC.
//! All WAL storage I/O goes through `IoBackend` / `AsyncFile`.
//!
//! ## Modules
//!
//! - [`record`] — `WALRecord` codec (**FROZEN** byte layout, version 1).
//! - [`segment`] — Segment file: header, record append, seal/footer, reader.
//! - [`index`] — `SegmentIndex`: slot → (disk, `segment_id`, offset).
//! - [`io_backend`] — WAL backend selection and file/block façade.
//! - [`pipeline_backend`] — Backend model for file, memory-block, and block pipelines.
//! - [`pipeline`] — `WalPipeline`: single-disk active segment state.
//! - [`pipeline_writer`] — Per-pipeline dedicated writer task: drain, batch write, `fdatasync`, ack.
//! - [`wal_engine`] — `WalEngine`: disk selection, rotation, append API.
//! - [`replay`] — Replay engine: discover, scan, CRC-truncate, rebuild.
//! - [`gc`] — GC worker: watermark-based segment unlink.
//! - [`config`] — `WalConfig` tunables.

#[cfg(feature = "test-util")]
mod block_backend;
mod file_backend;
pub mod io_backend;

use std::io;

pub mod gc;
pub mod index;
pub mod pipeline;
pub mod pipeline_backend;
pub mod pipeline_writer;
pub mod record;
pub mod replay;
pub mod segment;
pub mod wal_engine;

pub use crate::common::config::WalConfig;
#[cfg(feature = "test-util")]
pub use block_backend::{BlockDevice, BlockDeviceController};
pub use io_backend::{IoBackend, OpenOptions};
pub use record::{RecordType, WALRecord, WalRecordFormat};
pub use wal_engine::{BatchStats, WalEngine};

/// Backend-agnostic async WAL file handle.
///
/// Wraps the chosen backend's file type and dispatches all operations.
pub struct AsyncFile {
    pub(crate) inner: AsyncFileInner,
}

pub(crate) enum AsyncFileInner {
    File(file_backend::FileBackendFile),
    #[cfg(feature = "test-util")]
    Block(block_backend::BlockSegment),
}

impl AsyncFile {
    /// Write `data` at byte `offset`. Returns bytes written.
    ///
    /// # Errors
    /// Returns IO error if the write fails.
    pub async fn write_at(&mut self, data: &[u8], offset: u64) -> io::Result<usize> {
        match &mut self.inner {
            AsyncFileInner::File(f) => f.write_at(data, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.write_at(data, offset),
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
            AsyncFileInner::File(f) => f.write_vectored_at(bufs, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.write_vectored_at(bufs, offset),
        }
    }

    /// Read into `buf` starting at byte `offset`. Returns bytes read.
    ///
    /// # Errors
    /// Returns IO error if the read fails.
    pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        match &mut self.inner {
            AsyncFileInner::File(f) => f.read_at(buf, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.read_at(buf, offset),
        }
    }

    /// Read exactly `buf.len()` bytes at `offset`, or return `UnexpectedEof`.
    ///
    /// # Errors
    /// Returns IO error if the read fails or returns unexpected EOF.
    pub async fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        match &mut self.inner {
            AsyncFileInner::File(f) => f.read_exact_at(buf, offset).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.read_exact_at(buf, offset),
        }
    }

    /// Flush file data to durable storage.
    ///
    /// # Errors
    /// Returns IO error if the sync fails.
    pub async fn fdatasync(&self) -> io::Result<()> {
        match &self.inner {
            AsyncFileInner::File(f) => f.fdatasync().await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.fdatasync(),
        }
    }

    /// Flush file data + metadata to durable storage.
    ///
    /// # Errors
    /// Returns IO error if the sync fails.
    pub async fn fsync(&self) -> io::Result<()> {
        match &self.inner {
            AsyncFileInner::File(f) => f.fsync().await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.fsync(),
        }
    }

    /// Current file size in bytes.
    ///
    /// # Errors
    /// Returns IO error if the file size cannot be determined.
    pub async fn len(&mut self) -> io::Result<u64> {
        match &mut self.inner {
            AsyncFileInner::File(f) => f.len().await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.len(),
        }
    }

    /// Truncate file to `len` bytes.
    ///
    /// # Errors
    /// Returns IO error if truncation fails.
    pub async fn truncate(&self, len: u64) -> io::Result<()> {
        match &self.inner {
            AsyncFileInner::File(f) => f.truncate(len).await,
            #[cfg(feature = "test-util")]
            AsyncFileInner::Block(f) => f.truncate(len),
        }
    }
}
