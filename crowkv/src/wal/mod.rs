// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowKV` write-ahead log.
//!
//! Multi-disk segmented WAL with batched durable flush, ack contract, replay, and GC.
//! All WAL storage I/O goes through `IoBackend` / `WalFile`.
//!
//! ## Modules
//!
//! - [`record`] — `WALRecord` codec (**FROZEN** byte layout, version 1).
//! - [`segment`] — Segment file: header, record append, seal/footer, reader.
//! - [`index`] — `SegmentIndex`: slot → (disk, `segment_id`, offset).
//! - [`io_backend`] — WAL backend selection and file/block façade.
//! - [`wal_file`] — `WalFile`: backend-agnostic WAL file handle.
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
pub use wal_file::WalFile;
pub(crate) use wal_file::WalFileInner;

mod wal_file;
