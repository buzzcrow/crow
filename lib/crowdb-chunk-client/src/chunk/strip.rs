// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `StripWriter` enum + `StripResult`.
//!
//! `StripWriter` is a Rust enum (Ec/Mirror variants). `StripResult`
//! is the response returned to `ChunkWriter` on strip completion.
//! `StripPlacement` is gone — `EcStripWriter` holds `Arc<Chunk>` +
//! index and reads segments directly from the protobuf.

use bytes::Bytes;
use crowdb_protocol::common::ChunkId;
use tokio::task::JoinHandle;

use crate::Result;

/// Result of finishing a strip — returned by `StripWriter::finish` to
/// `ChunkWriter`.
#[derive(Debug)]
pub struct StripResult {
    pub chunk_id: ChunkId,
    pub strip_index_in_chunk: u32,
    pub data_blocks_written: u32,
    pub bytes_written: u64,
    /// True if the last block was < unit_bytes (partial strip at EOF).
    pub partial: bool,
    /// Parity task handles — joined by `ChunkWriter` at seal time
    /// (decoupled from strip finish in Phase 3.1).
    pub parity_handles: Vec<JoinHandle<Result<()>>>,
}

/// Strip writer enum — Rust enum (not trait object) for monomorphic
/// dispatch. `Ec` variant is used by the large-write flow; `Mirror`
/// is a placeholder stub.
pub enum StripWriter {
    Ec(crate::chunk::ec_strip_writer::EcStripWriter),
    Mirror(crate::chunk::mirror_strip_writer::MirrorStripWriter),
}

// Re-export FeedStatus from the public io module.
use crate::io::FeedStatus;

impl StripWriter {
    /// Push a data block to the strip.
    pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        match self {
            Self::Ec(w) => w.push(buffer).await,
            Self::Mirror(w) => w.push(buffer).await,
        }
    }

    /// End of strip: write parity (EC), fsync, return the strip result.
    pub async fn finish(&mut self) -> Result<StripResult> {
        match self {
            Self::Ec(w) => w.finish().await,
            Self::Mirror(w) => w.finish().await,
        }
    }

    /// Abort: drop in-flight writes, return already-durable state.
    pub async fn abort(&mut self) -> Result<StripResult> {
        match self {
            Self::Ec(w) => w.abort(),
            Self::Mirror(w) => w.abort().await,
        }
    }

    /// Non-async capacity hint.
    pub fn ready(&self) -> bool {
        match self {
            Self::Ec(w) => w.ready(),
            Self::Mirror(w) => w.ready(),
        }
    }

    /// True if the strip has any data blocks written.
    pub fn has_data(&self) -> bool {
        match self {
            Self::Ec(w) => w.data_blocks_written() > 0,
            Self::Mirror(w) => w.has_data(),
        }
    }
}
