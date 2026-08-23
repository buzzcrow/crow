// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `StripPlacement` (value + methods) + `StripWriter` enum +
//! `StripResult`.
//!
//! `StripPlacement` is a strip's placement — which chunk, index
//! within chunk, EC segments, unit size. Gains methods to kill the
//! repeated disk_id/offset extraction. `StripWriter` is a Rust enum
//! (Ec/Mirror variants). `StripResult` is the response returned to
//! `ChunkWriter` on strip completion.

use bytes::Bytes;
use crow_diskio_client::DiskId;
use crow_protocol::common::ChunkId;
use crow_protocol::diskdb::rpc::Segment;
use tokio::task::JoinHandle;

use crate::{IoError, Result};

/// A strip's placement: which chunk it belongs to, its index within
/// that chunk, and the disk segments for its EC blocks (data_num +
/// code_num segments, in order).
#[derive(Debug, Clone)]
pub struct StripPlacement {
    pub chunk_id: ChunkId,
    pub strip_index_in_chunk: u32,
    pub segments: Vec<Segment>,
    pub unit_kb: u32,
}

impl StripPlacement {
    /// Unit size in bytes = unit_kb * 1024.
    pub fn unit_bytes(&self) -> u64 {
        u64::from(self.unit_kb) * 1024
    }

    /// Get segment `i` (bounds-checked).
    pub fn segment(&self, i: usize) -> Result<&Segment> {
        self.segments
            .get(i)
            .ok_or_else(|| IoError::Internal(format!("segment {i} missing")))
    }

    /// Get the disk_id for segment `i`.
    pub fn disk_id(&self, i: usize) -> Result<DiskId> {
        let seg = self.segment(i)?;
        let did = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::Internal(format!("segment {i} missing disk_id")))?;
        Ok(DiskId::new(did.high, did.low))
    }

    /// Get the byte zone offset for segment `i` = unit_offset * unit_bytes.
    pub fn zone_offset(&self, i: usize) -> Result<u64> {
        let seg = self.segment(i)?;
        Ok(seg.unit_offset * self.unit_bytes())
    }

    /// Get the zone_index for segment `i`.
    pub fn zone_index(&self, i: usize) -> Result<u32> {
        Ok(self.segment(i)?.zone_index)
    }
}

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
    /// Parity task handles — joined by `ChunkWriter` at rotation or
    /// completion.
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
}
