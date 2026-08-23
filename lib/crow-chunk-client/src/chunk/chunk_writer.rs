// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkWriter` — chunk-info wrapper + write ability.
//!
//! Wraps the current `ChunkInfo` (protobuf `Chunk`) and provides write
//! ability for one chunk. The drive loop (`LargeObjectWriter`) controls
//! strip + chunk rotation by feeding placements; `ChunkWriter` pushes
//! blocks to the current strip and finishes strips on demand. Does NOT
//! own the drive loop. All chunkdb chunk operations (seal, delete,
//! append) go through this class.

use std::sync::Arc;

use bytes::Bytes;
use tracing::warn;

use crate::chunk::ec_strip_writer::EcStripWriter;
use crate::chunk::strip::{StripPlacement, StripResult, StripWriter};
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::traits::ChunkAllocator;
use crate::{IoError, Location, Result};
use crow_common::ec::EcScheme;
use crow_protocol::chunkdb::rpc::{DeleteChunkRequest, SealChunkRequest};
use crow_protocol::common::ChunkId;

/// Chunk-info wrapper + write ability. Does NOT own the drive loop.
pub struct ChunkWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) current_chunk_id: Option<ChunkId>,
    pub(crate) bytes_in_chunk: u64,
    pub(crate) strips_in_chunk: u32,
    pub(crate) current_strip: Option<StripWriter>,
}

impl ChunkWriter {
    /// Construct a new chunk writer (no chunk open yet).
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
    ) -> Self {
        Self {
            allocator,
            disk_writer,
            ec_scheme,
            config,
            current_chunk_id: None,
            bytes_in_chunk: 0,
            strips_in_chunk: 0,
            current_strip: None,
        }
    }

    /// Open a chunk from a pre-allocated placement. Constructs the
    /// first `EcStripWriter` for the strip.
    pub fn open(&mut self, placement: StripPlacement) -> Result<()> {
        self.current_chunk_id = Some(placement.chunk_id);
        self.bytes_in_chunk = 0;
        self.strips_in_chunk = 1;
        let strip = EcStripWriter::new(placement, self.disk_writer.clone(), self.ec_scheme);
        self.current_strip = Some(StripWriter::Ec(strip));
        Ok(())
    }

    /// Continue with a new strip on the same chunk (from a placement
    /// delivered by prefetch with the same chunk_id).
    pub fn continue_strip(&mut self, placement: StripPlacement) -> Result<()> {
        if placement.chunk_id != self.current_chunk_id.unwrap_or_default() {
            return Err(IoError::Internal("continue_strip with different chunk_id".into()));
        }
        let strip = EcStripWriter::new(placement, self.disk_writer.clone(), self.ec_scheme);
        self.current_strip = Some(StripWriter::Ec(strip));
        self.strips_in_chunk += 1;
        Ok(())
    }

    /// Push a data block to the current strip. Does NOT auto-rotate
    /// strips — the drive loop calls `finish_strip` + `continue_strip`
    /// / `open` when the strip is full.
    pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        let strip = self
            .current_strip
            .as_mut()
            .ok_or_else(|| IoError::Internal("push with no open strip".into()))?;
        strip.push(buffer).await
    }

    /// Finish the current strip. Records bytes written.
    pub async fn finish_strip(&mut self) -> Result<StripResult> {
        let mut strip = self
            .current_strip
            .take()
            .ok_or_else(|| IoError::Internal("finish_strip with no open strip".into()))?;
        let mut strip_result = strip.finish().await?;
        self.bytes_in_chunk += strip_result.bytes_written;
        // Transfer parity handles to the caller.
        strip_result.parity_handles = Vec::new(); // already joined in EcStripWriter::finish
        Ok(strip_result)
    }

    /// Is the current strip full (all data_num blocks written)?
    pub fn is_strip_full(&self) -> bool {
        match &self.current_strip {
            Some(s) => !s.ready(),
            None => true,
        }
    }

    /// Is there no current strip?
    pub fn current_strip_is_none(&self) -> bool {
        self.current_strip.is_none()
    }

    /// Append a new strip to the current chunk via `append_chunk` RPC.
    pub async fn append_strip(&mut self) -> Result<StripPlacement> {
        let chunk_id = self
            .current_chunk_id
            .ok_or_else(|| IoError::Internal("append_strip with no open chunk".into()))?;
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        crate::chunk::chunk_prefetch::append_strip(
            &*self.allocator,
            chunk_id,
            self.ec_scheme,
            self.strips_in_chunk,
            write_granularity_kb,
        )
        .await
    }

    /// Seal the chunk: seal_chunk RPC, return the chunk's Location.
    /// The current strip should already be finished.
    pub async fn seal(&mut self) -> Result<Location> {
        let chunk_id = self.current_chunk_id;
        let bytes_in_chunk = self.bytes_in_chunk;

        let location = match chunk_id {
            Some(cid) if bytes_in_chunk > 0 => {
                let unit_bytes = u64::from((self.config.read_buffer_size / 1024) as u32) * 1024;
                let sealed_length_units = (bytes_in_chunk / unit_bytes) as u32;
                self.allocator
                    .seal_chunk(SealChunkRequest {
                        chunk_id: Some(cid),
                        seal_length: sealed_length_units,
                    })
                    .await?;
                Location {
                    chunk_id: cid,
                    offset: 0,
                    length: bytes_in_chunk,
                    logical_offset: 0,
                    logical_length: bytes_in_chunk,
                }
            }
            Some(cid) => {
                warn!("seal: deleting empty chunk");
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest { chunk_id: Some(cid) })
                    .await;
                Location {
                    chunk_id: cid,
                    offset: 0,
                    length: 0,
                    logical_offset: 0,
                    logical_length: 0,
                }
            }
            None => {
                return Err(IoError::Internal("seal with no open chunk".into()));
            }
        };

        Ok(location)
    }

    /// Abort: cancel in-flight, delete the partial (unsealed) chunk.
    pub async fn abort(&mut self) -> Result<()> {
        if let Some(mut strip) = self.current_strip.take() {
            let _ = strip.abort().await;
        }
        // Delete the chunk if it was opened (has a chunk_id) and has
        // any data — either finished strips (bytes_in_chunk > 0) or
        // an in-progress strip (current_strip was Some).
        if let Some(chunk_id) = self.current_chunk_id {
            if self.bytes_in_chunk > 0 || self.strips_in_chunk > 0 {
                warn!("abort: deleting partial chunk");
                let _ = self
                    .allocator
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await;
            }
        }
        Ok(())
    }

    /// Non-async capacity hint. True if the current strip has room.
    pub fn ready(&self) -> bool {
        match &self.current_strip {
            Some(s) => s.ready(),
            None => false,
        }
    }

    /// Bytes written to the current chunk so far.
    pub fn bytes_in_chunk(&self) -> u64 {
        self.bytes_in_chunk
    }

    /// Current chunk id (if any).
    pub fn current_chunk_id(&self) -> Option<ChunkId> {
        self.current_chunk_id
    }

    /// Strips in the current chunk so far.
    pub fn strips_in_chunk(&self) -> u32 {
        self.strips_in_chunk
    }
}
