// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkWriter` — chunk wrapper + write ability.
//!
//! Owns the current `Chunk` protobuf (in `Arc`, shared with
//! `EcStripWriter` once Phase 2 lands). The drive loop
//! (`LargeObjectWriter`) controls strip + chunk rotation by feeding
//! cumulative `Chunk` values; `ChunkWriter` pushes blocks to the
//! current strip and finishes strips on demand. Does NOT own the
//! drive loop yet (Phase 3). All chunkdb chunk operations (seal,
//! delete, append) go through this class.

use std::sync::Arc;

use bytes::Bytes;
use tracing::warn;

use crate::chunk::chunk_prefetch::extract_placement_from_chunk;
use crate::chunk::ec_strip_writer::EcStripWriter;
use crate::chunk::strip::{StripResult, StripWriter};
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::traits::ChunkAllocator;
use crate::{IoError, Location, Result};
use crow_common::ec::EcScheme;
use crow_protocol::chunkdb::rpc::{Chunk, DeleteChunkRequest, SealChunkRequest};
use crow_protocol::common::ChunkId;

/// Chunk wrapper + write ability. Owns `Arc<Chunk>`; the drive loop
/// still controls strip + chunk rotation (Phase 3 moves the strip
/// drive loop in here).
pub struct ChunkWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk: Option<Arc<Chunk>>,
    pub(crate) write_cursor: u32,
    pub(crate) bytes_in_chunk: u64,
    pub(crate) object_size: Option<u64>,
    pub(crate) strips_remaining: Option<usize>,
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
            chunk: None,
            write_cursor: 0,
            bytes_in_chunk: 0,
            object_size: None,
            strips_remaining: None,
            current_strip: None,
        }
    }

    /// Open a chunk from a pre-allocated `Chunk` protobuf. Wraps it in
    /// `Arc`, opens the first strip (already present from
    /// `allocate_chunk`). `object_size` is stored for strip-prefetch
    /// planning (used in Phase 3.2; no behavior yet).
    pub fn open(&mut self, chunk: Chunk, object_size: Option<u64>) -> Result<()> {
        let chunk_id = chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("open: chunk missing id".into()))?;
        if chunk.strips.is_empty() {
            return Err(IoError::AllocationFailed("open: chunk has no strips".into()));
        }
        self.object_size = object_size;
        self.strips_remaining = compute_strips_remaining(object_size, &self.ec_scheme, &self.config);
        let chunk = Arc::new(chunk);
        let placement = extract_placement_from_chunk(&chunk, 0)?;
        let strip = EcStripWriter::new(placement, self.disk_writer.clone(), self.ec_scheme);
        self.chunk = Some(chunk);
        self.write_cursor = 0;
        self.bytes_in_chunk = 0;
        self.current_strip = Some(StripWriter::Ec(strip));
        let _ = chunk_id;
        Ok(())
    }

    /// Continue with a new strip on the same chunk. `chunk` is the
    /// cumulative `Chunk` protobuf (from `append_chunk` response) with
    /// the next strip appended. Arc-swaps `self.chunk` and opens the
    /// strip at `write_cursor + 1`.
    pub fn continue_strip(&mut self, chunk: Chunk) -> Result<()> {
        let new_id = chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("continue_strip: chunk missing id".into()))?;
        let cur_id = self.current_chunk_id();
        if cur_id != Some(new_id) {
            return Err(IoError::Internal("continue_strip with different chunk_id".into()));
        }
        let next_index = self.write_cursor + 1;
        let chunk = Arc::new(chunk);
        let placement = extract_placement_from_chunk(&chunk, next_index)?;
        let strip = EcStripWriter::new(placement, self.disk_writer.clone(), self.ec_scheme);
        self.chunk = Some(chunk);
        self.write_cursor = next_index;
        self.current_strip = Some(StripWriter::Ec(strip));
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

    /// Is the chunk full (bytes written >= max_chunk_size)? The object
    /// layer checks this after each push to decide chunk rotation.
    pub fn is_full(&self) -> bool {
        self.bytes_in_chunk >= self.config.max_chunk_size
    }

    /// Append a new strip to the current chunk via `append_chunk` RPC.
    /// Returns the full cumulative `Chunk` (with the new strip
    /// appended). Not used by the current drive loop (it goes through
    /// `ChunkPrefetch::on_demand`); kept for the Phase 3 internal
    /// strip prefetch.
    pub async fn append_strip(&mut self) -> Result<Chunk> {
        let chunk_id = self
            .current_chunk_id()
            .ok_or_else(|| IoError::Internal("append_strip with no open chunk".into()))?;
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        crate::chunk::chunk_prefetch::append_strip(
            &*self.allocator,
            chunk_id,
            self.ec_scheme,
            self.write_cursor + 1,
            write_granularity_kb,
        )
        .await
    }

    /// Seal the chunk: seal_chunk RPC, return the chunk's Location.
    /// The current strip should already be finished.
    pub async fn seal(&mut self) -> Result<Location> {
        let chunk_id = self.current_chunk_id();
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
        let had_strip = self.current_strip.is_some();
        if let Some(mut strip) = self.current_strip.take() {
            let _ = strip.abort().await;
        }
        // Delete the chunk if it was opened and has any data — either
        // finished strips (bytes_in_chunk > 0), an in-progress strip
        // (had_strip), or prior finished strips (write_cursor > 0).
        if let Some(chunk_id) = self.current_chunk_id() {
            if self.bytes_in_chunk > 0 || had_strip || self.write_cursor > 0 {
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

    /// Current chunk id (if any), derived from the owned `Chunk`.
    pub fn current_chunk_id(&self) -> Option<ChunkId> {
        self.chunk.as_ref().and_then(|c| c.id)
    }

    /// Strips opened in the current chunk so far (= write_cursor + 1
    /// when a chunk is open).
    pub fn strips_in_chunk(&self) -> u32 {
        if self.chunk.is_some() {
            self.write_cursor + 1
        } else {
            0
        }
    }
}

/// Compute the number of strips not yet allocated for a known-size
/// object. Returns `None` for unknown-size objects. No behavior in
/// Phase 1 (used by the Phase 3.2 internal strip prefetch).
fn compute_strips_remaining(
    object_size: Option<u64>,
    ec_scheme: &EcScheme,
    config: &ChunkClientConfig,
) -> Option<usize> {
    let total = object_size?;
    let unit_bytes = u64::from((config.read_buffer_size / 1024) as u32) * 1024;
    let strip_data_capacity = ec_scheme.data_num as u64 * unit_bytes;
    let total_strips = total.div_ceil(strip_data_capacity) as usize;
    Some(total_strips)
}
