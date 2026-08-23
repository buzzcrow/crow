// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkPrefetch` — pre-create chunks + append strips ahead of the
//! write cursor.
//!
//! Streams the full cumulative `Chunk` protobuf (one per strip-append)
//! to the object layer. The object layer extracts the latest strip's
//! placement via `extract_placement_from_chunk` (bridge — removed in
//! Phase 2 when `EcStripWriter` holds `Arc<Chunk>` directly). Strip
//! planning stays here until Phase 3.2 moves it into `ChunkWriter`.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::chunk::strip::StripPlacement;
use crate::config::ChunkClientConfig;
use crate::traits::ChunkAllocator;
use crate::{IoError, Result};
use crow_common::ec::EcScheme;
use crow_protocol::chunk_id::CHUNK_TYPE_REPO;
use crow_protocol::chunkdb::rpc::chunk_strip::Strip as StripOneof;
use crow_protocol::chunkdb::rpc::{AllocateChunkRequest, AppendChunkRequest, Chunk, ChunkType, StripType};
use crow_protocol::common::ChunkId;

/// Chunk preallocation + strip prefetch.
pub struct ChunkPrefetch {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk_type_byte: u8,
}

impl ChunkPrefetch {
    /// Construct a new prefetch task.
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
        chunk_type_byte: u8,
    ) -> Self {
        Self {
            allocator,
            ec_scheme,
            config,
            chunk_type_byte,
        }
    }

    /// Spawn the prefetch task. Returns the chunk receiver + join
    /// handle. Pre-creates `prefetch_chunk_count` chunks (default 1),
    /// each with 1 strip, then appends strips ahead of the write
    /// cursor. Sends the full cumulative `Chunk` after each
    /// strip-append.
    pub fn spawn(self, object_size: Option<u64>) -> (mpsc::Receiver<Result<Chunk>>, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(self.config.prealloc_depth.max(1));
        let handle = tokio::spawn(async move {
            if let Err(e) = self.run(object_size, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });
        (rx, handle)
    }

    /// Run the prealloc loop. Allocates chunks + strips ahead of the
    /// write cursor, bounded by the channel capacity. Sends the full
    /// cumulative `Chunk` after each strip-append.
    async fn run(self, object_size: Option<u64>, tx: &mpsc::Sender<Result<Chunk>>) -> Result<()> {
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        let unit_bytes = u64::from(write_granularity_kb) * 1024;
        let strip_data_capacity = self.ec_scheme.data_num as u64 * unit_bytes;
        let strips_per_chunk = (self.config.max_chunk_size / strip_data_capacity).max(1) as u32;

        let total_strips = object_size.map(|s| (s.div_ceil(strip_data_capacity)) as usize);

        let mut allocated = 0usize;
        let mut current_chunk_id: Option<ChunkId> = None;
        let mut strips_in_current_chunk: u32 = 0;

        loop {
            if let Some(total) = total_strips {
                if allocated >= total {
                    break;
                }
            }

            let chunk = match current_chunk_id {
                None => {
                    let c = allocate_new_chunk(
                        &self.allocator,
                        self.ec_scheme,
                        write_granularity_kb,
                        self.chunk_type_byte,
                    )
                    .await?;
                    current_chunk_id = c.id;
                    strips_in_current_chunk = 1;
                    c
                }
                Some(_) if strips_in_current_chunk >= strips_per_chunk => {
                    let c = allocate_new_chunk(
                        &self.allocator,
                        self.ec_scheme,
                        write_granularity_kb,
                        self.chunk_type_byte,
                    )
                    .await?;
                    current_chunk_id = c.id;
                    strips_in_current_chunk = 1;
                    c
                }
                Some(cid) => {
                    let c = append_strip(
                        &self.allocator,
                        cid,
                        self.ec_scheme,
                        strips_in_current_chunk,
                        write_granularity_kb,
                    )
                    .await?;
                    strips_in_current_chunk += 1;
                    c
                }
            };

            tx.send(Ok(chunk))
                .await
                .map_err(|_| IoError::Internal("prealloc receiver dropped".into()))?;
            allocated += 1;
        }
        Ok(())
    }

    /// On-demand strip allocation when the prefetch task has finished
    /// but more data remains. Appends to the current chunk if it has
    /// room, otherwise allocates a new chunk. Returns the full
    /// cumulative `Chunk`.
    pub async fn on_demand(
        &self,
        current_chunk_id: Option<ChunkId>,
        strips_in_current_chunk: u32,
    ) -> Result<Chunk> {
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        let unit_bytes = u64::from(write_granularity_kb) * 1024;
        let strip_data_capacity = self.ec_scheme.data_num as u64 * unit_bytes;
        let strips_per_chunk = (self.config.max_chunk_size / strip_data_capacity).max(1) as u32;

        match current_chunk_id {
            Some(cid) if strips_in_current_chunk < strips_per_chunk => {
                append_strip(
                    &self.allocator,
                    cid,
                    self.ec_scheme,
                    strips_in_current_chunk,
                    write_granularity_kb,
                )
                .await
            }
            _ => {
                allocate_new_chunk(
                    &self.allocator,
                    self.ec_scheme,
                    write_granularity_kb,
                    CHUNK_TYPE_REPO,
                )
                .await
            }
        }
    }
}

/// Allocate a new chunk with 1 strip and return the full `Chunk`.
pub(crate) async fn allocate_new_chunk(
    chunkdb: &dyn ChunkAllocator,
    ec_scheme: EcScheme,
    write_granularity_kb: u32,
    chunk_type_byte: u8,
) -> Result<Chunk> {
    let chunk_id = crow_protocol::generate_chunk_id(chunk_type_byte).to_proto();
    let req = AllocateChunkRequest {
        chunk_id: Some(chunk_id),
        write_granularity: write_granularity_kb,
        strip_count: 1,
        strip_type: StripType::Ec as i32,
        data_num: ec_scheme.data_num as u32,
        code_num: ec_scheme.code_num as u32,
        copy_count: 0,
        chunk_type: ChunkType::Repo as i32,
    };
    let resp = chunkdb.allocate_chunk(req).await?;
    resp.chunk
        .ok_or_else(|| IoError::AllocationFailed("allocate_chunk response missing chunk".into()))
}

/// Append 1 strip to an existing chunk and return the full cumulative
/// `Chunk`.
pub(crate) async fn append_strip(
    chunkdb: &dyn ChunkAllocator,
    chunk_id: ChunkId,
    ec_scheme: EcScheme,
    strip_index_in_chunk: u32,
    write_granularity_kb: u32,
) -> Result<Chunk> {
    let req = AppendChunkRequest {
        chunk_id: Some(chunk_id),
        strip_size: ec_scheme.data_num as u32,
        strip_count: 1,
        strip_type: StripType::Ec as i32,
        data_num: ec_scheme.data_num as u32,
        code_num: ec_scheme.code_num as u32,
        copy_count: 0,
    };
    let _ = (write_granularity_kb, strip_index_in_chunk);
    let resp = chunkdb.append_chunk(req).await?;
    resp.chunk
        .ok_or_else(|| IoError::AllocationFailed("append_chunk response missing chunk".into()))
}

/// Extract the placement of the strip at `strip_index` from a chunk.
/// Bridge used by `ChunkWriter` while `EcStripWriter` still takes
/// `StripPlacement` (removed in Phase 2).
pub(crate) fn extract_placement_from_chunk(chunk: &Chunk, strip_index: u32) -> Result<StripPlacement> {
    let strip = chunk
        .strips
        .get(strip_index as usize)
        .ok_or_else(|| IoError::AllocationFailed(format!("chunk has no strip {strip_index}")))?;
    let oneof = strip
        .strip
        .as_ref()
        .ok_or_else(|| IoError::AllocationFailed("chunk strip missing oneof".into()))?;
    let segments = match oneof {
        StripOneof::EcStrip(ec) => ec.segments.clone(),
        StripOneof::MirrorStrip(_) => {
            return Err(IoError::AllocationFailed("expected EC strip, got mirror".into()));
        }
    };
    Ok(StripPlacement {
        chunk_id: chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("chunk missing id".into()))?,
        strip_index_in_chunk: strip_index,
        segments,
        unit_kb: strip.unit_kb,
    })
}
