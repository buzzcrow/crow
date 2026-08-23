// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkPrefetch` — pre-create chunks + append strips ahead of the
//! write cursor.
//!
//! Replaces `spawn_prealloc_task` + `run_prealloc` with a class whose
//! `spawn` method starts the loop and whose fields replace the 7 loop
//! parameters. Pre-creates `prefetch_chunk_count` chunks at start
//! (default 1), then appends strips up to `prealloc_depth` ahead.

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
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AppendChunkRequest, Chunk, ChunkStrip, ChunkType, StripType,
};
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

    /// Spawn the prefetch task. Returns the placement receiver + join
    /// handle. Pre-creates `prefetch_chunk_count` chunks (default 1),
    /// each with 1 strip, before sending the first `StripPlacement`.
    pub fn spawn(self, object_size: Option<u64>) -> (mpsc::Receiver<Result<StripPlacement>>, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(self.config.prealloc_depth.max(1));
        let handle = tokio::spawn(async move {
            if let Err(e) = self.run(object_size, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });
        (rx, handle)
    }

    /// Run the prealloc loop. Allocates chunks + strips ahead of the
    /// write cursor, bounded by the channel capacity.
    async fn run(self, object_size: Option<u64>, tx: &mpsc::Sender<Result<StripPlacement>>) -> Result<()> {
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

            let placement = match current_chunk_id {
                None => {
                    let p = allocate_new_chunk(
                        &self.allocator,
                        self.ec_scheme,
                        write_granularity_kb,
                        self.chunk_type_byte,
                    )
                    .await?;
                    current_chunk_id = Some(p.chunk_id);
                    strips_in_current_chunk = 1;
                    p
                }
                Some(_) if strips_in_current_chunk >= strips_per_chunk => {
                    let p = allocate_new_chunk(
                        &self.allocator,
                        self.ec_scheme,
                        write_granularity_kb,
                        self.chunk_type_byte,
                    )
                    .await?;
                    current_chunk_id = Some(p.chunk_id);
                    strips_in_current_chunk = 1;
                    p
                }
                Some(cid) => {
                    let p = append_strip(
                        &self.allocator,
                        cid,
                        self.ec_scheme,
                        strips_in_current_chunk,
                        write_granularity_kb,
                    )
                    .await?;
                    strips_in_current_chunk += 1;
                    p
                }
            };

            tx.send(Ok(placement))
                .await
                .map_err(|_| IoError::Internal("prealloc receiver dropped".into()))?;
            allocated += 1;
        }
        Ok(())
    }

    /// On-demand strip allocation when the prefetch task has finished
    /// but more data remains. Appends to the current chunk if it has
    /// room, otherwise allocates a new chunk.
    pub async fn on_demand(
        &self,
        current_chunk_id: Option<ChunkId>,
        strips_in_current_chunk: u32,
    ) -> Result<StripPlacement> {
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

/// Allocate a new chunk with 1 strip and return its placement.
pub(crate) async fn allocate_new_chunk(
    chunkdb: &dyn ChunkAllocator,
    ec_scheme: EcScheme,
    write_granularity_kb: u32,
    chunk_type_byte: u8,
) -> Result<StripPlacement> {
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
    let chunk = resp
        .chunk
        .ok_or_else(|| IoError::AllocationFailed("allocate_chunk response missing chunk".into()))?;
    extract_placement_from_chunk(&chunk, 0)
}

/// Append 1 strip to an existing chunk and return its placement.
pub(crate) async fn append_strip(
    chunkdb: &dyn ChunkAllocator,
    chunk_id: ChunkId,
    ec_scheme: EcScheme,
    strip_index_in_chunk: u32,
    write_granularity_kb: u32,
) -> Result<StripPlacement> {
    let req = AppendChunkRequest {
        chunk_id: Some(chunk_id),
        strip_size: ec_scheme.data_num as u32,
        strip_count: 1,
        strip_type: StripType::Ec as i32,
        data_num: ec_scheme.data_num as u32,
        code_num: ec_scheme.code_num as u32,
        copy_count: 0,
    };
    let _ = write_granularity_kb;
    let resp = chunkdb.append_chunk(req).await?;
    let chunk = resp
        .chunk
        .ok_or_else(|| IoError::AllocationFailed("append_chunk response missing chunk".into()))?;
    extract_placement_from_chunk(&chunk, strip_index_in_chunk)
}

/// Extract the EC segments + unit_kb from the last strip of a chunk
/// response.
pub(crate) fn extract_last_strip(chunk: &Chunk) -> Option<(&ChunkStrip, &StripOneof)> {
    let last = chunk.strips.last()?;
    match &last.strip {
        Some(s) => Some((last, s)),
        None => None,
    }
}

/// Extract the placement of the last strip from a chunk response.
fn extract_placement_from_chunk(chunk: &Chunk, strip_index: u32) -> Result<StripPlacement> {
    let (strip, oneof) = extract_last_strip(chunk)
        .ok_or_else(|| IoError::AllocationFailed("chunk has no EC strips".into()))?;
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
