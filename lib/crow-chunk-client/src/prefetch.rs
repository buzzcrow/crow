// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Strip preallocation task + chunk prefetch.
//!
//! A background task that allocates strips and chunks ahead of the
//! write cursor, bounded by `prealloc_depth` strips + 1 chunk ahead.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::traits::ChunkAllocator;
use crate::{IoError, Result};
use crow_common::ec::EcScheme;
use crow_protocol::chunkdb::rpc::chunk_strip::Strip as StripOneof;
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AppendChunkRequest, Chunk, ChunkStrip, ChunkType, StripType,
};
use crow_protocol::common::ChunkId;

/// A strip's placement: which chunk it belongs to, its index within
/// that chunk, and the disk segments for its EC blocks (data_num +
/// code_num segments, in order).
#[derive(Debug, Clone)]
pub struct StripPlacement {
    pub chunk_id: ChunkId,
    pub strip_index_in_chunk: u32,
    pub segments: Vec<crow_protocol::diskdb::rpc::Segment>,
    pub unit_kb: u32,
}

/// Extract the EC segments + unit_kb from the last strip of a chunk
/// response. Returns `None` if the chunk has no strips or the last
/// strip is not EC.
pub(crate) fn extract_last_strip(chunk: &Chunk) -> Option<(&ChunkStrip, &StripOneof)> {
    let last = chunk.strips.last()?;
    match &last.strip {
        Some(s) => Some((last, s)),
        None => None,
    }
}

/// Run the preallocation task. Allocates the first chunk with 1 strip,
/// then appends strips up to `prealloc_depth` ahead of the write
/// cursor (bounded by the channel capacity). When the current chunk
/// reaches `max_chunk_size`, allocates a new chunk.
///
/// The task exits when:
/// - All planned strips are allocated (known size), or
/// - The receiver is dropped (EOF / abort), causing send to fail.
pub(crate) fn spawn_prealloc_task<A: ChunkAllocator + 'static>(
    chunkdb: Arc<A>,
    ec_scheme: EcScheme,
    max_chunk_size: u64,
    prealloc_depth: usize,
    write_granularity_kb: u32,
    object_size: Option<u64>,
    chunk_type_byte: u8,
) -> (mpsc::Receiver<Result<StripPlacement>>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(prealloc_depth.max(1));

    let handle = tokio::spawn(async move {
        if let Err(e) = run_prealloc(
            &chunkdb,
            ec_scheme,
            max_chunk_size,
            prealloc_depth,
            write_granularity_kb,
            object_size,
            chunk_type_byte,
            &tx,
        )
        .await
        {
            // Send error into channel; ignore send failure (receiver
            // already dropped).
            let _ = tx.send(Err(e)).await;
        }
        // tx is dropped here → channel closes → receiver gets None.
    });

    (rx, handle)
}

/// Send a strip placement, mapping send errors to a clean exit.
async fn send_placement(tx: &mpsc::Sender<Result<StripPlacement>>, placement: StripPlacement) -> Result<()> {
    tx.send(Ok(placement))
        .await
        .map_err(|_| IoError::Internal("prealloc receiver dropped".into()))
}

/// Allocate a new chunk with 1 strip and return its placement.
pub(crate) async fn allocate_new_chunk_public<A: ChunkAllocator>(
    chunkdb: &A,
    ec_scheme: EcScheme,
    write_granularity_kb: u32,
    chunk_type_byte: u8,
) -> Result<StripPlacement> {
    allocate_new_chunk(chunkdb, ec_scheme, write_granularity_kb, chunk_type_byte).await
}

/// Allocate a new chunk with 1 strip and return its placement.
async fn allocate_new_chunk<A: ChunkAllocator>(
    chunkdb: &A,
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
pub(crate) async fn append_strip<A: ChunkAllocator>(
    chunkdb: &A,
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
    let _ = write_granularity_kb; // write_granularity is set at chunk allocation
    let resp = chunkdb.append_chunk(req).await?;
    let chunk = resp
        .chunk
        .ok_or_else(|| IoError::AllocationFailed("append_chunk response missing chunk".into()))?;
    extract_placement_from_chunk(&chunk, strip_index_in_chunk)
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

#[allow(clippy::too_many_arguments)]
async fn run_prealloc<A: ChunkAllocator>(
    chunkdb: &A,
    ec_scheme: EcScheme,
    max_chunk_size: u64,
    _prealloc_depth: usize,
    write_granularity_kb: u32,
    object_size: Option<u64>,
    chunk_type_byte: u8,
    tx: &mpsc::Sender<Result<StripPlacement>>,
) -> Result<()> {
    let unit_bytes = u64::from(write_granularity_kb) * 1024;
    let strip_data_capacity = ec_scheme.data_num as u64 * unit_bytes;
    let strips_per_chunk = (max_chunk_size / strip_data_capacity).max(1) as u32;

    // Plan total strips if size is known. The prealloc task allocates
    // exactly the planned count; if the stream exceeds the hint, the
    // main write task allocates additional strips on-demand.
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

        // Decide: allocate new chunk or append to current.
        let placement = match current_chunk_id {
            None => {
                // Allocate new chunk with 1 strip.
                let p = allocate_new_chunk(chunkdb, ec_scheme, write_granularity_kb, chunk_type_byte).await?;
                current_chunk_id = Some(p.chunk_id);
                strips_in_current_chunk = 1;
                p
            }
            Some(_) if strips_in_current_chunk >= strips_per_chunk => {
                // Chunk full — allocate new chunk with 1 strip.
                let p = allocate_new_chunk(chunkdb, ec_scheme, write_granularity_kb, chunk_type_byte).await?;
                current_chunk_id = Some(p.chunk_id);
                strips_in_current_chunk = 1;
                p
            }
            Some(cid) => {
                // Append strip to current chunk.
                let p = append_strip(
                    chunkdb,
                    cid,
                    ec_scheme,
                    strips_in_current_chunk,
                    write_granularity_kb,
                )
                .await?;
                strips_in_current_chunk += 1;
                p
            }
        };

        send_placement(tx, placement).await?;
        allocated += 1;
    }

    Ok(())
}
