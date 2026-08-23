// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Pipeline stages: fetch + main write + parity.
//!
//! The pipeline drives block-granularity overlap: fetch reads 1 MB →
//! main write writes the data block immediately → hands parity off in
//! background → advances to the next strip without waiting for EC.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::prefetch::StripPlacement;
use crate::traits::{BlockWriter, ChunkAllocator};
use crate::{IoError, Location, Result};
use crow_common::ec::{encode_parity_from_shards, EcScheme};
use crow_diskio_client::DiskId as DiskioDiskId;
use crow_protocol::chunk_id::CHUNK_TYPE_REPO;
use crow_protocol::chunkdb::rpc::{DeleteChunkRequest, SealChunkRequest};
use crow_protocol::common::ChunkId;

/// Convert a proto `DiskId` to the diskio-client `DiskId`.
pub(crate) fn to_diskio_id(proto_id: &crow_protocol::common::DiskId) -> DiskioDiskId {
    DiskioDiskId::new(proto_id.high, proto_id.low)
}

/// Run the fetch stage: reads from `reader` in ≤ `read_buffer_size`
/// chunks, accumulates to full blocks, and sends `Bytes` to the block
/// channel. On EOF, sends any partial last block, then returns (drops
/// the sender → main write task sees EOF).
pub(crate) async fn run_fetch_stage<R>(mut reader: R, block_tx: mpsc::Sender<Bytes>, read_buffer_size: usize)
where
    R: tokio::io::AsyncRead + Unpin + Send,
{
    let mut buf = BytesMut::with_capacity(read_buffer_size);
    let mut read_buf = vec![0u8; read_buffer_size];

    loop {
        match reader.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);
                while buf.len() >= read_buffer_size {
                    let block = buf.split_to(read_buffer_size);
                    if block_tx.send(block.freeze()).await.is_err() {
                        return;
                    }
                }
            }
            Err(_) => {
                warn!("fetch stage read error, sending partial buffer");
                break;
            }
        }
    }
    if !buf.is_empty() {
        let _ = block_tx.send(buf.freeze()).await;
    }
}

/// Spawn a parity task for one strip. EC-encodes parity from the data
/// shards, writes parity blocks, and fsyncs all disks. Bounded by the
/// `parity_depth` semaphore (acquired before calling).
pub(crate) fn spawn_parity_task<W: BlockWriter + 'static>(
    diskio: Arc<W>,
    ec_scheme: EcScheme,
    data_shards: Vec<Bytes>,
    segments: Vec<crow_protocol::diskdb::rpc::Segment>,
    unit_bytes: u64,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        // EC-encode parity from the data shards.
        let shard_refs: Vec<&[u8]> = data_shards.iter().map(Bytes::as_ref).collect();
        let parity = encode_parity_from_shards(ec_scheme, &shard_refs)?;

        // Write parity blocks to segments data_num..total.
        for (i, parity_block) in parity.iter().enumerate() {
            let seg = segments
                .get(ec_scheme.data_num + i)
                .ok_or_else(|| IoError::Internal("parity segment missing".into()))?;
            let disk_id = seg
                .disk_id
                .as_ref()
                .ok_or_else(|| IoError::Internal("segment missing disk_id".into()))?;
            let zone_offset = seg.unit_offset * unit_bytes;
            diskio
                .write(
                    to_diskio_id(disk_id),
                    seg.zone_index,
                    zone_offset,
                    Bytes::from(parity_block.clone()),
                )
                .await?;
        }

        // Fsync all disks in the strip (deduplicated).
        let mut fsynced: HashSet<(u64, u64)> = HashSet::new();
        for seg in &segments {
            if let Some(did) = &seg.disk_id {
                let key = (did.high, did.low);
                if fsynced.insert(key) {
                    diskio.fsync(to_diskio_id(did)).await?;
                }
            }
        }
        Ok(())
    })
}

/// State for the main write task.
struct MainWriteState {
    current_chunk_id: Option<ChunkId>,
    current_chunk_bytes: u64,
    logical_offset: u64,
    parity_handles: Vec<JoinHandle<Result<()>>>,
    locations: Vec<Location>,
}

impl MainWriteState {
    fn new() -> Self {
        Self {
            current_chunk_id: None,
            current_chunk_bytes: 0,
            logical_offset: 0,
            parity_handles: Vec::new(),
            locations: Vec::new(),
        }
    }
}

/// Write one strip's data blocks with whole-strip retry. Receives
/// blocks from `block_rx`, writing each to its segment. On a diskio
/// write failure, allocates a fresh strip placement via `append_chunk`
/// on the same chunk and re-writes all buffered blocks to the new
/// placement, then continues receiving the remaining blocks. Up to
/// `MAX_RETRIES` attempts; on exhaustion returns `IoError::WriteFailed`.
/// The failed strip's segments are leaked (R94 coarse; R110 refines to
/// single-block replacement).
async fn write_strip_with_retry<A, W>(
    chunkdb: &A,
    diskio: &W,
    ec_scheme: EcScheme,
    placement: &mut StripPlacement,
    block_rx: &mut mpsc::Receiver<Bytes>,
    first_block: Option<Bytes>,
) -> Result<(Vec<Bytes>, bool)>
where
    A: ChunkAllocator,
    W: BlockWriter,
{
    const MAX_RETRIES: u32 = 3;
    let data_num = ec_scheme.data_num;
    let mut data_shards: Vec<Bytes> = Vec::with_capacity(data_num);
    let mut next_to_write: usize = 0;
    let mut attempts: u32 = 0;
    let mut got_eof = false;

    // Seed with the first block if provided (on-demand allocation case:
    // we consumed a block from the channel to detect remaining data).
    if let Some(b) = first_block {
        data_shards.push(b.clone());
    }

    loop {
        // All data_num blocks written → strip data complete.
        if next_to_write >= data_num {
            break;
        }
        // Get the block to write: buffered (retry/seed) or from channel.
        let bytes = if next_to_write < data_shards.len() {
            data_shards[next_to_write].clone()
        } else if let Some(b) = block_rx.recv().await {
            data_shards.push(b.clone());
            b
        } else {
            got_eof = true;
            break;
        };

        let strip_unit_bytes = u64::from(placement.unit_kb) * 1024;
        let seg = placement
            .segments
            .get(next_to_write)
            .ok_or_else(|| IoError::Internal(format!("segment {next_to_write} missing")))?;
        let disk_id = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::Internal("segment missing disk_id".into()))?;
        let zone_offset = seg.unit_offset * strip_unit_bytes;

        match diskio
            .write(to_diskio_id(disk_id), seg.zone_index, zone_offset, bytes)
            .await
        {
            Ok(()) => {
                next_to_write += 1;
            }
            Err(e) => {
                attempts += 1;
                if attempts >= MAX_RETRIES {
                    return Err(IoError::WriteFailed(format!(
                        "strip write failed after {MAX_RETRIES} retries: {e}"
                    )));
                }
                warn!(
                    attempt = attempts,
                    "strip data write failed, retrying with new placement"
                );
                *placement = crate::prefetch::append_strip(
                    chunkdb,
                    placement.chunk_id,
                    ec_scheme,
                    placement.strip_index_in_chunk + attempts,
                    0,
                )
                .await?;
                next_to_write = 0;
            }
        }
    }

    Ok((data_shards, got_eof))
}

/// Allocate a strip on-demand when the prealloc task has finished but
/// more data remains. Appends to the current chunk if it has room,
/// otherwise allocates a new chunk.
async fn allocate_on_demand<A: ChunkAllocator>(
    chunkdb: &A,
    ec_scheme: EcScheme,
    current_chunk_id: Option<ChunkId>,
    strips_in_current_chunk: u32,
    strips_per_chunk: u32,
    write_granularity_kb: u32,
) -> Result<StripPlacement> {
    match current_chunk_id {
        Some(cid) if strips_in_current_chunk < strips_per_chunk => {
            crate::prefetch::append_strip(
                chunkdb,
                cid,
                ec_scheme,
                strips_in_current_chunk,
                write_granularity_kb,
            )
            .await
        }
        _ => {
            crate::prefetch::allocate_new_chunk_public(
                chunkdb,
                ec_scheme,
                write_granularity_kb,
                CHUNK_TYPE_REPO,
            )
            .await
        }
    }
}

/// Run the main write task: receives strip placements from the
/// prealloc channel and data blocks from the fetch channel, writes
/// data blocks to disk, hands off parity per strip, and handles chunk
/// rotation + completion.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_main_write_task<A, W>(
    chunkdb: Arc<A>,
    diskio: Arc<W>,
    ec_scheme: EcScheme,
    max_chunk_size: u64,
    parity_depth: usize,
    unit_bytes: u64,
    mut prealloc_rx: mpsc::Receiver<Result<StripPlacement>>,
    mut block_rx: mpsc::Receiver<Bytes>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<Vec<Location>>
where
    A: ChunkAllocator + 'static,
    W: BlockWriter + 'static,
{
    let parity_sem = Arc::new(tokio::sync::Semaphore::new(parity_depth));
    let mut state = MainWriteState::new();
    let write_granularity_kb = (unit_bytes / 1024) as u32;
    let strip_data_capacity = ec_scheme.data_num as u64 * unit_bytes;
    let strips_per_chunk = (max_chunk_size / strip_data_capacity).max(1) as u32;
    let mut strips_in_current_chunk: u32 = 0;

    loop {
        // 1. Await next strip placement. When prealloc is done (None),
        //    check if more data remains — if so, allocate on-demand.
        let (mut placement, first_block) = match prealloc_rx.recv().await {
            Some(Ok(p)) => (p, None),
            Some(Err(e)) => {
                abort_and_cleanup(&chunkdb, &diskio, &mut state).await;
                return Err(e);
            }
            None => {
                // Prealloc done. Peek at block_rx: if no data, we're
                // done; if data remains, allocate a strip on-demand.
                let Ok(first) = block_rx.try_recv() else {
                    break;
                };
                let p = allocate_on_demand(
                    &chunkdb,
                    ec_scheme,
                    state.current_chunk_id,
                    strips_in_current_chunk,
                    strips_per_chunk,
                    write_granularity_kb,
                )
                .await?;
                (p, Some(first))
            }
        };

        // 2. Check for chunk rotation.
        if let Some(old_id) = state.current_chunk_id {
            if placement.chunk_id != old_id {
                rotate_chunk(&chunkdb, &mut state, unit_bytes).await?;
                strips_in_current_chunk = 0;
            }
        }
        state.current_chunk_id = Some(placement.chunk_id);
        strips_in_current_chunk += 1;

        // 3. Receive + write data blocks with whole-strip retry.
        let (data_shards, got_eof) = match write_strip_with_retry(
            &chunkdb,
            &diskio,
            ec_scheme,
            &mut placement,
            &mut block_rx,
            first_block,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                abort_and_cleanup(&chunkdb, &diskio, &mut state).await;
                return Err(e);
            }
        };

        if data_shards.is_empty() {
            // EOF with no blocks for this strip — don't write it.
            break;
        }

        // 4. Hand off to parity task (bounded by semaphore).
        let strip_unit_bytes = u64::from(placement.unit_kb) * 1024;
        let strip_data_blocks = data_shards.len() as u64;
        let permit = parity_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IoError::Internal("parity semaphore closed".into()))?;
        let handle = spawn_parity_task(
            diskio.clone(),
            ec_scheme,
            data_shards,
            placement.segments.clone(),
            strip_unit_bytes,
        );
        state.parity_handles.push(handle);

        // Track bytes written for this strip.
        let strip_bytes = strip_data_blocks * strip_unit_bytes;
        state.current_chunk_bytes += strip_bytes;

        // Drop the permit — the semaphore bounds concurrent spawns,
        // not concurrent execution. Parity tasks run to completion in
        // the background and are joined at chunk rotation / completion.
        drop(permit);

        if got_eof {
            break;
        }
    }

    // 5. Drain: on clean finish, join parity + seal the current chunk.
    //    On abort (cancel), drop parity handles + delete the partial
    //    chunk; return the already-sealed Locations for caller cleanup.
    if *cancel_rx.borrow() {
        for h in state.parity_handles.drain(..) {
            h.abort();
        }
        if let Some(chunk_id) = state.current_chunk_id {
            if state.current_chunk_bytes > 0 {
                warn!("abort: deleting partial chunk");
                let _ = chunkdb
                    .delete_chunk(DeleteChunkRequest {
                        chunk_id: Some(chunk_id),
                    })
                    .await;
            }
        }
        return Ok(state.locations);
    }
    finish_and_seal(&chunkdb, &mut state, unit_bytes).await?;
    Ok(state.locations)
}

/// Rotate: join parity tasks for current chunk, seal it, record
/// Location, reset for the new chunk.
async fn rotate_chunk<A: ChunkAllocator>(
    chunkdb: &A,
    state: &mut MainWriteState,
    unit_bytes: u64,
) -> Result<()> {
    let chunk_id = state.current_chunk_id.unwrap();

    // Join in-flight parity tasks.
    join_parity_handles(&mut state.parity_handles).await?;

    // Seal the chunk.
    let sealed_length_units = (state.current_chunk_bytes / unit_bytes) as u32;
    chunkdb
        .seal_chunk(SealChunkRequest {
            chunk_id: Some(chunk_id),
            seal_length: sealed_length_units,
        })
        .await?;

    // Record Location.
    state.locations.push(Location {
        chunk_id,
        offset: 0,
        length: state.current_chunk_bytes,
        logical_offset: state.logical_offset,
        logical_length: state.current_chunk_bytes,
    });
    state.logical_offset += state.current_chunk_bytes;
    state.current_chunk_bytes = 0;
    Ok(())
}

/// Finish: join all parity tasks, seal the current chunk (if it has
/// data), delete it if empty.
async fn finish_and_seal<A: ChunkAllocator>(
    chunkdb: &A,
    state: &mut MainWriteState,
    unit_bytes: u64,
) -> Result<()> {
    // Join in-flight parity tasks.
    join_parity_handles(&mut state.parity_handles).await?;

    if state.current_chunk_bytes > 0 {
        if let Some(chunk_id) = state.current_chunk_id {
            let sealed_length_units = (state.current_chunk_bytes / unit_bytes) as u32;
            chunkdb
                .seal_chunk(SealChunkRequest {
                    chunk_id: Some(chunk_id),
                    seal_length: sealed_length_units,
                })
                .await?;
            state.locations.push(Location {
                chunk_id,
                offset: 0,
                length: state.current_chunk_bytes,
                logical_offset: state.logical_offset,
                logical_length: state.current_chunk_bytes,
            });
        }
    } else if let Some(chunk_id) = state.current_chunk_id {
        // Empty chunk — delete it.
        warn!("deleting empty chunk at completion");
        let _ = chunkdb
            .delete_chunk(DeleteChunkRequest {
                chunk_id: Some(chunk_id),
            })
            .await;
    }
    Ok(())
}

/// Abort: drop parity handles, delete the current (unsealed) chunk.
async fn abort_and_cleanup<A: ChunkAllocator, W: BlockWriter>(
    chunkdb: &A,
    _diskio: &Arc<W>,
    state: &mut MainWriteState,
) {
    // Drop parity handles — tasks finish in background.
    for handle in state.parity_handles.drain(..) {
        handle.abort();
    }
    // Delete the current unsealed chunk.
    if let Some(chunk_id) = state.current_chunk_id {
        if state.current_chunk_bytes > 0 {
            warn!("aborting write, deleting partial chunk");
            let _ = chunkdb
                .delete_chunk(DeleteChunkRequest {
                    chunk_id: Some(chunk_id),
                })
                .await;
        }
    }
}

/// Join all parity task handles, collecting the first error.
async fn join_parity_handles(handles: &mut Vec<JoinHandle<Result<()>>>) -> Result<()> {
    let mut first_err: Option<IoError> = None;
    for handle in handles.drain(..) {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(IoError::Internal(format!("parity task panicked: {e}")));
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
