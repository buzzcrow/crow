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

/// Run the main write task: receives strip placements from the
/// prealloc channel and data blocks from the fetch channel, writes
/// data blocks to disk, hands off parity per strip, and handles chunk
/// rotation + completion.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_main_write_task<A, W>(
    chunkdb: Arc<A>,
    diskio: Arc<W>,
    ec_scheme: EcScheme,
    _max_chunk_size: u64,
    parity_depth: usize,
    unit_bytes: u64,
    prealloc_rx: &mut mpsc::Receiver<Result<StripPlacement>>,
    mut block_rx: mpsc::Receiver<Bytes>,
) -> Result<Vec<Location>>
where
    A: ChunkAllocator + 'static,
    W: BlockWriter + 'static,
{
    let data_num = ec_scheme.data_num;
    let parity_sem = Arc::new(tokio::sync::Semaphore::new(parity_depth));
    let mut state = MainWriteState::new();

    loop {
        // 1. Await next strip placement.
        let placement = match prealloc_rx.recv().await {
            Some(Ok(p)) => p,
            Some(Err(e)) => {
                abort_and_cleanup(&chunkdb, &diskio, &mut state).await;
                return Err(e);
            }
            None => break, // prealloc done (all strips allocated)
        };

        // 2. Check for chunk rotation.
        if let Some(old_id) = state.current_chunk_id {
            if placement.chunk_id != old_id {
                rotate_chunk(&chunkdb, &mut state, unit_bytes).await?;
            }
        }
        state.current_chunk_id = Some(placement.chunk_id);

        let strip_unit_bytes = u64::from(placement.unit_kb) * 1024;
        let segments = placement.segments;

        // 3. Receive data_num blocks for this strip.
        let mut data_shards: Vec<Bytes> = Vec::with_capacity(data_num);
        let mut got_eof = false;

        for block_idx in 0..data_num {
            if let Some(bytes) = block_rx.recv().await {
                let seg = segments
                    .get(block_idx)
                    .ok_or_else(|| IoError::Internal(format!("segment {block_idx} missing")))?;
                let disk_id = seg
                    .disk_id
                    .as_ref()
                    .ok_or_else(|| IoError::Internal("segment missing disk_id".into()))?;
                let zone_offset = seg.unit_offset * strip_unit_bytes;
                diskio
                    .write(to_diskio_id(disk_id), seg.zone_index, zone_offset, bytes.clone())
                    .await
                    .map_err(|e| IoError::WriteFailed(e.to_string()))?;
                data_shards.push(bytes);
            } else {
                got_eof = true;
                break;
            }
        }

        if data_shards.is_empty() {
            // EOF with no blocks for this strip — don't write it.
            break;
        }

        // 4. Hand off to parity task (bounded by semaphore).
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
            segments.clone(),
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

    // 5. Drain: join all parity tasks, seal current chunk, return.
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
