// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-pipeline dedicated writer task (W3).
//!
//! Each pipeline has one long-running async task that owns the active
//! `WalSegment` exclusively. Append callers push encoded records onto an
//! unbounded mpsc channel and await a oneshot ack. The writer drains all
//! queued records in a batch, writes them to the segment in one `pwrite`,
//! issues a single `fdatasync`, then resolves every pending ack.
//!
//! ## Scheduling
//!
//! The writer parks on `rx.recv()` when idle (zero CPU). On wake it drains
//! all ready records with `try_recv`, optionally coalesces for a bounded
//! window, then flushes once and acks the whole batch.
//!
//! ## Durability contract
//!
//! An ack resolves `Ok` only after the covering `fdatasync` succeeds. On
//! failure the writer marks the WAL failed, fails all batched acks, drains
//! and fails remaining queued records, then exits.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};
use tracing::{error, info, trace};

use crate::paxos::roles::SlotIndex;
use crate::paxos::PxGroupId;

use super::index::{SegmentIndex, SegmentMeta, SlotLocation};
use super::record::{RecordFrame, WalRecordFormat};
use super::segment::WalSegment;
use super::IoBackend;

/// Encoded record ready for the writer. Binary records use a zero-copy frame
/// that borrows the payload via `Bytes`; text-line records keep the formatted
/// line as bytes.
pub(crate) enum EncodedRecord {
    Binary(RecordFrame),
    TextLine(Vec<u8>),
}

impl EncodedRecord {
    /// Total on-disk byte length.
    #[must_use]
    pub fn total_len(&self) -> usize {
        match self {
            Self::Binary(frame) => frame.total_len(),
            Self::TextLine(bytes) => bytes.len(),
        }
    }

    /// Append this record's slices to `slices` for a vectored write.
    pub fn append_io_slices<'a>(&'a self, slices: &mut Vec<std::io::IoSlice<'a>>) {
        match self {
            Self::Binary(frame) => frame.append_io_slices(slices),
            Self::TextLine(bytes) => slices.push(std::io::IoSlice::new(bytes)),
        }
    }
}

/// A pending write request queued by an append caller.
pub(crate) struct PendingWrite {
    /// Already-encoded record.
    pub encoded: EncodedRecord,
    /// Slot index for index update (0 = metadata record, not indexed).
    pub slot: SlotIndex,
    /// Ack channel: resolves with the record's `SlotLocation` when the
    /// covering flush succeeds.
    pub ack: oneshot::Sender<io::Result<SlotLocation>>,
}

/// Commands sent to the writer task.
pub(crate) enum WriterCommand {
    /// A pending write from an append caller.
    Write(PendingWrite),
    /// Seal the active segment and open a new one. The oneshot is resolved
    /// after the seal is durable.
    Seal { ack: oneshot::Sender<io::Result<()>> },
}

/// Spawn the writer task for one pipeline. Returns the command sender.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_pipeline_writer(
    pipeline_idx: usize,
    backend: Arc<IoBackend>,
    pipeline_path: std::path::PathBuf,
    record_format: WalRecordFormat,
    group_id: PxGroupId,
    next_segment_id: Arc<AtomicU64>,
    segment_size: u64,
    coalesce: Duration,
    watchdog: Duration,
    batch_bytes: usize,
    failed: Arc<AtomicBool>,
    index: Arc<parking_lot::Mutex<SegmentIndex>>,
    flush_count: Arc<AtomicU64>,
    records_flushed: Arc<AtomicU64>,
) -> (mpsc::UnboundedSender<WriterCommand>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel::<WriterCommand>();
    let jh = tokio::spawn(pipeline_writer_loop(
        rx,
        pipeline_idx,
        backend,
        pipeline_path,
        record_format,
        group_id,
        next_segment_id,
        segment_size,
        coalesce,
        watchdog,
        batch_bytes,
        failed,
        index,
        flush_count,
        records_flushed,
    ));
    (tx, jh)
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn pipeline_writer_loop(
    mut rx: mpsc::UnboundedReceiver<WriterCommand>,
    pipeline_idx: usize,
    backend: Arc<IoBackend>,
    pipeline_path: std::path::PathBuf,
    record_format: WalRecordFormat,
    group_id: PxGroupId,
    next_segment_id: Arc<AtomicU64>,
    segment_size: u64,
    coalesce: Duration,
    watchdog: Duration,
    batch_bytes: usize,
    failed: Arc<AtomicBool>,
    index: Arc<parking_lot::Mutex<SegmentIndex>>,
    flush_count: Arc<AtomicU64>,
    records_flushed: Arc<AtomicU64>,
) {
    // Reusable per-batch buffers — declared outside the loop so their
    // allocations persist across wake cycles (no per-batch malloc/free).
    let mut batch: Vec<PendingWrite> = Vec::new();
    let mut offsets: Vec<u64> = Vec::new();

    // Create the initial active segment.
    let mut segment = match create_segment(
        &backend,
        &pipeline_path,
        &next_segment_id,
        group_id,
        record_format,
    )
    .await
    {
        Ok(seg) => seg,
        Err(e) => {
            failed.store(true, Ordering::Release);
            error!(pipeline_idx, error = %e, "failed to create initial WAL segment");
            drain_and_fail_all(&mut rx);
            return;
        }
    };

    loop {
        // Block until at least one command arrives (zero CPU when idle).
        let Some(cmd) = rx.recv().await else {
            // Channel closed — engine dropped. Seal and exit.
            let _ = segment.seal().await;
            register_sealed(&index, &segment, pipeline_idx);
            return;
        };

        match cmd {
            WriterCommand::Seal { ack } => {
                // Seal current segment, register it, open a new one.
                let result = segment.seal().await;
                if let Err(ref _e) = result {
                    failed.store(true, Ordering::Release);
                } else {
                    register_sealed(&index, &segment, pipeline_idx);
                }
                // Open new segment regardless (even on seal error, so future
                // appends don't panic — though failed flag will reject them).
                if result.is_ok() {
                    match create_segment(
                        &backend,
                        &pipeline_path,
                        &next_segment_id,
                        group_id,
                        record_format,
                    )
                    .await
                    {
                        Ok(new_seg) => segment = new_seg,
                        Err(e) => {
                            failed.store(true, Ordering::Release);
                            error!(pipeline_idx, error = %e, "failed to create new segment after seal");
                            let _ = ack.send(Err(e));
                            drain_and_fail_all(&mut rx);
                            return;
                        }
                    }
                }
                let _ = ack.send(result);
            }
            WriterCommand::Write(first) => {
                batch.clear();
                batch.push(first);
                let mut batch_bytes_acc = batch[0].encoded.total_len();
                let mut pending_seal_acks: Vec<oneshot::Sender<io::Result<()>>> = Vec::new();

                // Wake-drain: pull every command already queued.
                loop {
                    match rx.try_recv() {
                        Ok(WriterCommand::Write(req)) => {
                            batch_bytes_acc += req.encoded.total_len();
                            batch.push(req);
                            if batch_bytes_acc >= batch_bytes {
                                break;
                            }
                        }
                        Ok(WriterCommand::Seal { ack }) => {
                            pending_seal_acks.push(ack);
                            break;
                        }
                        Err(_) => break,
                    }
                }

                // Optional coalescing window.
                if !coalesce.is_zero() && batch_bytes_acc < batch_bytes {
                    let window = coalesce.min(watchdog);
                    let deadline = Instant::now() + window;
                    while batch_bytes_acc < batch_bytes {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            break;
                        }
                        match timeout(remaining, rx.recv()).await {
                            Ok(Some(WriterCommand::Write(req))) => {
                                batch_bytes_acc += req.encoded.total_len();
                                batch.push(req);
                            }
                            Ok(Some(WriterCommand::Seal { ack })) => {
                                pending_seal_acks.push(ack);
                                break;
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                }

                // Capture segment_id before potential rotation.
                let segment_id = segment.segment_id;

                // Write all batched records to the segment, then fdatasync.
                let write_result = write_batch(&mut segment, &batch, &mut offsets).await;

                match write_result {
                    Ok(()) => {
                        // Track batch aggregation stats.
                        flush_count.fetch_add(1, Ordering::Relaxed);
                        records_flushed.fetch_add(batch.len() as u64, Ordering::Relaxed);

                        // Update index for non-metadata records.
                        {
                            let mut idx = index.lock();
                            for (i, req) in batch.iter().enumerate() {
                                if req.slot != 0 {
                                    idx.insert(
                                        req.slot,
                                        SlotLocation {
                                            disk_idx: pipeline_idx,
                                            segment_id,
                                            file_offset: offsets[i],
                                        },
                                    );
                                }
                            }
                        }

                        // Check rotation after write.
                        if segment.is_full(segment_size) {
                            if let Err(e) = segment.seal().await {
                                failed.store(true, Ordering::Release);
                                fail_batch(&mut batch, &e);
                                drain_and_fail_all(&mut rx);
                                return;
                            }
                            register_sealed(&index, &segment, pipeline_idx);
                            match create_segment(
                                &backend,
                                &pipeline_path,
                                &next_segment_id,
                                group_id,
                                record_format,
                            )
                            .await
                            {
                                Ok(new_seg) => segment = new_seg,
                                Err(e) => {
                                    failed.store(true, Ordering::Release);
                                    fail_batch(&mut batch, &e);
                                    drain_and_fail_all(&mut rx);
                                    return;
                                }
                            }
                        }

                        // Resolve all acks with the record's SlotLocation.
                        for (i, req) in batch.drain(..).enumerate() {
                            let _ = req.ack.send(Ok(SlotLocation {
                                disk_idx: pipeline_idx,
                                segment_id,
                                file_offset: offsets[i],
                            }));
                        }

                        // Process any pending seal acks from the drain phase.
                        for seal_ack in pending_seal_acks.drain(..) {
                            let result = segment.seal().await;
                            if let Err(ref _e) = result {
                                failed.store(true, Ordering::Release);
                            } else {
                                register_sealed(&index, &segment, pipeline_idx);
                            }
                            if result.is_ok() {
                                match create_segment(
                                    &backend,
                                    &pipeline_path,
                                    &next_segment_id,
                                    group_id,
                                    record_format,
                                )
                                .await
                                {
                                    Ok(new_seg) => segment = new_seg,
                                    Err(e) => {
                                        failed.store(true, Ordering::Release);
                                        let _ = seal_ack.send(Err(e));
                                        drain_and_fail_all(&mut rx);
                                        return;
                                    }
                                }
                            }
                            let _ = seal_ack.send(result);
                        }
                    }
                    Err(e) => {
                        failed.store(true, Ordering::Release);
                        fail_batch(&mut batch, &e);
                        drain_and_fail_all(&mut rx);
                        return;
                    }
                }
            }
        }
    }
}

/// Maximum number of `IoSlice` entries per `writev` call. Linux `IOV_MAX` is
/// 1024; macOS is the same. We use a conservative constant to avoid platform
/// probes at runtime.
const MAX_IOV: usize = 1024;

/// Write a batch of encoded records to the segment in one vectored write,
/// then `fdatasync`. Fills `offsets` with the file offset of each record in
/// the batch. The caller-owned `offsets` Vec is cleared and reused across
/// batches to avoid per-batch heap allocation.
async fn write_batch(
    segment: &mut WalSegment,
    batch: &[PendingWrite],
    offsets: &mut Vec<u64>,
) -> io::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let total_len: usize = batch.iter().map(|r| r.encoded.total_len()).sum();

    // Compute per-record offsets from the current segment tail.
    offsets.clear();
    offsets.reserve(batch.len());
    let base_offset = segment.len();
    let mut cur = base_offset;
    for req in batch {
        offsets.push(cur);
        cur += req.encoded.total_len() as u64;
    }

    // Build a vectored slice list covering the whole batch. Binary records
    // contribute four slices (frame_len, header, payload, crc); text-line
    // records contribute one. This avoids any caller-side concatenation copy.
    //
    // This Vec is allocated per batch (not reused across wake cycles) because
    // `IoSlice<'a>` borrows data from `EncodedRecord` inside `batch`; the
    // borrow checker prevents hoisting it outside the `Write` arm. Pre-sized
    // to avoid reallocation — 1 malloc per batch, 0 reallocs.
    let mut io_slices: Vec<std::io::IoSlice<'_>> = Vec::with_capacity(batch.len() * 4);
    for req in batch {
        req.encoded.append_io_slices(&mut io_slices);
    }

    // Write in chunks of MAX_IOV slices. Each chunk advances the segment
    // write_offset, so the next chunk starts at the right place.
    let mut chunk_start = 0;
    while chunk_start < io_slices.len() {
        let chunk_end = (chunk_start + MAX_IOV).min(io_slices.len());
        let chunk = &io_slices[chunk_start..chunk_end];
        if chunk.iter().map(|s| s.len()).sum::<usize>() > 0 {
            segment.write_raw_vectored(chunk).await?;
        }
        chunk_start = chunk_end;
    }

    // Update segment metadata (min/max slot, record_count).
    for req in batch {
        if req.slot != 0 {
            if req.slot < segment.min_slot {
                segment.min_slot = req.slot;
            }
            if req.slot > segment.max_slot {
                segment.max_slot = req.slot;
            }
        }
        segment.record_count += 1;
    }

    // Single durable flush for the whole batch.
    segment.fdatasync().await?;

    trace!(
        segment_id = segment.segment_id,
        records = batch.len(),
        bytes = total_len,
        "batch written and flushed"
    );

    Ok(())
}

/// Create a new segment, allocating the next segment id.
async fn create_segment(
    backend: &IoBackend,
    path: &std::path::Path,
    next_segment_id: &AtomicU64,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
) -> io::Result<WalSegment> {
    let seg_id = next_segment_id.fetch_add(1, Ordering::Relaxed);
    WalSegment::create_with_format(backend, path, seg_id, group_id, record_format).await
}

/// Register a sealed segment in the index.
fn register_sealed(index: &parking_lot::Mutex<SegmentIndex>, segment: &WalSegment, pipeline_idx: usize) {
    let meta = SegmentMeta {
        segment_id: segment.segment_id,
        disk_idx: pipeline_idx,
        min_slot: segment.min_slot,
        max_slot: segment.max_slot,
        record_count: segment.record_count,
    };
    index.lock().register_segment(meta);
    info!(
        segment_id = segment.segment_id,
        min_slot = segment.min_slot,
        max_slot = segment.max_slot,
        "segment sealed"
    );
}

/// Fail all pending writes in a batch with the given error. Drains the
/// batch Vec in place so the allocation can be reused across wake cycles.
fn fail_batch(batch: &mut Vec<PendingWrite>, e: &io::Error) {
    let kind = e.kind();
    for req in batch.drain(..) {
        let _ = req
            .ack
            .send(Err(io::Error::new(kind, "WAL durable flush failed")));
    }
}

/// Drain all remaining commands from the channel and fail any Write acks.
fn drain_and_fail_all(rx: &mut mpsc::UnboundedReceiver<WriterCommand>) {
    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            WriterCommand::Write(req) => {
                let _ = req.ack.send(Err(io::Error::other("WAL disk failed")));
            }
            WriterCommand::Seal { ack } => {
                let _ = ack.send(Err(io::Error::other("WAL disk failed")));
            }
        }
    }
}
