// Copyright 2026-present Gian <crow.db@outlook.com>
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
//! The writer parks on `timeout(watchdog, rx.recv())` when idle. On wake it
//! drains all ready records with `try_recv`, then flushes once and acks the
//! whole batch. The watchdog (`wal_flush_watchdog_ms`, default 100 ms) is a
//! safety-net timer that fires periodically while idle to drain any queued
//! record in case of a missed wake — the idle wakeup does a `try_recv` and
//! re-parks if nothing is queued (no I/O, ~10 wakeups/s at the default).
//!
//! ## Durability contract
//!
//! An ack resolves `Ok` only after the covering `fdatasync` succeeds. On
//! failure the writer marks the WAL failed, fails all batched acks, drains
//! and fails remaining queued records, then exits.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};
use tracing::{debug, error, trace};

use crate::metrics::{Bandwidth, LatencySummary};
use crate::paxos::roles::SlotIndex;
use crate::paxos::PxGroupId;

use super::index::{SegmentMeta, ShardedSegmentIndex, SlotLocation};
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

/// Acknowledgment sender for Seal/Flush commands.
type WriterAck = oneshot::Sender<io::Result<()>>;

/// Commands sent to the writer task.
pub(crate) enum WriterCommand {
    /// A pending write from an append caller.
    Write(PendingWrite),
    /// Seal the active segment and open a new one. The oneshot is resolved
    /// after the seal is durable.
    Seal { ack: oneshot::Sender<io::Result<()>> },
    /// Durably flush the active segment (real `fsync`/`sync_all`). Used
    /// during shutdown to persist data even when `--no-fsync` is set.
    /// Does NOT seal or rotate the segment.
    Flush { ack: oneshot::Sender<io::Result<()>> },
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
    watchdog: Duration,
    batch_bytes: usize,
    failed: Arc<AtomicBool>,
    index: Arc<ShardedSegmentIndex>,
    flush_count: Arc<AtomicU64>,
    records_flushed: Arc<AtomicU64>,
    watchdog_wakeups: Arc<AtomicU64>,
    skip_fsync: bool,
    fsync_summary: Arc<OnceLock<Arc<LatencySummary>>>,
    write_bandwidth: Arc<OnceLock<Arc<Bandwidth>>>,
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
        watchdog,
        batch_bytes,
        failed,
        index,
        flush_count,
        records_flushed,
        watchdog_wakeups,
        skip_fsync,
        fsync_summary,
        write_bandwidth,
    ));
    (tx, jh)
}

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
    watchdog: Duration,
    batch_bytes: usize,
    failed: Arc<AtomicBool>,
    index: Arc<ShardedSegmentIndex>,
    flush_count: Arc<AtomicU64>,
    records_flushed: Arc<AtomicU64>,
    watchdog_wakeups: Arc<AtomicU64>,
    skip_fsync: bool,
    fsync_summary: Arc<OnceLock<Arc<LatencySummary>>>,
    write_bandwidth: Arc<OnceLock<Arc<Bandwidth>>>,
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
        let Some(cmd) = recv_command(
            &mut rx,
            watchdog,
            &mut segment,
            &index,
            pipeline_idx,
            &watchdog_wakeups,
        )
        .await
        else {
            return;
        };

        match cmd {
            WriterCommand::Seal { ack } => {
                if !handle_seal_command(
                    &mut segment,
                    ack,
                    &mut rx,
                    &backend,
                    &pipeline_path,
                    &next_segment_id,
                    group_id,
                    record_format,
                    &index,
                    pipeline_idx,
                    &failed,
                )
                .await
                {
                    return;
                }
            }
            WriterCommand::Write(first) => {
                if !handle_write_command(
                    first,
                    &mut batch,
                    &mut offsets,
                    &mut segment,
                    &mut rx,
                    &backend,
                    &pipeline_path,
                    record_format,
                    group_id,
                    &next_segment_id,
                    segment_size,
                    batch_bytes,
                    skip_fsync,
                    &fsync_summary,
                    &write_bandwidth,
                    &index,
                    pipeline_idx,
                    &flush_count,
                    &records_flushed,
                    &failed,
                )
                .await
                {
                    return;
                }
            }
            WriterCommand::Flush { ack } => {
                // Drain any pending writes first, then do a real fsync
                // on the active segment. This is the shutdown path —
                // always durable, regardless of --no-fsync.
                let result = segment.flush().await;
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                let _ = ack.send(result);
            }
        }
    }
}

/// Process a Write command: batch-drain, write, fsync, index, rotate, ack.
/// Returns `false` if a fatal error occurred and the writer loop should exit.
#[allow(clippy::too_many_arguments)]
async fn handle_write_command(
    first: PendingWrite,
    batch: &mut Vec<PendingWrite>,
    offsets: &mut Vec<u64>,
    segment: &mut WalSegment,
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    record_format: WalRecordFormat,
    group_id: PxGroupId,
    next_segment_id: &Arc<AtomicU64>,
    segment_size: u64,
    batch_bytes: usize,
    skip_fsync: bool,
    fsync_summary: &OnceLock<Arc<LatencySummary>>,
    write_bandwidth: &OnceLock<Arc<Bandwidth>>,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    flush_count: &Arc<AtomicU64>,
    records_flushed: &Arc<AtomicU64>,
    failed: &Arc<AtomicBool>,
) -> bool {
    batch.clear();
    batch.push(first);
    let mut batch_bytes_acc = batch[0].encoded.total_len();
    let (mut pending_seal_acks, pending_flush_acks) =
        drain_pending_commands(rx, batch, &mut batch_bytes_acc, batch_bytes);

    // Capture segment_id before potential rotation.
    let segment_id = segment.segment_id;

    let write_result = write_batch(
        segment,
        batch,
        offsets,
        skip_fsync,
        fsync_summary,
        write_bandwidth,
    )
    .await;

    match write_result {
        Ok(()) => {
            flush_count.fetch_add(1, Ordering::Relaxed);
            records_flushed.fetch_add(batch.len() as u64, Ordering::Relaxed);
            update_index_for_batch(index, batch, offsets, pipeline_idx, segment_id);

            if let Err(e) = rotate_if_full(
                segment,
                backend,
                pipeline_path,
                next_segment_id,
                group_id,
                record_format,
                index,
                pipeline_idx,
                segment_size,
            )
            .await
            {
                failed.store(true, Ordering::Release);
                fail_batch(batch, &e);
                drain_and_fail_all(rx);
                return false;
            }

            resolve_batch_acks(batch, offsets, pipeline_idx, segment_id);

            if !process_pending_seal_acks(
                segment,
                &mut pending_seal_acks,
                rx,
                backend,
                pipeline_path,
                next_segment_id,
                group_id,
                record_format,
                index,
                pipeline_idx,
                failed,
            )
            .await
            {
                for ack in pending_flush_acks {
                    let _ = ack.send(Err(io::Error::other("WAL disk failed")));
                }
                return false;
            }
            for ack in pending_flush_acks {
                let result = segment.flush().await;
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                let _ = ack.send(result);
            }
            true
        }
        Err(e) => {
            failed.store(true, Ordering::Release);
            fail_batch(batch, &e);
            drain_and_fail_all(rx);
            false
        }
    }
}

/// Park until a command arrives or the watchdog fires. Returns `None` when
/// the channel is closed (engine dropped), after sealing and flushing. On a
/// watchdog wakeup with no queued command, re-parks and waits again.
async fn recv_command(
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    watchdog: Duration,
    segment: &mut WalSegment,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    watchdog_wakeups: &Arc<AtomicU64>,
) -> Option<WriterCommand> {
    loop {
        match timeout(watchdog, rx.recv()).await {
            Ok(Some(cmd)) => return Some(cmd),
            Ok(None) => {
                let _ = segment.seal().await;
                let _ = segment.flush().await;
                register_sealed(index, segment, pipeline_idx);
                return None;
            }
            Err(_) => {
                watchdog_wakeups.fetch_add(1, Ordering::Relaxed);
                if let Ok(cmd) = rx.try_recv() {
                    return Some(cmd);
                }
            }
        }
    }
}

/// Handle a Seal command: seal the current segment, register it, open a new
/// one, and resolve the ack. Returns `false` if a fatal error occurred.
#[allow(clippy::too_many_arguments)]
async fn handle_seal_command(
    segment: &mut WalSegment,
    ack: oneshot::Sender<io::Result<()>>,
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    next_segment_id: &Arc<AtomicU64>,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    failed: &Arc<AtomicBool>,
) -> bool {
    let result = segment.seal().await;
    if result.is_err() {
        failed.store(true, Ordering::Release);
    } else {
        register_sealed(index, segment, pipeline_idx);
    }
    if result.is_ok() {
        match create_segment(backend, pipeline_path, next_segment_id, group_id, record_format).await {
            Ok(new_seg) => *segment = new_seg,
            Err(e) => {
                failed.store(true, Ordering::Release);
                error!(pipeline_idx, error = %e, "failed to create new segment after seal");
                let _ = ack.send(Err(e));
                drain_and_fail_all(rx);
                return false;
            }
        }
    }
    let _ = ack.send(result);
    true
}

/// Drain all commands already queued in the channel, accumulating writes into
/// `batch` and collecting any seal/flush acks. Returns the pending seal and
/// flush acks that must be processed after the batch is flushed. At most one
/// of the two vectors is non-empty (the first Seal/Flush causes a break).
fn drain_pending_commands(
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    batch: &mut Vec<PendingWrite>,
    batch_bytes_acc: &mut usize,
    batch_bytes: usize,
) -> (Vec<WriterAck>, Vec<WriterAck>) {
    let mut pending_seal_acks: Vec<WriterAck> = Vec::new();
    let mut pending_flush_acks: Vec<WriterAck> = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(WriterCommand::Write(req)) => {
                *batch_bytes_acc += req.encoded.total_len();
                batch.push(req);
                if *batch_bytes_acc >= batch_bytes {
                    break;
                }
            }
            Ok(WriterCommand::Seal { ack }) => {
                pending_seal_acks.push(ack);
                break;
            }
            Ok(WriterCommand::Flush { ack }) => {
                pending_flush_acks.push(ack);
                break;
            }
            Err(_) => break,
        }
    }
    (pending_seal_acks, pending_flush_acks)
}

/// Insert index entries for every non-metadata record in the batch.
fn update_index_for_batch(
    index: &Arc<ShardedSegmentIndex>,
    batch: &[PendingWrite],
    offsets: &[u64],
    pipeline_idx: usize,
    segment_id: u64,
) {
    for (i, req) in batch.iter().enumerate() {
        if req.slot != 0 {
            index.insert(
                pipeline_idx,
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

/// If the segment is full, seal it, register it, and open a new segment.
/// Returns `Err` if sealing or creating the new segment fails.
#[allow(clippy::too_many_arguments)]
async fn rotate_if_full(
    segment: &mut WalSegment,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    next_segment_id: &Arc<AtomicU64>,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    segment_size: u64,
) -> io::Result<()> {
    if !segment.is_full(segment_size) {
        return Ok(());
    }
    segment.seal().await?;
    register_sealed(index, segment, pipeline_idx);
    *segment = create_segment(backend, pipeline_path, next_segment_id, group_id, record_format).await?;
    Ok(())
}

/// Send each pending write its resolved `SlotLocation`.
fn resolve_batch_acks(batch: &mut Vec<PendingWrite>, offsets: &[u64], pipeline_idx: usize, segment_id: u64) {
    for (i, req) in batch.drain(..).enumerate() {
        let _ = req.ack.send(Ok(SlotLocation {
            disk_idx: pipeline_idx,
            segment_id,
            file_offset: offsets[i],
        }));
    }
}

/// Process pending seal acks collected during the wake-drain phase.
/// Each seal ack triggers a segment seal + new-segment open cycle.
/// Returns `false` if a fatal error occurred and the writer loop should exit.
#[allow(clippy::too_many_arguments)]
async fn process_pending_seal_acks(
    segment: &mut WalSegment,
    pending_seal_acks: &mut Vec<oneshot::Sender<io::Result<()>>>,
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    next_segment_id: &Arc<AtomicU64>,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    failed: &Arc<AtomicBool>,
) -> bool {
    for seal_ack in pending_seal_acks.drain(..) {
        let result = segment.seal().await;
        if result.is_err() {
            failed.store(true, Ordering::Release);
        } else {
            register_sealed(index, segment, pipeline_idx);
        }
        if result.is_ok() {
            match create_segment(backend, pipeline_path, next_segment_id, group_id, record_format).await {
                Ok(new_seg) => *segment = new_seg,
                Err(e) => {
                    failed.store(true, Ordering::Release);
                    let _ = seal_ack.send(Err(e));
                    drain_and_fail_all(rx);
                    return false;
                }
            }
        }
        let _ = seal_ack.send(result);
    }
    true
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
    skip_fsync: bool,
    fsync_summary: &OnceLock<Arc<LatencySummary>>,
    write_bandwidth: &OnceLock<Arc<Bandwidth>>,
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

    // Single durable flush for the whole batch — skipped when skip_fsync is
    // set (benchmark isolation mode: data written but not durably flushed).
    if !skip_fsync {
        let fsync_start = Instant::now();
        segment.fdatasync().await?;
        if let Some(s) = fsync_summary.get() {
            #[allow(clippy::cast_possible_truncation)]
            s.observe(fsync_start.elapsed().as_nanos() as u64);
        }
    }

    // Observe batch write bytes (regardless of fsync — the write happened).
    if let Some(bw) = write_bandwidth.get() {
        #[allow(clippy::cast_possible_truncation)]
        bw.observe(total_len as u64);
    }

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
fn register_sealed(index: &ShardedSegmentIndex, segment: &WalSegment, pipeline_idx: usize) {
    let meta = SegmentMeta {
        segment_id: segment.segment_id,
        disk_idx: pipeline_idx,
        min_slot: segment.min_slot,
        max_slot: segment.max_slot,
        record_count: segment.record_count,
    };
    index.register_segment(pipeline_idx, meta);
    debug!(
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
            WriterCommand::Seal { ack } | WriterCommand::Flush { ack } => {
                let _ = ack.send(Err(io::Error::other("WAL disk failed")));
            }
        }
    }
}
