// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `WalEngine` — multi-disk WAL coordinator (P2 W8).
//!
//! Owns the disk set, pipeline handles, segment index, and per-pipeline
//! writer tasks. Provides the `append` API consumed by the acceptor
//! durability hook (W6).

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::{info, trace};

use crate::common::config::WalConfig;
use crate::metrics::{Bandwidth, Counter, LatencySummary};
use crate::paxos::roles::SlotIndex;
use crate::paxos::PxGroupId;

use super::index::{SegmentIndex, SlotLocation};
use super::pipeline::WalPipeline;
use super::pipeline_backend::{WalBlockAlignment, WalPipelineBackend};
use super::pipeline_writer::{spawn_pipeline_writer, EncodedRecord, PendingWrite, WriterCommand};
use super::record::{WALRecord, WalRecordFormat};
use super::IoBackend;

/// Snapshot of WAL batch aggregation stats for observability/benchmarking.
#[derive(Clone, Copy, Debug, Default)]
pub struct BatchStats {
    /// Total number of durable flush batches executed across all pipelines.
    pub flush_count: u64,
    /// Total number of records flushed across all batches.
    pub records_flushed: u64,
    /// Total number of watchdog wakeups across all pipelines (idle writer
    /// safety-net timer fires). Non-zero while idle indicates the writer is
    /// cycling; near-zero under load indicates the wake path is working.
    pub watchdog_wakeups: u64,
}

/// Cumulative block device counter snapshot, read by the engine collector
/// to compute per-window deltas for the metrics registry.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlockDeviceSnapshot {
    pub logical_bytes_written: u64,
    pub physical_bytes_written: u64,
    pub rmw_count: u64,
}

/// Registered counter handles for block device metrics. Stored on
/// `WalEngine` via `OnceLock` and polled by the engine collector.
pub struct BlockDeviceCounterHandles {
    pub logical_bytes: Arc<Counter>,
    pub physical_bytes: Arc<Counter>,
    pub rmw: Arc<Counter>,
}

impl BatchStats {
    /// Average number of records per batch (0 if no flushes yet).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn avg_batch_size(&self) -> f64 {
        self.records_flushed
            .checked_div(self.flush_count)
            .map_or(0.0, |v| v as f64)
    }
}

/// The main WAL handle, shared (via `Arc`) by the acceptor and the GC worker.
pub struct WalEngine {
    backend: Arc<IoBackend>,
    #[allow(dead_code)]
    config: WalConfig,
    group_id: PxGroupId,
    /// One pipeline per disk. Each pipeline is independently lock-free for
    /// appends — the writer task owns the segment exclusively.
    pipelines: Vec<WalPipeline>,
    /// In-memory index: slot → location. Updated by writer tasks after flush.
    index: Arc<parking_lot::Mutex<SegmentIndex>>,
    /// Monotonically increasing segment id counter.
    next_segment_id: Arc<AtomicU64>,
    /// Number of configured pipelines (cached for lock-free `select_pipeline`).
    pipeline_count: usize,
    /// Set to true on disk I/O error; stops further writes.
    failed: Arc<AtomicBool>,
    /// Highest slot covered by a persisted snapshot marker.
    ///
    /// Records at or below this slot may be GC'd once the caller's safety
    /// criteria are met. `0` means no snapshot has been recorded.
    snapshot_slot: AtomicU64,
    /// Join handles for the writer tasks (aborted on drop).
    writer_tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Total number of durable flush batches across all pipelines.
    flush_count: Arc<AtomicU64>,
    /// Total number of records flushed across all batches.
    records_flushed: Arc<AtomicU64>,
    /// Total number of watchdog wakeups across all pipelines (idle writer
    /// safety-net timer fires).
    watchdog_wakeups: Arc<AtomicU64>,
    /// Optional latency summary for `append` calls. Set via
    /// [`Self::set_append_summary`] when a metrics registry is wired.
    append_summary: OnceLock<Arc<LatencySummary>>,
    /// Optional latency summary for `fdatasync` calls. Shared with writer
    /// tasks via `Arc<OnceLock>` so it can be set after spawn.
    fsync_summary: Arc<OnceLock<Arc<LatencySummary>>>,
    /// Optional bandwidth handle for batch write bytes. Shared with writer
    /// tasks via `Arc<OnceLock>` so it can be set after spawn.
    write_bandwidth: Arc<OnceLock<Arc<Bandwidth>>>,
    /// Optional block device counter handles for logical/physical bytes and
    /// RMW count. Set via [`Self::set_block_device_counters`] when a metrics
    /// registry is wired and the backend is `BlockDevice` or `MemBlock`.
    block_device_counters: OnceLock<BlockDeviceCounterHandles>,
}

impl Drop for WalEngine {
    fn drop(&mut self) {
        // Drop writer_tx by dropping pipelines, then abort tasks.
        for task in self.writer_tasks.lock().drain(..) {
            task.abort();
        }
    }
}

impl WalEngine {
    /// Create a new WAL manager for a consensus group.
    ///
    /// Creates disk directories if they don't exist. Does NOT replay
    /// (replay is a separate step via `replay::replay_group`).
    ///
    /// # Errors
    /// Returns IO error if disk paths cannot be created or accessed.
    pub async fn create(
        backend: Arc<IoBackend>,
        config: WalConfig,
        group_id: PxGroupId,
    ) -> io::Result<Arc<Self>> {
        Self::create_with_next_segment_id(backend, config, group_id, 1).await
    }

    /// Like [`create`] but with a custom initial `next_segment_id`.
    ///
    /// Use this when restoring from an existing WAL so the writer creates
    /// the *next* segment (e.g. `seg-0000002.ck`) instead of overwriting
    /// the existing `seg-0000001.ck`. The writer creates its initial
    /// active segment immediately on spawn, so setting
    /// `next_segment_id` after `create` via [`set_next_segment_id`]
    /// is too late — the initial segment is already created.
    ///
    /// # Errors
    /// Returns `io::Error` if the WAL directory cannot be created or
    /// the pipeline writer fails to spawn.
    pub async fn create_with_next_segment_id(
        backend: Arc<IoBackend>,
        config: WalConfig,
        group_id: PxGroupId,
        initial_next_segment_id: u64,
    ) -> io::Result<Arc<Self>> {
        let pipeline_count = config.wal_disks.len();
        let failed = Arc::new(AtomicBool::new(false));
        let index = Arc::new(parking_lot::Mutex::new(SegmentIndex::new()));
        let next_segment_id = Arc::new(AtomicU64::new(initial_next_segment_id));
        let flush_count = Arc::new(AtomicU64::new(0));
        let records_flushed = Arc::new(AtomicU64::new(0));
        let watchdog_wakeups = Arc::new(AtomicU64::new(0));
        let fsync_summary: Arc<OnceLock<Arc<LatencySummary>>> = Arc::new(OnceLock::new());
        let write_bandwidth: Arc<OnceLock<Arc<Bandwidth>>> = Arc::new(OnceLock::new());

        let watchdog = Duration::from_millis(config.wal_flush_watchdog_ms);
        let batch_bytes = config.wal_flush_batch_bytes;
        let segment_size = config.wal_segment_size;

        let mut pipelines = Vec::with_capacity(pipeline_count);
        let mut writer_tasks = Vec::with_capacity(pipeline_count);

        for (idx, disk_path) in config.wal_disks.iter().enumerate() {
            let group_dir = disk_path.join(format!("group{group_id}"));
            backend.create_dir_all(&group_dir).await?;
            let alignment = config.alignment();
            let pipeline_backend = match alignment {
                WalBlockAlignment::Unaligned => WalPipelineBackend::file(disk_path.clone()),
                WalBlockAlignment::Aligned { .. } => {
                    WalPipelineBackend::block(disk_path.to_string_lossy(), alignment)
                }
            };
            let record_format = select_record_format(config.wal_record_format);

            let (writer_tx, task) = spawn_pipeline_writer(
                idx,
                backend.clone(),
                group_dir.clone(),
                record_format,
                group_id,
                next_segment_id.clone(),
                segment_size,
                watchdog,
                batch_bytes,
                failed.clone(),
                index.clone(),
                flush_count.clone(),
                records_flushed.clone(),
                watchdog_wakeups.clone(),
                config.wal_skip_fsync,
                Arc::clone(&fsync_summary),
                Arc::clone(&write_bandwidth),
            );
            writer_tasks.push(task);

            pipelines.push(WalPipeline {
                pipeline_path: group_dir,
                backend: pipeline_backend,
                writer_tx,
                record_format,
            });
        }

        info!(
            group_id,
            pipeline_count,
            io_backend = ?backend,
            wal_aligned = config.wal_aligned,
            wal_io_unit_bytes = config.wal_io_unit_bytes,
            skip_fsync = config.wal_skip_fsync,
            "wal engine created"
        );

        Ok(Arc::new(Self {
            backend,
            config,
            group_id,
            pipelines,
            index,
            next_segment_id,
            pipeline_count,
            failed,
            snapshot_slot: AtomicU64::new(0),
            writer_tasks: parking_lot::Mutex::new(writer_tasks),
            flush_count,
            records_flushed,
            watchdog_wakeups,
            append_summary: OnceLock::new(),
            fsync_summary,
            write_bandwidth,
            block_device_counters: OnceLock::new(),
        }))
    }

    /// Append a WAL record, write it durably, and return the location.
    ///
    /// This is the **ack contract** path: the future only resolves after
    /// the record's durable flush completes.
    ///
    /// # Errors
    /// Returns IO error if the write or durable flush fails, or if WAL disk has failed.
    pub async fn append(&self, record: &WALRecord) -> io::Result<SlotLocation> {
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("WAL disk failed"));
        }

        let started = Instant::now();
        let pipeline_idx = self.select_pipeline(record);
        let pipeline = &self.pipelines[pipeline_idx];

        // Encode the record using the format resolved for this pipeline. Binary
        // uses the zero-copy frame; text-line keeps the formatted line.
        let encoded = match pipeline.record_format {
            WalRecordFormat::Binary => EncodedRecord::Binary(record.encode_frame()),
            WalRecordFormat::TextLine => EncodedRecord::TextLine(record.encode_text_line().into_bytes()),
            WalRecordFormat::Auto => unreachable!("Auto must be resolved at pipeline creation"),
        };

        // Enqueue to the writer task and await the durable-flush ack.
        //
        // Per-record allocations (unavoidable, inherent to async ack design):
        //   - `oneshot::channel()` — 1 allocation for the ack future
        //   - `mpsc::unbounded_send` — 1 allocation for the channel node
        // These are the minimum required by the channel-based ack contract
        // and cannot be eliminated without changing the concurrency model.
        let (ack_tx, ack_rx) = oneshot::channel();
        let pending = PendingWrite {
            encoded,
            slot: record.slot,
            ack: ack_tx,
        };

        pipeline
            .writer_tx
            .send(WriterCommand::Write(pending))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer stopped"))?;

        // Ack contract (W3/W6): resolve only after durable flush.
        // The writer sends back the SlotLocation.
        let loc = ack_rx
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer dropped ack"))??;

        trace!(
            group_id = self.group_id,
            slot = record.slot,
            pipeline_idx,
            "wal record appended and durably flushed"
        );

        if let Some(s) = self.append_summary.get() {
            #[allow(clippy::cast_possible_truncation)]
            s.observe(started.elapsed().as_nanos() as u64);
        }

        Ok(loc)
    }

    /// Select pipeline via deterministic slot affinity.
    fn select_pipeline(&self, record: &WALRecord) -> usize {
        if self.pipeline_count <= 1 {
            return 0;
        }
        let lane_slot = if record.slot == 0 { 0 } else { record.slot };
        let hash = lane_slot.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ record.group_id;
        usize::try_from(hash % self.pipeline_count as u64).expect("pipeline_count exceeds usize")
    }

    /// Seal all active segments (used during shutdown or forced rotation).
    ///
    /// # Errors
    /// Returns IO error if sealing any segment fails.
    pub async fn seal_all(&self) -> io::Result<()> {
        let mut acks = Vec::with_capacity(self.pipelines.len());
        for pipeline in &self.pipelines {
            let (tx, rx) = oneshot::channel();
            pipeline
                .writer_tx
                .send(WriterCommand::Seal { ack: tx })
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer stopped"))?;
            acks.push(rx);
        }
        for rx in acks {
            rx.await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer dropped ack"))??;
        }
        Ok(())
    }

    /// Durably flush all active segments (real `fsync`/`sync_all`). Used
    /// during shutdown to persist WAL data even when `--no-fsync` is set.
    /// Does NOT seal or rotate segments — just forces a durable flush.
    ///
    /// # Errors
    /// Returns IO error if flushing any segment fails.
    pub async fn flush_all(&self) -> io::Result<()> {
        let mut acks = Vec::with_capacity(self.pipelines.len());
        for pipeline in &self.pipelines {
            let (tx, rx) = oneshot::channel();
            pipeline
                .writer_tx
                .send(WriterCommand::Flush { ack: tx })
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer stopped"))?;
            acks.push(rx);
        }
        for rx in acks {
            rx.await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL writer dropped ack"))??;
        }
        Ok(())
    }

    /// Access the segment index (for GC, replay, lookup).
    pub fn index(&self) -> &parking_lot::Mutex<SegmentIndex> {
        &self.index
    }

    /// The group id this manager serves.
    #[must_use]
    pub fn group_id(&self) -> PxGroupId {
        self.group_id
    }

    /// Whether the disk has been marked failed.
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Return the highest slot covered by a persisted snapshot marker.
    #[must_use]
    pub fn snapshot_slot(&self) -> SlotIndex {
        self.snapshot_slot.load(Ordering::Acquire)
    }

    /// Set the highest slot covered by a persisted snapshot marker.
    pub fn set_snapshot_slot(&self, slot: SlotIndex) {
        self.snapshot_slot.store(slot, Ordering::Release);
    }

    /// Backend reference (for replay, GC file ops).
    pub fn backend(&self) -> &Arc<IoBackend> {
        &self.backend
    }

    /// Config reference.
    #[allow(dead_code)]
    pub(super) fn config(&self) -> &WalConfig {
        &self.config
    }

    /// Per-pipeline backend descriptors, in disk order. Reflects the alignment
    /// selected from [`WalConfig::wal_alignment`] at construction.
    pub fn pipeline_backends(&self) -> Vec<WalPipelineBackend> {
        self.pipelines
            .iter()
            .map(|pipeline| pipeline.backend.clone())
            .collect()
    }

    /// Pipeline paths (group-level subdirectories).
    pub fn disk_group_paths(&self) -> Vec<PathBuf> {
        self.pipelines
            .iter()
            .map(|pipeline| pipeline.pipeline_path.clone())
            .collect()
    }

    /// Set the next segment id (used after replay to resume from the highest seen).
    pub fn set_next_segment_id(&self, id: u64) {
        self.next_segment_id.store(id, Ordering::Release);
    }

    /// Get the failed flag for sharing with external components.
    pub fn failed_flag(&self) -> Arc<AtomicBool> {
        self.failed.clone()
    }

    /// Snapshot of batch aggregation stats across all pipelines.
    #[must_use]
    pub fn batch_stats(&self) -> BatchStats {
        BatchStats {
            flush_count: self.flush_count.load(Ordering::Relaxed),
            records_flushed: self.records_flushed.load(Ordering::Relaxed),
            watchdog_wakeups: self.watchdog_wakeups.load(Ordering::Relaxed),
        }
    }

    /// Short backend label for metric names (e.g. "file", "mem", "block").
    #[must_use]
    pub fn backend_label(&self) -> &'static str {
        match self.backend.as_ref() {
            IoBackend::File => "file",
            IoBackend::MemBlock(_) => "mem",
            IoBackend::BlockDevice(_) => "block",
        }
    }

    /// Attach a latency summary for `append` instrumentation.
    /// Called once during group creation when a metrics registry is available.
    pub fn set_append_summary(&self, summary: Arc<LatencySummary>) {
        let _ = self.append_summary.set(summary);
    }

    /// Attach latency summary and bandwidth handles for `fdatasync` and
    /// batch write bytes instrumentation. Called once during group creation
    /// when a metrics registry is available. Shared with writer tasks via
    /// `Arc<OnceLock>` so they pick up the handles without a restart.
    pub fn set_fsync_metrics(&self, fsync: Arc<LatencySummary>, bandwidth: Arc<Bandwidth>) {
        let _ = self.fsync_summary.set(fsync);
        let _ = self.write_bandwidth.set(bandwidth);
    }

    /// Attach block device counter handles for logical/physical bytes and
    /// RMW count. Called once during group creation when a metrics registry
    /// is available and the backend is `BlockDevice` or `MemBlock`.
    pub fn set_block_device_counters(&self, handles: BlockDeviceCounterHandles) {
        let _ = self.block_device_counters.set(handles);
    }

    /// Read cumulative block device counters for the engine collector's
    /// pre-flush poll. Returns `None` when no block device counters are
    /// registered (e.g. `File` backend or no metrics registry).
    #[must_use]
    pub fn block_device_snapshot(&self) -> Option<BlockDeviceSnapshot> {
        let _handles = self.block_device_counters.get()?;
        match self.backend.as_ref() {
            IoBackend::BlockDevice(dev) => Some(BlockDeviceSnapshot {
                logical_bytes_written: dev.logical_bytes_written(),
                physical_bytes_written: dev.physical_bytes_written(),
                rmw_count: dev.rmw_count(),
            }),
            IoBackend::MemBlock(dev) => Some(BlockDeviceSnapshot {
                logical_bytes_written: dev.logical_bytes_written(),
                physical_bytes_written: dev.physical_bytes_written(),
                rmw_count: dev.rmw_count(),
            }),
            IoBackend::File => None,
        }
    }

    /// Access the block device counter handles, if registered.
    #[must_use]
    pub fn block_device_counter_handles(&self) -> Option<&BlockDeviceCounterHandles> {
        self.block_device_counters.get()
    }
}

fn select_record_format(configured: WalRecordFormat) -> WalRecordFormat {
    match configured {
        WalRecordFormat::Auto => WalRecordFormat::Binary,
        explicit => explicit,
    }
}
