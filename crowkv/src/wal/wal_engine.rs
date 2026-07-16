// Copyright 2026-present buzzcrow <buzzcrow@126.com>
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
use crate::metrics::LatencySummary;
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
    /// Optional latency summary for `append` calls. Set via
    /// [`Self::set_append_summary`] when a metrics registry is wired.
    append_summary: OnceLock<Arc<LatencySummary>>,
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
        let pipeline_count = config.wal_disks.len();
        let failed = Arc::new(AtomicBool::new(false));
        let index = Arc::new(parking_lot::Mutex::new(SegmentIndex::new()));
        let next_segment_id = Arc::new(AtomicU64::new(1));
        let flush_count = Arc::new(AtomicU64::new(0));
        let records_flushed = Arc::new(AtomicU64::new(0));

        let coalesce = Duration::from_micros(config.wal_flush_coalesce_us);
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
                coalesce,
                watchdog,
                batch_bytes,
                failed.clone(),
                index.clone(),
                flush_count.clone(),
                records_flushed.clone(),
            );
            writer_tasks.push(task);

            pipelines.push(WalPipeline {
                pipeline_path: group_dir,
                backend: pipeline_backend,
                writer_tx,
                record_format,
            });
        }

        info!(group_id, pipeline_count, "wal engine created");

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
            append_summary: OnceLock::new(),
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
    pub fn config(&self) -> &WalConfig {
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
        }
    }

    /// Attach a latency summary for `append` instrumentation.
    /// Called once during group creation when a metrics registry is available.
    pub fn set_append_summary(&self, summary: Arc<LatencySummary>) {
        let _ = self.append_summary.set(summary);
    }
}

fn select_record_format(configured: WalRecordFormat) -> WalRecordFormat {
    match configured {
        WalRecordFormat::Auto => WalRecordFormat::Binary,
        explicit => explicit,
    }
}
