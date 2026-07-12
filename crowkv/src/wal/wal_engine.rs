//! `WalEngine` — multi-disk WAL coordinator (P2 W8).
//!
//! Owns the disk set, active segments (one per disk), segment index,
//! and fsync workers. Provides the `append` API consumed by the acceptor
//! durability hook (W6).

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::common::config::WalConfig;
use crate::paxos::PxGroupId;

use super::index::{SegmentIndex, SegmentMeta, SlotLocation};
use super::pipeline_backend::{WalBlockAlignment, WalPipelineBackend};
use super::record::{WALRecord, WalRecordFormat};
use super::segment::WalSegment;
use super::IoBackend;

/// The main WAL handle, shared (via `Arc`) by the acceptor and the GC worker.
pub struct WalEngine {
    backend: Arc<IoBackend>,
    config: WalConfig,
    group_id: PxGroupId,
    /// One active (unsealed) segment per disk.
    ///
    /// `tokio::sync::Mutex` because the guard is held across `.await`
    /// (segment create, record write, fdatasync). Using `parking_lot::Mutex`
    /// would make the resulting future `!Send`.
    pipelines: tokio::sync::Mutex<Vec<WalPipeline>>,
    /// In-memory index: slot → location. Accessed synchronously only.
    index: parking_lot::Mutex<SegmentIndex>,
    /// Monotonically increasing segment id counter.
    next_segment_id: AtomicU64,
    /// Round-robin counter for disk selection.
    rr_counter: AtomicU64,
    /// Number of configured pipelines (cached for lock-free `select_pipeline`).
    pipeline_count: usize,
    /// Set to true on disk I/O error; stops further writes.
    failed: Arc<AtomicBool>,
}

struct WalPipeline {
    pipeline_path: PathBuf,
    backend: WalPipelineBackend,
    active_segment: Option<WalSegment>,
    pipeline_idx: usize,
    record_format: WalRecordFormat,
}

impl WalPipeline {
    async fn ensure_active_segment(
        &mut self,
        backend: &IoBackend,
        next_segment_id: &AtomicU64,
        group_id: PxGroupId,
    ) -> io::Result<()> {
        if self.active_segment.is_none() {
            let seg_id = next_segment_id.fetch_add(1, Ordering::Relaxed);
            let seg = WalSegment::create_with_format(
                backend,
                &self.pipeline_path,
                seg_id,
                group_id,
                self.record_format,
            )
            .await?;
            self.active_segment = Some(seg);
        }
        Ok(())
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
        let mut pipelines = Vec::with_capacity(config.wal_disks.len());
        for (idx, disk_path) in config.wal_disks.iter().enumerate() {
            let group_dir = disk_path.join(format!("group{group_id}"));
            backend.create_dir_all(&group_dir).await?;
            let pipeline_backend = match config.wal_alignment {
                WalBlockAlignment::Unaligned => WalPipelineBackend::file(disk_path.clone()),
                WalBlockAlignment::Aligned { .. } => {
                    WalPipelineBackend::block(disk_path.to_string_lossy(), config.wal_alignment)
                }
            };
            let record_format = select_record_format(config.wal_record_format, &pipeline_backend);
            pipelines.push(WalPipeline {
                pipeline_path: group_dir,
                backend: pipeline_backend,
                active_segment: None,
                pipeline_idx: idx,
                record_format,
            });
        }

        let pipeline_count = pipelines.len();
        info!(group_id, pipeline_count, "wal engine created");

        Ok(Arc::new(Self {
            backend,
            config,
            group_id,
            pipelines: tokio::sync::Mutex::new(pipelines),
            index: parking_lot::Mutex::new(SegmentIndex::new()),
            next_segment_id: AtomicU64::new(1),
            rr_counter: AtomicU64::new(0),
            pipeline_count,
            failed: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Append a WAL record, write to disk, fdatasync, and return the location.
    ///
    /// This is the **ack contract** path: the future only resolves after
    /// the record is fsynced to disk.
    ///
    /// # Panics
    /// Panics if the segment is not available (internal invariant violation).
    ///
    /// # Errors
    /// Returns IO error if the write or fsync fails, or if WAL disk has failed.
    pub async fn append(&self, record: &WALRecord) -> io::Result<SlotLocation> {
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("WAL disk failed"));
        }

        let pipeline_idx = self.select_pipeline();

        let mut pipelines = self.pipelines.lock().await;
        let pipeline = &mut pipelines[pipeline_idx];

        pipeline
            .ensure_active_segment(&self.backend, &self.next_segment_id, self.group_id)
            .await?;

        let seg = pipeline.active_segment.as_mut().unwrap();
        let file_offset = seg.append(record).await?;
        let segment_id = seg.segment_id;

        if seg.is_full(self.config.wal_segment_size) {
            self.rotate_pipeline(&mut pipelines[pipeline_idx]).await?;
        }

        if let Some(seg) = pipelines[pipeline_idx].active_segment.as_ref() {
            if let Err(e) = seg.file().fdatasync().await {
                error!(group_id = self.group_id, pipeline_idx, error = %e, "fdatasync failed");
                self.failed.store(true, Ordering::Release);
                return Err(e);
            }
        }

        drop(pipelines);

        let loc = SlotLocation {
            disk_idx: pipeline_idx,
            segment_id,
            file_offset,
        };

        if record.slot != 0 {
            self.index.lock().insert(record.slot, loc);
        }

        debug!(
            group_id = self.group_id,
            slot = record.slot,
            pipeline_idx,
            segment_id,
            file_offset,
            "wal record appended and fsynced"
        );

        Ok(loc)
    }

    /// Select pipeline via round-robin (lock-free).
    fn select_pipeline(&self) -> usize {
        if self.pipeline_count == 0 {
            return 0;
        }
        usize::try_from(self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.pipeline_count as u64)
            .expect("pipeline_count exceeds usize")
    }

    /// Rotate the active segment on a pipeline: seal current, open new.
    async fn rotate_pipeline(&self, pipeline: &mut WalPipeline) -> io::Result<()> {
        if let Some(mut old_seg) = pipeline.active_segment.take() {
            old_seg.seal().await?;

            let meta = SegmentMeta {
                segment_id: old_seg.segment_id,
                disk_idx: pipeline.pipeline_idx,
                min_slot: old_seg.min_slot,
                max_slot: old_seg.max_slot,
                record_count: old_seg.record_count,
            };
            self.index.lock().register_segment(meta);

            info!(
                group_id = self.group_id,
                segment_id = old_seg.segment_id,
                min_slot = old_seg.min_slot,
                max_slot = old_seg.max_slot,
                "segment sealed"
            );
        }

        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let seg = WalSegment::create_with_format(
            &self.backend,
            &pipeline.pipeline_path,
            seg_id,
            self.group_id,
            pipeline.record_format,
        )
        .await?;
        pipeline.active_segment = Some(seg);
        Ok(())
    }

    /// Seal all active segments (used during shutdown or forced rotation).
    ///
    /// # Errors
    /// Returns IO error if sealing any segment fails.
    pub async fn seal_all(&self) -> io::Result<()> {
        let mut pipelines = self.pipelines.lock().await;
        for pipeline in pipelines.iter_mut() {
            if let Some(seg) = pipeline.active_segment.as_mut() {
                seg.seal().await?;
                let meta = SegmentMeta {
                    segment_id: seg.segment_id,
                    disk_idx: pipeline.pipeline_idx,
                    min_slot: seg.min_slot,
                    max_slot: seg.max_slot,
                    record_count: seg.record_count,
                };
                self.index.lock().register_segment(meta);
            }
            pipeline.active_segment = None;
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
    pub async fn pipeline_backends(&self) -> Vec<WalPipelineBackend> {
        self.pipelines
            .lock()
            .await
            .iter()
            .map(|pipeline| pipeline.backend.clone())
            .collect()
    }

    /// Pipeline paths (group-level subdirectories).
    pub async fn disk_group_paths(&self) -> Vec<PathBuf> {
        self.pipelines
            .lock()
            .await
            .iter()
            .map(|pipeline| pipeline.pipeline_path.clone())
            .collect()
    }

    /// Set the next segment id (used after replay to resume from the highest seen).
    pub fn set_next_segment_id(&self, id: u64) {
        self.next_segment_id.store(id, Ordering::Release);
    }

    /// Get the failed flag for sharing with fsync workers.
    pub fn failed_flag(&self) -> Arc<AtomicBool> {
        self.failed.clone()
    }
}

fn select_record_format(configured: WalRecordFormat, backend: &WalPipelineBackend) -> WalRecordFormat {
    match configured {
        WalRecordFormat::Auto => match backend {
            WalPipelineBackend::File(_) | WalPipelineBackend::MemBlock(_) => WalRecordFormat::TextLine,
            WalPipelineBackend::Block(_) => WalRecordFormat::Binary,
        },
        explicit => explicit,
    }
}
