//! WAL GC worker (P2 W14).
//!
//! Periodically unlinks segments whose records are all below the GC watermark.
//! Segment-granular: a whole segment is unlinked only when every record in it
//! has `slot < gc_slot`. Uses the segment index footer slot ranges.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use super::wal_engine::WalEngine;

/// Spawn the GC worker background task.
///
/// The task runs until `cancel` is set to true or the `WalEngine` is dropped.
pub fn spawn_gc_worker(
    wal: Arc<WalEngine>,
    gc_tick: Duration,
    cancel: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(gc_loop(wal, gc_tick, cancel))
}

async fn gc_loop(wal: Arc<WalEngine>, gc_tick: Duration, cancel: Arc<AtomicBool>) {
    loop {
        tokio::time::sleep(gc_tick).await;
        if cancel.load(Ordering::Acquire) {
            info!(group_id = wal.group_id(), "gc worker shutting down");
            return;
        }
        if let Err(e) = run_gc_pass(&wal).await {
            warn!(group_id = wal.group_id(), error = %e, "gc pass failed");
        }
    }
}

/// GC watermark source. The caller provides the current `safe_slot`.
///
/// `gc_slot = min(safe_slot, snapshot_slot)`.
/// For P2, `snapshot_slot` is `u64::MAX` (stub until P5).
#[must_use]
pub fn compute_gc_slot(safe_slot: u64, snapshot_slot: u64) -> u64 {
    safe_slot.min(snapshot_slot)
}

/// Run one GC pass: unlink segments fully below `gc_slot`.
///
/// # Errors
/// Returns IO error if segment files cannot be unlinked.
pub async fn run_gc_pass(wal: &WalEngine) -> io::Result<usize> {
    // For V1, use u64::MAX as snapshot_slot and a conservative safe_slot.
    // The actual safe_slot integration will come from the group's contiguous_applied.
    // For now, GC is a no-op unless explicitly driven.
    run_gc_with_watermark(wal, 0).await
}

/// Run one GC pass with an explicit `gc_slot` watermark.
///
/// # Errors
/// Returns IO error if segment files cannot be unlinked.
pub async fn run_gc_with_watermark(wal: &WalEngine, gc_slot: u64) -> io::Result<usize> {
    if gc_slot == 0 {
        return Ok(0);
    }

    let backend = wal.backend();
    let mut unlinked = 0usize;
    let disk_paths = wal.disk_group_paths().await;

    // Collect segments eligible for GC.
    let eligible: Vec<(u64, usize, std::path::PathBuf)> = {
        let index = wal.index().lock();
        index
            .segments()
            .filter(|meta| meta.max_slot > 0 && meta.max_slot < gc_slot)
            .map(|meta| {
                let dir = &disk_paths[meta.disk_idx];
                let filename = format!("seg-{:07}.log", meta.segment_id);
                let path = dir.join(filename);
                (meta.segment_id, meta.disk_idx, path)
            })
            .collect()
    };

    // Check min_retention.
    // For V1, skip retention check (it requires file mtime which SimDisk doesn't have).

    for (seg_id, _disk_idx, path) in &eligible {
        match backend.unlink(path).await {
            Ok(()) => {
                wal.index().lock().remove_segment(*seg_id);
                unlinked += 1;
                debug!(
                    group_id = wal.group_id(),
                    segment_id = seg_id,
                    "gc: unlinked segment"
                );
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Already gone — clean up index.
                wal.index().lock().remove_segment(*seg_id);
            }
            Err(e) => {
                warn!(
                    group_id = wal.group_id(),
                    segment_id = seg_id,
                    error = %e,
                    "gc: failed to unlink segment"
                );
            }
        }
    }

    if unlinked > 0 {
        info!(group_id = wal.group_id(), unlinked, gc_slot, "gc pass complete");
    }

    Ok(unlinked)
}
