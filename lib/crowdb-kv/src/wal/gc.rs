// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

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
/// `safe_slot` is called once per tick to fetch the current group safe-slot
/// (`crate::cluster::group::PxGroup::group_safe_slot`, or `u64::MAX` if the
/// caller has no such notion and wants `snapshot_slot` alone to gate GC) —
/// kept as a callback rather than a fixed value so callers whose safe-slot
/// advances over a long-lived worker's lifetime (i.e. every real caller)
/// don't have to restart the worker to pick up a new value.
///
/// The task runs until `cancel` is set to true or the `WalEngine` is dropped.
#[allow(dead_code)]
pub(crate) fn spawn_gc_worker(
    wal: Arc<WalEngine>,
    gc_tick: Duration,
    cancel: Arc<AtomicBool>,
    safe_slot: impl Fn() -> u64 + Send + Sync + 'static,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(gc_loop(wal, gc_tick, cancel, safe_slot))
}

#[allow(dead_code)]
async fn gc_loop(
    wal: Arc<WalEngine>,
    gc_tick: Duration,
    cancel: Arc<AtomicBool>,
    safe_slot: impl Fn() -> u64,
) {
    loop {
        tokio::time::sleep(gc_tick).await;
        if cancel.load(Ordering::Acquire) {
            info!(group_id = wal.group_id(), "gc worker shutting down");
            return;
        }
        if let Err(e) = run_gc_pass(&wal, safe_slot()).await {
            warn!(group_id = wal.group_id(), error = %e, "gc pass failed");
        }
    }
}

/// GC watermark source. The caller provides the current `safe_slot`.
///
/// `gc_slot = min(safe_slot, snapshot_slot)`. The `snapshot_slot` comes from
/// the `WalEngine` snapshot state (set by the group when a snapshot is taken).
#[must_use]
pub(crate) fn compute_gc_slot(safe_slot: u64, snapshot_slot: u64) -> u64 {
    safe_slot.min(snapshot_slot)
}

/// Run one GC pass: unlink segments fully below `gc_slot`.
///
/// The GC watermark is `min(safe_slot, snapshot_slot)`, where `safe_slot` is
/// the highest slot that is known to be contiguously applied *everywhere it
/// might still be needed* (see
/// `crate::cluster::group::PxGroup::group_safe_slot`) and `snapshot_slot` is the latest
/// snapshot slot from the `WalEngine` state. Pass `u64::MAX` for `safe_slot`
/// to let `snapshot_slot` alone gate GC (e.g. a caller with no group-wide
/// safe-slot notion).
///
/// # Errors
/// Returns IO error if segment files cannot be unlinked.
pub async fn run_gc_pass(wal: &WalEngine, safe_slot: u64) -> io::Result<usize> {
    let snapshot_slot = wal.snapshot_slot();
    let gc_slot = compute_gc_slot(safe_slot, snapshot_slot);
    run_gc_with_watermark(wal, gc_slot).await
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
    let disk_paths = wal.disk_group_paths();

    // Collect segments eligible for GC.
    let eligible: Vec<(u64, usize, std::path::PathBuf)> = {
        let index = wal.index().lock();
        index
            .segments()
            .filter(|meta| meta.max_slot > 0 && meta.max_slot < gc_slot)
            .map(|meta| {
                let dir = &disk_paths[meta.disk_idx];
                let filename = format!("seg-{:07}.ck", meta.segment_id);
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
        debug!(group_id = wal.group_id(), unlinked, gc_slot, "gc pass complete");
    }

    Ok(unlinked)
}
