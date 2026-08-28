// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Scanner background task + shared state. Implements `BackgroundTask`
//! so it runs on the shared `BgRunner` with the same stop signal as
//! compaction + keepalive. The `ScanState` is shared between the task
//! and the crow-rpc service handlers (`TriggerScan` / `GetScanStatus`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crow_protocol::common::DiskId;

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::{DdbConfig, ScannerConfig};
use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::zone::DdbZone;
use crate::scanner::ghost::{scan_ghosts, GhostScanResult};
use crate::scanner::integrity::{scan_integrity, IntegrityScanResult};
use crate::scanner::leak::scan_for_leaks;

/// Per-disk zone list: `(zone_index, unit_capacity, zone_arc)`.
type ZoneList = Vec<(u32, u32, Arc<DdbZone>)>;

/// Why a zone's journal replay fell back to strategy 1 (full scan).
/// Mirrors `recalc::FallbackReason` but lives in the scanner module
/// so the ghost scan can reference it without a cross-module dep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackReason {
    JournalScanGcGap,
    SnapshotCrcFail,
}

/// Summary of one scan cycle — stored in `ScanState` and returned by
/// the admin RPCs.
#[derive(Debug, Clone, Default)]
pub struct ScanSummary {
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub zones_scanned: u64,
    pub zones_skipped_active: u64,
    pub zones_skipped_compacting: u64,
    pub ghost_busy: u64,
    pub ghost_free: u64,
    pub uncompacted_lag: u64,
    pub corrupt_snapshots: u64,
    pub corrupt_records: u64,
    pub owner_mismatches: u64,
    pub leak_status: String,
}

impl ScanSummary {
    /// Total drift = real ghost-busy + ghost-free (excludes normal
    /// uncompacted, which is not drift).
    #[must_use]
    pub fn drift_total(&self) -> u64 {
        self.ghost_busy + self.ghost_free
    }

    /// Total corrupt = snapshots + records.
    #[must_use]
    pub fn corrupt_total(&self) -> u64 {
        self.corrupt_snapshots + self.corrupt_records
    }
}

/// Shared scanner state — holds the last scan summary + the
/// `TriggerScan` request flag. Cheap to clone (all fields are `Arc`).
#[derive(Clone)]
pub struct ScanState {
    /// Last completed scan summary.
    last: Arc<RwLock<Option<ScanSummary>>>,
    /// Set by `TriggerScan` — checked at the start of the next
    /// `run_cycle`. Cleared when the cycle starts.
    scan_requested: Arc<AtomicBool>,
    /// `true` while a scan is in progress (prevents overlap).
    in_progress: Arc<AtomicBool>,
}

impl ScanState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            last: Arc::new(RwLock::new(None)),
            scan_requested: Arc::new(AtomicBool::new(false)),
            in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Request an immediate scan on the next tick. Returns the
    /// current `in_progress` flag so the caller can report it.
    pub fn request_scan(&self) -> bool {
        self.scan_requested.store(true, Ordering::Release);
        self.in_progress.load(Ordering::Acquire)
    }

    /// Whether a scan has been requested (and not yet consumed).
    pub fn is_scan_requested(&self) -> bool {
        self.scan_requested.load(Ordering::Acquire)
    }

    /// Consume the scan-requested flag (called at the start of a
    /// cycle).
    fn consume_scan_request(&self) -> bool {
        self.scan_requested.swap(false, Ordering::AcqRel)
    }

    /// Whether a scan is currently in progress.
    pub fn is_in_progress(&self) -> bool {
        self.in_progress.load(Ordering::Acquire)
    }

    /// Read the last summary (cloned). `None` if no scan has run.
    #[must_use]
    pub fn last_summary(&self) -> Option<ScanSummary> {
        self.last.read().unwrap().clone()
    }

    /// Record a completed scan summary.
    fn record_summary(&self, summary: ScanSummary) {
        *self.last.write().unwrap() = Some(summary);
    }

    /// Test-only setter to inject a summary.
    #[cfg(feature = "test-util")]
    pub fn record_summary_for_tests(&self, summary: ScanSummary) {
        self.record_summary(summary);
    }
}

impl Default for ScanState {
    fn default() -> Self {
        Self::new()
    }
}

/// The scanner background task. Runs on `BgRunner` with a `TimerFn`
/// trigger reading `scanner.scan_interval_secs` from the shared
/// config handle.
pub struct ScannerTask {
    state: ScanState,
    config: Arc<ArcSwap<DdbConfig>>,
}

impl ScannerTask {
    #[must_use]
    pub fn new(state: ScanState, config: Arc<ArcSwap<DdbConfig>>) -> Self {
        Self { state, config }
    }

    /// Access the shared `ScanState` (for wiring into the crow-rpc
    /// service handlers).
    #[must_use]
    pub fn state(&self) -> ScanState {
        self.state.clone()
    }

    /// Run one scan cycle across all owned disk-groups.
    async fn run_scan(&self, ctx: &BgCtx) -> ScanSummary {
        let started = Instant::now();
        let started_at_ms = now_ms();
        let config = ctx.config.load();
        let scanner_cfg = &config.scanner;

        let mut total_ghost = GhostScanResult::default();
        let mut total_integrity = IntegrityScanResult::default();
        let mut zones_scanned: u64 = 0;
        let mut zones_skipped_active: u64 = 0;
        let mut zones_skipped_compacting: u64 = 0;

        for dg_id in ctx.container.disk_group_ids() {
            let Some(dg) = ctx.container.get_disk_group(dg_id) else {
                continue;
            };
            let bind = *dg.bind.read().unwrap();
            let disks_snapshot = snapshot_disk_zones(&dg);

            for (disk_id, zones) in disks_snapshot {
                let active_zones = collect_active_zones(&dg, disk_id);
                scan_one_disk(
                    &ctx.kv,
                    bind,
                    disk_id,
                    &zones,
                    &active_zones,
                    scanner_cfg,
                    &mut total_ghost,
                    &mut total_integrity,
                    &mut zones_scanned,
                    &mut zones_skipped_active,
                    &mut zones_skipped_compacting,
                )
                .await;
            }
        }

        let total_leak = scan_for_leaks().await;
        let duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        let summary = ScanSummary {
            started_at_ms,
            duration_ms,
            zones_scanned,
            zones_skipped_active,
            zones_skipped_compacting,
            ghost_busy: total_ghost.ghost_busy,
            ghost_free: total_ghost.ghost_free,
            uncompacted_lag: total_ghost.uncompacted_lag,
            corrupt_snapshots: total_integrity.corrupt_snapshots,
            corrupt_records: total_integrity.corrupt_records,
            owner_mismatches: total_integrity.owner_mismatches,
            leak_status: total_leak.status.to_string(),
        };

        update_metrics(ctx, &summary);
        tracing::info!(
            zones_scanned = summary.zones_scanned,
            ghost_busy = summary.ghost_busy,
            ghost_free = summary.ghost_free,
            uncompacted_lag = summary.uncompacted_lag,
            corrupt_snapshots = summary.corrupt_snapshots,
            corrupt_records = summary.corrupt_records,
            owner_mismatches = summary.owner_mismatches,
            duration_ms = summary.duration_ms,
            "scanner cycle complete"
        );

        self.state.record_summary(summary.clone());
        summary
    }
}

/// Snapshot all disks + zones in a disk-group for the scan loop.
fn snapshot_disk_zones(dg: &crate::model::disk_group::DdbDiskGroup) -> Vec<(DiskId, ZoneList)> {
    let disks = dg.disks.read().unwrap();
    disks
        .iter()
        .map(|d| {
            let zones = d.zones.read().unwrap();
            let zone_info: ZoneList = zones
                .iter()
                .map(|z| (z.zone_index, z.unit_capacity, Arc::clone(z)))
                .collect();
            (d.disk_id, zone_info)
        })
        .collect()
}

/// Collect `Arc<DdbZone>` clones for the active zone set of a disk.
/// The `Arc`s keep the zones alive for the scan duration (the active
/// set is RCU-published via `Arc` swap).
fn collect_active_zones(dg: &crate::model::disk_group::DdbDiskGroup, disk_id: DiskId) -> Vec<Arc<DdbZone>> {
    let disks = dg.disks.read().unwrap();
    let Some(disk) = disks.iter().find(|d| d.disk_id == disk_id) else {
        return Vec::new();
    };
    let active = disk.active_zone_context.read().unwrap();
    active.iter().cloned().collect()
}

/// Run ghost + integrity scans on one disk's zones, accumulating
/// results into the running totals.
#[allow(clippy::too_many_arguments)]
async fn scan_one_disk(
    kv: &Arc<DdbKvClient>,
    bind: Bind,
    disk_id: DiskId,
    zones: &ZoneList,
    active_zones: &[Arc<DdbZone>],
    scanner_cfg: &ScannerConfig,
    total_ghost: &mut GhostScanResult,
    total_integrity: &mut IntegrityScanResult,
    zones_scanned: &mut u64,
    zones_skipped_active: &mut u64,
    zones_skipped_compacting: &mut u64,
) {
    if scanner_cfg.ghost.detect {
        let r = scan_ghosts(
            kv,
            bind,
            disk_id,
            zones,
            active_zones,
            scanner_cfg.ghost.auto_correct,
            scanner_cfg.reverify_delay_ms,
        )
        .await;
        total_ghost.ghost_busy += r.ghost_busy;
        total_ghost.ghost_free += r.ghost_free;
        total_ghost.uncompacted_lag += r.uncompacted_lag;
        *zones_skipped_active += r.skipped_active;
        *zones_skipped_compacting += r.skipped_compacting;
        *zones_scanned += zones.len() as u64 - r.skipped_active - r.skipped_compacting;
    }

    if scanner_cfg.integrity.verify {
        let r = scan_integrity(
            kv,
            bind,
            disk_id,
            zones,
            active_zones,
            scanner_cfg.integrity.detect_owner_mismatch,
        )
        .await;
        total_integrity.corrupt_snapshots += r.corrupt_snapshots;
        total_integrity.corrupt_records += r.corrupt_records;
        total_integrity.owner_mismatches += r.owner_mismatches;
    }
}

/// Update scanner metrics after a cycle.
fn update_metrics(ctx: &BgCtx, summary: &ScanSummary) {
    ctx.metrics.scanner_runs_total.inc();
    ctx.metrics.scanner_duration_ms.observe(summary.duration_ms);
    ctx.metrics.scanner_ghosts_found.set(summary.drift_total());
    ctx.metrics.scanner_drift_found.set(summary.drift_total());
    ctx.metrics.scanner_corrupt_records.set(summary.corrupt_total());
}

impl BackgroundTask for ScannerTask {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            let _requested = self.state.consume_scan_request();
            if self.state.is_in_progress() {
                return Ok(());
            }
            self.state.in_progress.store(true, Ordering::Release);
            let _ = self.run_scan(ctx).await;
            self.state.in_progress.store(false, Ordering::Release);
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        let config = Arc::clone(&self.config);
        Trigger::TimerFn(Box::new(move || {
            let secs = config.load().scanner.scan_interval_secs;
            Duration::from_secs(u64::from(secs))
        }))
    }

    fn name(&self) -> &'static str {
        "scanner"
    }
}

/// Current epoch time in milliseconds.
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().try_into().unwrap_or(u64::MAX))
}
