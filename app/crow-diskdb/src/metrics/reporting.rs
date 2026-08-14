// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! R74 §7 — reporting loop. A `BackgroundTask` that bridges the
//! per-disk hot-path atomic counters (`DiskMetrics`) + bitmap-derived
//! gauges to the crow-common `Gauge`/`Counter` reporting layer on a
//! cadence. Gauges are derived snapshots updated on the reporting
//! interval, not hot-path writes.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::DdbConfig;
use crate::metrics::DiskdbMetrics;

/// Reporting loop — flushes per-disk period counters into the
/// crow-common `allocate_total`/`free_total` counters and recomputes
/// the bitmap-derived gauges each tick.
pub struct ReportingTask {
    metrics: DiskdbMetrics,
    config: Arc<ArcSwap<DdbConfig>>,
}

impl ReportingTask {
    #[must_use]
    pub fn new(metrics: DiskdbMetrics, config: Arc<ArcSwap<DdbConfig>>) -> Self {
        Self { metrics, config }
    }

    /// One reporting tick: swap periods, recompute gauges.
    fn tick(&self, ctx: &BgCtx) {
        let mut total_allocate_count = 0u64;
        let mut total_free_count = 0u64;
        let mut disk_capacity_bytes = 0u64;
        let mut disk_busy_bytes = 0u64;
        let mut disk_free_bytes = 0u64;
        let mut disk_active_zone_count = 0u32;
        let mut disk_total_zone_count = 0u32;
        let mut dg_capacity_bytes = 0u64;
        let mut dg_busy_bytes = 0u64;
        let mut dg_free_bytes = 0u64;

        let dg_ids = ctx.container.disk_group_ids();
        for dg_id in dg_ids {
            if let Some(dg) = ctx.container.get_disk_group(dg_id) {
                let u = dg.aggregate_usage();
                dg_capacity_bytes += u.capacity_bytes;
                dg_busy_bytes += u.busy_bytes;
                dg_free_bytes += u.free_bytes;
                for du in &u.disks {
                    disk_capacity_bytes += du.capacity_bytes;
                    disk_busy_bytes += du.busy_bytes;
                    disk_free_bytes += du.free_bytes;
                    disk_active_zone_count += du.active_zone_count;
                    disk_total_zone_count += du.zone_count;
                    // Flush per-disk period counters into the crow-common totals.
                    if let Some(disk_metrics) = dg.disk_metrics(du.disk_id) {
                        let snap = disk_metrics.swap_periods();
                        total_allocate_count += snap.allocate_count;
                        total_free_count += snap.free_count;
                    }
                }
            }
        }

        // Flush period deltas into the crow-common counters.
        self.metrics.allocate_total.inc_by(total_allocate_count);
        self.metrics.free_total.inc_by(total_free_count);

        // Recompute gauges from the bitmap.
        self.metrics.disk_capacity_bytes.set(disk_capacity_bytes);
        self.metrics.disk_busy_bytes.set(disk_busy_bytes);
        self.metrics.disk_free_bytes.set(disk_free_bytes);
        self.metrics
            .disk_active_zone_count
            .set(u64::from(disk_active_zone_count));
        self.metrics
            .disk_total_zone_count
            .set(u64::from(disk_total_zone_count));
        self.metrics.dg_capacity_bytes.set(dg_capacity_bytes);
        self.metrics.dg_busy_bytes.set(dg_busy_bytes);
        self.metrics.dg_free_bytes.set(dg_free_bytes);
        self.metrics
            .owned_disk_group_count
            .set(ctx.container.disk_group_count() as u64);
        self.metrics.degraded.set(u64::from(ctx.container.is_degraded()));
        self.metrics
            .last_sync_age_secs
            .set(ctx.container.last_sync_age_secs());
    }
}

impl BackgroundTask for ReportingTask {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            self.tick(ctx);
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        let config = Arc::clone(&self.config);
        Trigger::TimerFn(Box::new(move || {
            let secs = config.load().reporting.interval_secs;
            Duration::from_secs(u64::from(secs))
        }))
    }

    fn name(&self) -> &'static str {
        "reporting"
    }
}
