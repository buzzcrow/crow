// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb metrics — registered in the `crow-common` `MetricsRegistry`.
//!
//! R72 counters (kept): `zone.allocate.retry.cms.bit`,
//! `disk.bad.impacted_blocks`. R74 §11 extends the set with gauges
//! (derived from the bitmap on the reporting tick), counters
//! (allocate/free totals + errors + sync/compaction), and the latency
//! hierarchy (histograms for hot paths, summaries for cold paths).

pub mod disk;
pub mod recalc;
pub mod reporting;

use std::sync::Arc;

use crow_common::metrics::{Counter, Gauge, LatencyHistogram, LatencySummary, MetricsRegistry};

pub use disk::{DiskMetrics, PeriodSnapshot};
pub use recalc::{DiskGroupRecalcResult, FallbackReason, RecalcEngine, RecalcResult};
pub use reporting::ReportingTask;

/// Handles for the diskdb metric set (R72 counters + R74 §11 set).
#[derive(Clone)]
pub struct DiskdbMetrics {
    // ── R72 counters ──────────────────────────────────────────────
    /// `zone.allocate.retry.cms.bit` — CAS retry counter.
    pub allocate_retry_cas_bit: Arc<Counter>,
    /// `disk.bad.impacted_blocks` — bad-disk impacted block gauge
    /// (absolute count of busy blocks on bad disks, updated by the
    /// recovery scan task).
    pub disk_bad_impacted_blocks: Arc<Gauge>,

    // ── R74 §11 gauges (derived from the bitmap on the reporting tick) ──
    pub disk_capacity_bytes: Arc<Gauge>,
    pub disk_busy_bytes: Arc<Gauge>,
    pub disk_free_bytes: Arc<Gauge>,
    pub disk_active_zone_count: Arc<Gauge>,
    pub disk_total_zone_count: Arc<Gauge>,
    pub dg_capacity_bytes: Arc<Gauge>,
    pub dg_busy_bytes: Arc<Gauge>,
    pub dg_free_bytes: Arc<Gauge>,
    pub owned_disk_group_count: Arc<Gauge>,
    pub degraded: Arc<Gauge>,
    pub last_sync_age_secs: Arc<Gauge>,

    // ── R74 §11 counters ──────────────────────────────────────────
    pub allocate_total: Arc<Counter>,
    pub free_total: Arc<Counter>,
    pub allocate_errors_total: Arc<Counter>,
    pub sync_success_total: Arc<Counter>,
    pub sync_failure_total: Arc<Counter>,
    pub compaction_records_deleted_total: Arc<Counter>,

    // ── R74 §11 latency histograms (hot paths) ────────────────────
    pub allocate_rpc_latency: Arc<LatencyHistogram>,
    pub allocate_bitmap_scan_latency: Arc<LatencyHistogram>,
    pub allocate_kv_persist_latency: Arc<LatencyHistogram>,
    pub free_rpc_latency: Arc<LatencyHistogram>,
    pub free_bitmap_clear_latency: Arc<LatencyHistogram>,
    pub free_kv_persist_latency: Arc<LatencyHistogram>,

    // ── R74 §11 latency summaries (cold paths) ───────────────────
    pub allocate_zone_rotate_latency: Arc<LatencySummary>,
    pub sync_latency: Arc<LatencySummary>,
    pub sync_read_group0_latency: Arc<LatencySummary>,
    pub sync_apply_changes_latency: Arc<LatencySummary>,
    pub compaction_latency: Arc<LatencySummary>,
    pub compaction_scan_free_latency: Arc<LatencySummary>,
    pub compaction_merge_bitmap_latency: Arc<LatencySummary>,
    pub compaction_kv_persist_latency: Arc<LatencySummary>,
    pub sync_duration_ms: Arc<LatencySummary>,
    pub recovery_duration_ms: Arc<LatencySummary>,

    // ── Scanner metrics (background scanner) ──────────────────────
    pub scanner_runs_total: Arc<Counter>,
    pub scanner_duration_ms: Arc<LatencySummary>,
    pub scanner_ghosts_found: Arc<Gauge>,
    pub scanner_drift_found: Arc<Gauge>,
    pub scanner_corrupt_records: Arc<Gauge>,
}

impl DiskdbMetrics {
    /// Register the full metric set (R72 + R74 §11) in the given
    /// registry.
    pub fn register(registry: &mut MetricsRegistry) -> Self {
        Self {
            // R72 counters.
            allocate_retry_cas_bit: registry.register_counter("zone.allocate.retry.cms.bit"),
            disk_bad_impacted_blocks: registry.register_gauge("disk.bad.impacted_blocks"),
            // R74 gauges.
            disk_capacity_bytes: registry.register_gauge("disk.capacity_bytes"),
            disk_busy_bytes: registry.register_gauge("disk.busy_bytes"),
            disk_free_bytes: registry.register_gauge("disk.free_bytes"),
            disk_active_zone_count: registry.register_gauge("disk.active_zone_count"),
            disk_total_zone_count: registry.register_gauge("disk.total_zone_count"),
            dg_capacity_bytes: registry.register_gauge("dg.capacity_bytes"),
            dg_busy_bytes: registry.register_gauge("dg.busy_bytes"),
            dg_free_bytes: registry.register_gauge("dg.free_bytes"),
            owned_disk_group_count: registry.register_gauge("owned_disk_group_count"),
            degraded: registry.register_gauge("degraded"),
            last_sync_age_secs: registry.register_gauge("last_sync_age_secs"),
            // R74 counters.
            allocate_total: registry.register_counter("allocate.total"),
            free_total: registry.register_counter("free.total"),
            allocate_errors_total: registry.register_counter("allocate.errors.total"),
            sync_success_total: registry.register_counter("sync.success.total"),
            sync_failure_total: registry.register_counter("sync.failure.total"),
            compaction_records_deleted_total: registry.register_counter("compaction.records_deleted.total"),
            // R74 latency histograms (hot paths).
            allocate_rpc_latency: registry.register_histogram("allocate.rpc.latency_us"),
            allocate_bitmap_scan_latency: registry.register_histogram("allocate.bitmap_scan.latency_us"),
            allocate_kv_persist_latency: registry.register_histogram("allocate.kv_persist.latency_us"),
            free_rpc_latency: registry.register_histogram("free.rpc.latency_us"),
            free_bitmap_clear_latency: registry.register_histogram("free.bitmap_clear.latency_us"),
            free_kv_persist_latency: registry.register_histogram("free.kv_persist.latency_us"),
            // R74 latency summaries (cold paths).
            allocate_zone_rotate_latency: registry.register_summary("allocate.zone_rotate.latency_us"),
            sync_latency: registry.register_summary("sync.latency_us"),
            sync_read_group0_latency: registry.register_summary("sync.read_group0.latency_us"),
            sync_apply_changes_latency: registry.register_summary("sync.apply_changes.latency_us"),
            compaction_latency: registry.register_summary("compaction.latency_us"),
            compaction_scan_free_latency: registry.register_summary("compaction.scan_free.latency_us"),
            compaction_merge_bitmap_latency: registry.register_summary("compaction.merge_bitmap.latency_us"),
            compaction_kv_persist_latency: registry.register_summary("compaction.kv_persist.latency_us"),
            sync_duration_ms: registry.register_summary("sync_duration_ms"),
            recovery_duration_ms: registry.register_summary("recovery_duration_ms"),
            // Scanner metrics.
            scanner_runs_total: registry.register_counter("scanner.runs.total"),
            scanner_duration_ms: registry.register_summary("scanner.duration_ms"),
            scanner_ghosts_found: registry.register_gauge("scanner.ghosts_found"),
            scanner_drift_found: registry.register_gauge("scanner.drift_found"),
            scanner_corrupt_records: registry.register_gauge("scanner.corrupt_records"),
        }
    }

    /// Create a no-op set of metrics (for tests that don't have a
    /// registry).
    #[must_use]
    pub fn disabled() -> Self {
        let mut registry = MetricsRegistry::new();
        Self::register(&mut registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_creates_all_handles() {
        let mut reg = MetricsRegistry::new();
        let m = DiskdbMetrics::register(&mut reg);
        // Touch a few handles to confirm they're usable.
        m.allocate_total.inc();
        m.disk_busy_bytes.set(42);
        m.allocate_rpc_latency.observe(1_000);
        m.sync_latency.observe(5_000);
        assert_eq!(m.allocate_total.snapshot().count, 1);
        assert_eq!(m.disk_busy_bytes.snapshot(), 42);
    }

    #[test]
    fn disabled_is_usable() {
        let m = DiskdbMetrics::disabled();
        m.free_total.inc_by(3);
        assert_eq!(m.free_total.snapshot().count, 3);
    }
}
