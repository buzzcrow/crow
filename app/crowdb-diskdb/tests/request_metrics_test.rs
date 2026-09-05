// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_common::metrics::{MetricPoint, MetricsRegistry};
use crowdb_diskdb::metrics::{DiskdbMetrics, RequestKind};

#[test]
fn request_metrics_track_success_error_latency_and_inflight() {
    let mut registry = MetricsRegistry::new();
    let metrics = DiskdbMetrics::register(&mut registry);

    let mut success = metrics.requests.start(RequestKind::AllocateBlocks);
    assert_eq!(gauge(&registry, "request.allocate_blocks.inflight.g"), 1);
    success.mark_success();
    drop(success);

    let failure = metrics.requests.start(RequestKind::AllocateBlocks);
    drop(failure);

    assert_eq!(total(&registry, "request.allocate_blocks.lh"), 2);
    assert_eq!(total(&registry, "request.allocate_blocks.errors.c"), 1);
    assert_eq!(gauge(&registry, "request.allocate_blocks.inflight.g"), 0);
}

fn total(registry: &MetricsRegistry, name: &str) -> u64 {
    match registry.snapshot_named(name, 1.0).expect("metric") {
        MetricPoint::Counter { total, .. } | MetricPoint::Histogram { total, .. } => total,
        point => panic!("unexpected metric kind: {}", point.kind()),
    }
}

fn gauge(registry: &MetricsRegistry, name: &str) -> u64 {
    match registry.snapshot_named(name, 1.0).expect("metric") {
        MetricPoint::Gauge { value, .. } => value,
        point => panic!("unexpected metric kind: {}", point.kind()),
    }
}

#[test]
fn every_request_kind_has_its_own_metric_family() {
    let mut registry = MetricsRegistry::new();
    let metrics = DiskdbMetrics::register(&mut registry);
    let kinds = [
        (RequestKind::AllocateBlocks, "allocate_blocks"),
        (RequestKind::FreeBlocks, "free_blocks"),
        (RequestKind::CommitBlocks, "commit_blocks"),
        (RequestKind::QueryCapacityStats, "query_capacity_stats"),
        (RequestKind::GetDiskGroupInfo, "get_disk_group_info"),
        (RequestKind::GetDiskInfo, "get_disk_info"),
        (RequestKind::RebuildZoneBitmap, "rebuild_zone_bitmap"),
        (RequestKind::RecalcDiskUsage, "recalc_disk_usage"),
        (RequestKind::CompactZone, "compact_zone"),
        (RequestKind::TriggerScan, "trigger_scan"),
        (RequestKind::GetScanStatus, "get_scan_status"),
    ];

    for (kind, name) in kinds {
        let mut request = metrics.requests.start(kind);
        request.mark_success();
        drop(request);
        assert!(registry
            .snapshot_named(&format!("request.{name}.lh"), 1.0)
            .is_some());
        assert!(registry
            .snapshot_named(&format!("request.{name}.inflight.g"), 1.0)
            .is_some());
        assert!(registry
            .snapshot_named(&format!("request.{name}.errors.c"), 1.0)
            .is_some());
    }
}
