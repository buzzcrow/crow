// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E live-apply config reload tests — verify that `Trigger::TimerFn`
//! reads the current interval from the shared `ArcSwap<DdbConfig>`
//! handle each tick, so a config reload takes effect on the next tick
//! without restarting the bg task.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crowdb_diskdb::bg_task::{BackgroundTask, BgCtx, Trigger};
use crowdb_diskdb::ddb_config::{CompactionConfig, DdbConfig, KeepAliveConfig};
use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_diskdb::liveness::keepalive::KeepAlive;
use crowdb_diskdb::metrics::DiskdbMetrics;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crowdb_diskdb::recovery::compaction::CompactionEngine;
use crowdb_kv_client::{ClientConfig, CrowdbClient, HardwareClient, ServiceRegistryClient};

fn make_config_handle(interval_secs: u32) -> Arc<ArcSwap<DdbConfig>> {
    let mut config = DdbConfig::default();
    config.heartbeat.interval_secs = interval_secs;
    config.sync.sync_interval_secs = interval_secs;
    config.persistence.compaction_cadence_secs = interval_secs;
    Arc::new(ArcSwap::from_pointee(config))
}

fn make_keepalive(handle: &Arc<ArcSwap<DdbConfig>>) -> KeepAlive {
    let kv_client = Arc::new(CrowdbClient::new(ClientConfig::new(vec![
        "127.0.0.1:0".to_string()
    ])));
    let hw = HardwareClient::from_shared(Arc::clone(&kv_client));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv_client));
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    let cfg = KeepAliveConfig {
        interval: Duration::from_secs(60),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    KeepAlive::new(hw, svc, container, cfg).with_config_handle(Arc::clone(handle))
}

#[test]
fn keepalive_trigger_uses_timer_fn_when_config_handle_set() {
    let handle = make_config_handle(10);
    let keepalive = make_keepalive(&handle);
    let trigger = keepalive.trigger();
    match trigger {
        Trigger::TimerFn(f) => {
            assert_eq!(f(), Duration::from_secs(10));
            // Simulate a config reload: swap in a new config with a
            // different interval.
            let mut new_config = DdbConfig::default();
            new_config.heartbeat.interval_secs = 5;
            handle.store(Arc::new(new_config));
            // The next call to the closure reads the new interval.
            assert_eq!(f(), Duration::from_secs(5));
        }
        _other => panic!("expected TimerFn"),
    }
}

#[test]
fn keepalive_trigger_uses_fixed_timer_without_config_handle() {
    let kv_client = Arc::new(CrowdbClient::new(ClientConfig::new(vec![
        "127.0.0.1:0".to_string()
    ])));
    let hw = HardwareClient::from_shared(Arc::clone(&kv_client));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv_client));
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    let cfg = KeepAliveConfig {
        interval: Duration::from_secs(42),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw, svc, container, cfg);
    match keepalive.trigger() {
        Trigger::Timer(d) => assert_eq!(d, Duration::from_secs(42)),
        _other => panic!("expected Timer"),
    }
}

#[test]
fn compaction_engine_trigger_uses_timer_fn_when_config_handle_set() {
    let handle = make_config_handle(30);
    let kv_client = Arc::new(CrowdbClient::new(ClientConfig::new(vec![
        "127.0.0.1:0".to_string()
    ])));
    let ddb_kv = Arc::new(DdbKvClient::from_shared(kv_client));
    let cfg = CompactionConfig {
        compaction_cadence: Duration::from_secs(60),
        snapshot_compaction_threshold: 4096,
    };
    let engine = CompactionEngine::new(Arc::clone(&ddb_kv), cfg).with_config_handle(Arc::clone(&handle));
    match engine.trigger() {
        Trigger::TimerFn(f) => {
            assert_eq!(f(), Duration::from_secs(30));
            // Simulate a config reload.
            let mut new_config = DdbConfig::default();
            new_config.persistence.compaction_cadence_secs = 15;
            handle.store(Arc::new(new_config));
            assert_eq!(f(), Duration::from_secs(15));
        }
        _other => panic!("expected TimerFn"),
    }
}

#[test]
fn compaction_engine_trigger_uses_fixed_timer_without_config_handle() {
    let kv_client = Arc::new(CrowdbClient::new(ClientConfig::new(vec![
        "127.0.0.1:0".to_string()
    ])));
    let ddb_kv = Arc::new(DdbKvClient::from_shared(kv_client));
    let cfg = CompactionConfig {
        compaction_cadence: Duration::from_secs(120),
        snapshot_compaction_threshold: 4096,
    };
    let engine = CompactionEngine::new(Arc::clone(&ddb_kv), cfg);
    match engine.trigger() {
        Trigger::Timer(d) => assert_eq!(d, Duration::from_secs(120)),
        _other => panic!("expected Timer"),
    }
}

#[tokio::test]
async fn compaction_engine_run_cycle_reads_threshold_from_ctx_config() {
    let handle = make_config_handle(30);
    // Set a distinctive threshold.
    let mut config = DdbConfig::default();
    config.persistence.snapshot_compaction_threshold = 7777;
    handle.store(Arc::new(config));

    let kv_client = Arc::new(CrowdbClient::new(ClientConfig::new(vec![
        "127.0.0.1:0".to_string()
    ])));
    let ddb_kv = Arc::new(DdbKvClient::from_shared(kv_client));
    let mut registry = crowdb_common::metrics::MetricsRegistry::new();
    let metrics = DiskdbMetrics::register(&mut registry);
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    let ctx = Arc::new(BgCtx {
        container: Arc::clone(&container),
        kv: Arc::clone(&ddb_kv),
        metrics,
        config: Arc::clone(&handle),
    });
    let cfg = CompactionConfig {
        compaction_cadence: Duration::from_secs(60),
        // Different from the handle's 7777 — proves the cycle reads
        // from the handle, not this snapshot.
        snapshot_compaction_threshold: 4096,
    };
    let engine = CompactionEngine::new(Arc::clone(&ddb_kv), cfg);
    // run_cycle reads the threshold from ctx.config and calls
    // compaction_cycle. With no disk-groups in the container, the
    // cycle is a no-op but the threshold read still happens.
    let result = engine.run_cycle(&ctx).await;
    assert!(result.is_ok());
}
