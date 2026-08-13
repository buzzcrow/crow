// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use crow_common::metrics::MetricsRegistry;
use crow_diskdb::data_group_client::DataGroupClient;
use crow_diskdb::ddb_config::{validate, DdbConfig};
use crow_diskdb::domain::disk_group_container::DdbDiskGroupContainer;
use crow_diskdb::metrics::DiskdbMetrics;
use crow_diskdb::recovery::compaction::{CompactionConfig, CompactionEngine};
use crow_diskdb::recovery::RecoveryEngine;
use crow_diskdb::service::DiskdbService;
use crow_diskdb::sync::{SyncConfig, SyncLoop};
use crow_kv_client::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient};
use crow_protocol::DiskIdExt;
use tracing::info;

/// CROW diskdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crow-diskdb", about = "CROW distributed disk-block allocator")]
struct Cli {
    /// Config file path (JSON).
    #[arg(long)]
    config: Option<String>,

    /// gRPC listen address (overrides config).
    #[arg(long)]
    listen_addr: Option<String>,

    /// HTTP management listen address (overrides config).
    #[arg(long)]
    http_addr: Option<String>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();
    tracing_subscriber::fmt().init();

    let config = load_config(&args);
    if let Err(e) = validate(&config) {
        panic!("invalid config: {e}");
    }
    info!(?config, "crow-diskdb starting");

    // Generate instance ID if not set.
    let instance_id = config
        .server
        .instance_id
        .as_ref()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(crow_kv_client::new_client_id);

    // Build the in-memory disk-group container.
    let container = Arc::new(DdbDiskGroupContainer::new(instance_id));

    // Build one shared kv-client. The system group (store 0, group 0)
    // leader and data-group leaders are discovered lazily via the
    // kv-server HTTP management API seeds from config — no pre-seeding.
    // One client is shared across all service classes (hardware,
    // service-registry, data-group) since `CrowkvClient` is fully
    // interior-mutable; each service class takes it via `from_shared`.
    let kv_client = Arc::new(CrowkvClient::new(ClientConfig::new(
        config.server.kv_server_mgmt_seeds.clone(),
    )));
    let (sys_store, sys_group) = kv_client.system_group();
    info!(
        store_id = sys_store,
        group_id = sys_group,
        seeds = ?config.server.kv_server_mgmt_seeds,
        "kv-client built; system group leader will be discovered lazily"
    );
    let hw = HardwareClient::from_shared(Arc::clone(&kv_client));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv_client));
    let dg_kv = Arc::new(DataGroupClient::from_shared(Arc::clone(&kv_client)));
    let dg_kv_sync = DataGroupClient::from_shared(Arc::clone(&kv_client));

    // Register diskdb metrics (§11: `zone.allocate.retry.cms.bit`,
    // `disk.bad.impacted_blocks`). The CAS retry counter is attached
    // to each `Zone` during disk-add init so the allocate path can
    // increment it on each failed `cas_bit`.
    let mut metrics_registry = MetricsRegistry::new();
    let metrics = DiskdbMetrics::register(&mut metrics_registry);

    // Start the sync loop.
    let sync_cfg = SyncConfig {
        interval: std::time::Duration::from_secs(u64::from(config.sync.sync_interval_secs)),
        miss_threshold: config.heartbeat.miss_threshold,
        zone_rotate_count: config.storage.zone_rotate_count,
        cas_retry_limit: config.storage.cas_retry_limit,
    };
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let mut sync_loop = SyncLoop::new(hw, svc, container.clone(), sync_cfg)
        .with_data_group_client(dg_kv_sync)
        .with_cas_retry_metric(metrics.allocate_retry_cas_bit);

    // Blocking initial sync — run one tick before serving gRPC to
    // populate the in-memory node/disk/zone state. Without this, the
    // first allocate/free request would find an empty container.
    info!("running blocking initial sync");
    let init_outcome = sync_loop.sync_once().await;
    info!(
        groups_added = init_outcome.groups_added,
        disks_added = init_outcome.disks_added,
        duration_ms = init_outcome.sync_duration_ms,
        "initial sync complete"
    );

    let sync_handle = tokio::spawn(sync_loop.run(stop_rx));

    // R73 recovery — for each owned disk-group, replay the journal
    // from the latest ZoneValue snapshot to reconstruct in-memory
    // zone bitmaps. Runs after the blocking initial sync (which
    // populated the container with empty zones + bind). Zones with
    // existing snapshots are replayed via strategy 2; zones without
    // snapshots fall back to strategy 1 (full scan).
    let recovery_engine = Arc::new(RecoveryEngine::new(
        Arc::clone(&dg_kv),
        config.persistence.recovery_concurrency,
    ));
    info!("running R73 recovery");
    for dg_id in container.disk_group_ids() {
        if let Some(dg) = container.get_disk_group(dg_id) {
            let bind = *dg.bind.read().unwrap();
            let disks: Vec<(
                crow_protocol::common::DiskId,
                crow_protocol::diskdb::rpc::DiskValue,
            )> = {
                let disks_guard = dg.disks.read().unwrap();
                disks_guard
                    .iter()
                    .map(|d| (d.disk_id, *d.disk_value.read().unwrap()))
                    .collect()
            };
            for (disk_id, disk_value) in &disks {
                let zone_count = disk_value.zone_count;
                let zone_size_units = disk_value.zone_size_units;
                for zi in 0..zone_count {
                    #[allow(clippy::cast_possible_truncation)]
                    let unit_capacity = if zi == zone_count - 1 {
                        let remaining = disk_value.capacity_units - (u64::from(zi) * zone_size_units);
                        let rounded = (remaining / 64) * 64;
                        rounded as u32
                    } else {
                        zone_size_units as u32
                    };
                    // Strategy 2 (journal scan replay) with strategy 1
                    // fallback. On any error, log and continue — the
                    // zone is left with its (possibly empty) bitmap
                    // from disk_add_init, which is correct for fresh
                    // disks and conservative for recovered disks.
                    match recovery_engine
                        .rebuild_zone_bitmap_full_scan(bind, *disk_id, zi, unit_capacity)
                        .await
                    {
                        Ok((recovered_zone, stats)) => {
                            // Replace the empty zone from disk_add_init
                            // with the recovered zone.
                            if let Some(disk) = dg
                                .disks
                                .read()
                                .unwrap()
                                .iter()
                                .find(|d| d.disk_id == *disk_id)
                                .cloned()
                            {
                                let mut zones = disk.zones.write().unwrap();
                                if (zi as usize) < zones.len() {
                                    zones[zi as usize] = Arc::new(recovered_zone);
                                }
                            }
                            info!(
                                disk = %disk_id.to_display_string(),
                                zone = zi,
                                used_units = stats.used_units,
                                free_units = stats.free_units,
                                "zone recovered (strategy 1)"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                disk = %disk_id.to_display_string(),
                                zone = zi,
                                error = %e,
                                "zone recovery failed; leaving empty zone"
                            );
                        }
                    }
                }
            }
            // Rebuild active zone sets + allocatable disks after
            // recovery.
            let disks_guard = dg.disks.read().unwrap();
            for disk in disks_guard.iter() {
                disk.rebuild_active_zones(config.storage.zone_rotate_count);
            }
            dg.rebuild_allocating_disks();
        }
    }
    info!("R73 recovery complete");

    // Start the compaction loop (R73 strategy 3).
    let compaction_cfg = CompactionConfig {
        compaction_cadence: std::time::Duration::from_secs(u64::from(
            config.persistence.compaction_cadence_secs,
        )),
        snapshot_compaction_threshold: config.persistence.snapshot_compaction_threshold,
    };
    let compaction_engine = Arc::new(CompactionEngine::new(Arc::clone(&dg_kv), compaction_cfg));
    let compaction_handle = {
        let ce = Arc::clone(&compaction_engine);
        let container = Arc::clone(&container);
        tokio::spawn(async move {
            ce.compaction_loop(container).await;
        })
    };

    // Serve gRPC.
    let listen_addr: SocketAddr = config.server.listen_addr.parse().expect("valid listen_addr");
    let grpc_service = DiskdbService::new(
        container.clone(),
        Arc::clone(&dg_kv),
        config.storage.clone(),
        Arc::clone(&recovery_engine),
    )
    .into_server();
    info!(%listen_addr, "gRPC server listening");
    let grpc_result = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(listen_addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("received shutdown signal");
            let _ = stop_tx.send(());
        })
        .await;

    if let Err(e) = grpc_result {
        tracing::error!("gRPC server error: {e}");
    }
    let _ = sync_handle.await;
    let _ = compaction_handle.await;
    info!("crow-diskdb stopped");
}

fn load_config(args: &Cli) -> DdbConfig {
    let mut config = if let Some(path) = &args.config {
        let p = std::path::Path::new(path);
        DdbConfig::load_from_file(p).unwrap_or_else(|e| {
            panic!("failed to load config file {path}: {e}");
        })
    } else {
        DdbConfig::default()
    };

    if let Some(addr) = &args.listen_addr {
        config.server.listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }

    config
}
