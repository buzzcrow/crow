// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use crow_common::metrics::MetricsRegistry;
use crow_diskdb::bg_task::{BgCtx, BgRunner};
use crow_diskdb::ddb_config::{validate, CompactionConfig, DdbConfig, KeepAliveConfig};
use crow_diskdb::ddb_kv_client::DdbKvClient;
use crow_diskdb::health;
use crow_diskdb::liveness::keepalive::KeepAlive;
use crow_diskdb::liveness::lifecycle::StartupPhase;
use crow_diskdb::liveness::notify::NotifyHandler;
use crow_diskdb::metrics::{DiskdbMetrics, RecalcEngine, ReportingTask};
use crow_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crow_diskdb::recovery::compaction::{CompactionEngine, PreparatoryThread};
use crow_diskdb::recovery::ZoneLoader;
use crow_diskdb::scanner::{ScanState, ScannerTask};
use crow_diskdb::service::DiskdbService;
use crow_kv_client::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient, WatchNotifyClient};
use tracing::info;

/// CROW diskdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crow-diskdb", about = "CROW distributed disk-block allocator")]
struct Cli {
    /// Required: config file path (TOML). Example: `conf/crow_diskdb_config.toml`
    #[arg(long)]
    config: String,

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
    // `load_from_file` already validates, but CLI overrides may
    // invalidate — re-validate before proceeding.
    if let Err(e) = validate(&config) {
        panic!("invalid config: {e}");
    }
    let config = Arc::new(arc_swap::ArcSwap::from_pointee(config));
    info!(config = ?*config.load(), "crow-diskdb starting");

    // Spawn a config file watcher for live reload. Dynamic fields
    // (timer intervals, thresholds) are read from the shared handle
    // each tick; static fields log a diff but require restart.
    let watcher_config = Arc::clone(&config);
    let _config_watcher = match crow_common::config::watch::<crow_diskdb::ddb_config::DdbConfig, _>(
        std::path::Path::new(&args.config),
        move |new| {
            let old = (**watcher_config.load()).clone();
            crow_common::config::log_diff(&old, new);
            watcher_config.store(Arc::new(new.clone()));
        },
    ) {
        Ok(w) => Some(w),
        Err(e) => {
            tracing::warn!(error = %e, "config file watcher failed to start; live reload disabled");
            None
        }
    };

    // Generate instance ID if not set.
    let instance_id = config
        .load()
        .server
        .instance_id
        .as_ref()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(crow_kv_client::new_client_id);

    // Build the in-memory disk-group container. Phase = Init.
    let container = Arc::new(DdbDiskGroupContainer::new(instance_id));

    // Build one shared kv-client. The system group (store 0, group 0)
    // leader and data-group leaders are discovered lazily via the
    // kv-server HTTP management API seeds from config — no pre-seeding.
    // One client is shared across all service classes (hardware,
    // service-registry, data-group) since `CrowkvClient` is fully
    // interior-mutable; each service class takes it via `from_shared`.
    let kv_client = Arc::new(CrowkvClient::new(ClientConfig::new(
        config.load().server.kv_server_mgmt_seeds.clone(),
    )));
    let (sys_store, sys_group) = kv_client.system_group();
    info!(
        store_id = sys_store,
        group_id = sys_group,
        seeds = ?config.load().server.kv_server_mgmt_seeds,
        "kv-client built; system group leader will be discovered lazily"
    );
    let hw = HardwareClient::from_shared(Arc::clone(&kv_client));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv_client));
    let dg_kv = Arc::new(DdbKvClient::from_shared(Arc::clone(&kv_client)));
    let dg_kv_sync = DdbKvClient::from_shared(Arc::clone(&kv_client));

    // Register diskdb metrics (§11: `zone.allocate.retry.cms.bit`,
    // `disk.bad.impacted_blocks`). The CAS retry counter is attached
    // to each `Zone` during disk-add init so the allocate path can
    // increment it on each failed `cas_bit`.
    let mut metrics_registry = MetricsRegistry::new();
    let metrics = DiskdbMetrics::register(&mut metrics_registry);

    // Phase = Syncing. Run initial keep-alive tick (blocking) to
    // populate the in-memory disk-group/disk/zone state.
    container.set_lifecycle_phase(StartupPhase::Syncing);
    let keepalive_cfg = KeepAliveConfig {
        interval: std::time::Duration::from_secs(u64::from(config.load().sync.sync_interval_secs)),
        miss_threshold: config.load().heartbeat.miss_threshold,
        zone_rotate_count: config.load().storage.zone_rotate_count,
        cas_retry_limit: config.load().storage.cas_retry_limit,
        temp_failure_timeout_secs: config.load().heartbeat.temp_failure_timeout_secs,
    };
    let notify_enabled = config.load().notify.notify_enabled;
    let sync_trigger = if notify_enabled {
        Some(Arc::new(tokio::sync::Notify::new()))
    } else {
        None
    };
    let keepalive = KeepAlive::new(hw, svc, container.clone(), keepalive_cfg)
        .with_ddb_kv_client(dg_kv_sync)
        .with_cas_retry_metric(Arc::clone(&metrics.allocate_retry_cas_bit))
        .with_config_handle(Arc::clone(&config))
        .with_grpc_endpoint(config.load().server.listen_addr.clone())
        .with_metrics(metrics.clone());
    let keepalive = if let Some(ref trigger) = sync_trigger {
        keepalive.with_sync_trigger(Arc::clone(trigger))
    } else {
        keepalive
    };

    info!("running blocking initial keep-alive tick");
    let init_outcome = keepalive.tick().await;
    info!(
        groups_added = init_outcome.groups_added,
        disks_added = init_outcome.disks_added,
        duration_ms = init_outcome.sync_duration_ms,
        "initial keep-alive tick complete"
    );

    // Build the gRPC service + start serving immediately (before
    // zone loading). Mutating RPCs are gated on lifecycle phase = Up,
    // so allocate/free/rebuild return `unavailable` during loading.
    // Read-only RPCs (GetDiskGroupInfo, GetDiskInfo) are allowed.
    let zone_loader = Arc::new(ZoneLoader::new(
        Arc::clone(&dg_kv),
        config.load().persistence.load_concurrency,
    ));
    let recalc_engine = Arc::new(RecalcEngine::new(Arc::clone(&dg_kv), Arc::clone(&container)));
    let scan_state = ScanState::new();
    let listen_addr: SocketAddr = config
        .load()
        .server
        .listen_addr
        .parse()
        .expect("valid listen_addr");
    let grpc_service = DiskdbService::new(
        container.clone(),
        Arc::clone(&dg_kv),
        config.load().storage.clone(),
        Arc::clone(&zone_loader),
        Arc::clone(&recalc_engine),
        scan_state.clone(),
        Arc::new(metrics.clone()),
    )
    .into_server();
    info!(%listen_addr, "gRPC server listening (zone load pending)");

    // Phase = Loading. Spawn zone loading as a background task — the
    // main task does not block on it. Each disk-group's zones are
    // loaded; when all are done, phase transitions to Up.
    container.set_lifecycle_phase(StartupPhase::Loading);
    let load_container = Arc::clone(&container);
    let load_kv = Arc::clone(&dg_kv);
    let load_cfg = (**config.load()).clone();
    let load_metrics = metrics.clone();
    let load_handle = tokio::spawn(async move {
        run_zone_load(load_kv, load_container, &load_cfg, &load_metrics).await;
    });

    // Build the background-task runner with keepalive + compaction.
    // Both share one stop signal; the runner spawns them and joins on
    // shutdown.
    let compaction_cfg = CompactionConfig {
        compaction_cadence: std::time::Duration::from_secs(u64::from(
            config.load().persistence.compaction_cadence_secs,
        )),
        snapshot_compaction_threshold: config.load().persistence.snapshot_compaction_threshold,
    };
    let compaction_engine = Arc::new(
        CompactionEngine::new(Arc::clone(&dg_kv), compaction_cfg.clone())
            .with_config_handle(Arc::clone(&config)),
    );
    let preparatory_thread = Arc::new(
        PreparatoryThread::new(Arc::clone(&dg_kv), compaction_cfg).with_config_handle(Arc::clone(&config)),
    );
    let keepalive_task: Arc<dyn crow_diskdb::bg_task::BackgroundTask> = Arc::new(keepalive);
    let reporting_task: Arc<dyn crow_diskdb::bg_task::BackgroundTask> =
        Arc::new(ReportingTask::new(metrics.clone(), Arc::clone(&config)));
    let scanner_task: Arc<dyn crow_diskdb::bg_task::BackgroundTask> =
        Arc::new(ScannerTask::new(scan_state, Arc::clone(&config)));
    let runner = BgRunner::new()
        .register(keepalive_task)
        .register(compaction_engine)
        .register(preparatory_thread)
        .register(reporting_task)
        .register(scanner_task);
    let stop = runner.stop_handle();
    let bg_ctx = Arc::new(BgCtx {
        container: Arc::clone(&container),
        kv: Arc::clone(&dg_kv),
        metrics,
        config: Arc::clone(&config),
    });
    let bg_handles = runner.spawn(&bg_ctx);

    // Spawn the watch/notify handler when notify is enabled. It
    // subscribes to group-0 prefixes and wakes the keepalive sync
    // loop on notify (triggering an immediate re-sync of changed
    // keys). The timer remains as a safety-net poller.
    let notify_handle = if notify_enabled {
        let watch_client = WatchNotifyClient::from_shared(Arc::clone(&kv_client));
        let trigger = sync_trigger
            .as_ref()
            .map(Arc::clone)
            .expect("sync_trigger set when notify_enabled");
        let notify_stop = stop.clone();
        Some(tokio::spawn(async move {
            let handler = NotifyHandler::new(watch_client, trigger);
            handler.run(notify_stop).await;
        }))
    } else {
        None
    };

    // Start the HTTP management/health server. Exposes `/ready`
    // (aliased `/health`) returning the current `StartupPhase` +
    // degraded flag so orchestrators can poll readiness during
    // zone loading.
    let http_listen_addr: SocketAddr = config
        .load()
        .server
        .http_listen_addr
        .parse()
        .expect("valid http_listen_addr");
    let http_stop = Arc::new(tokio::sync::Notify::new());
    let http_stop_signal = Arc::clone(&http_stop);
    let http_container = Arc::clone(&container);
    let http_handle = tokio::spawn(async move {
        let app = health::router(http_container);
        info!(%http_listen_addr, "HTTP health server listening");
        let listener = tokio::net::TcpListener::bind(http_listen_addr)
            .await
            .expect("bind http_listen_addr");
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                http_stop_signal.notified().await;
                info!("HTTP health server shutting down");
            })
            .await
            .expect("HTTP health server error");
    });

    // Serve gRPC until shutdown signal.
    let grpc_result = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(listen_addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("received shutdown signal");
            stop.notify_waiters();
            http_stop.notify_waiters();
        })
        .await;

    if let Err(e) = grpc_result {
        tracing::error!("gRPC server error: {e}");
    }
    for h in bg_handles {
        let _ = h.await;
    }
    let _ = http_handle.await;
    let _ = load_handle.await;
    if let Some(h) = notify_handle {
        let _ = h.await;
    }
    info!("crow-diskdb stopped");
}

/// Run R73 zone loading for all owned disk-groups, then set phase to
/// Up. Uses `ZoneLoader::load_disk_group` (strategy 2 journal replay
/// with strategy 1 full-scan fallback) and replaces each disk-group in
/// the container with its loaded counterpart.
async fn run_zone_load(
    kv: Arc<DdbKvClient>,
    container: Arc<DdbDiskGroupContainer>,
    config: &DdbConfig,
    metrics: &DiskdbMetrics,
) {
    info!("running R73 zone load (background)");
    let zone_loader = ZoneLoader::new(kv, config.persistence.load_concurrency);
    for dg_id in container.disk_group_ids() {
        let Some(dg) = container.get_disk_group(dg_id) else {
            continue;
        };
        let bind = *dg.bind.read().unwrap();
        let disks: Vec<(
            crow_protocol::common::DiskId,
            crow_protocol::diskdb::rpc::DiskValue,
        )> = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard
                .iter()
                .map(|d| (d.disk_id, d.disk_value.read().unwrap().clone()))
                .collect()
        };
        let load_start = std::time::Instant::now();
        let loaded = zone_loader
            .load_disk_group(
                dg_id,
                dg.node_id,
                dg.rack_id,
                bind,
                &disks,
                config.storage.zone_rotate_count,
            )
            .await;
        container.replace_disk_group(loaded);
        metrics
            .recovery_duration_ms
            .observe(load_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        info!(dg_id, "disk-group loaded (strategy 2 + strategy 1 fallback)");
    }
    container.set_lifecycle_phase(StartupPhase::Up);
    info!("R73 zone load complete; phase = Up");
}

fn load_config(args: &Cli) -> DdbConfig {
    let config_path = &args.config;
    let mut config = crow_common::config::load_from_file::<DdbConfig>(std::path::Path::new(config_path))
        .unwrap_or_else(|e| {
            panic!("failed to load config file {config_path}: {e}");
        });

    if let Some(addr) = &args.listen_addr {
        config.server.listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }

    config
}
