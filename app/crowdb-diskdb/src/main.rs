// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-diskdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use crowdb_common::metrics::{MetricsRegistry, MetricsRunner};
use crowdb_diskdb::bg_task::{BgCtx, BgRunner};
use crowdb_diskdb::ddb_config::{validate, CompactionConfig, DdbConfig, KeepAliveConfig};
use crowdb_diskdb::ddb_kv_client::DdbKvClient;
use crowdb_diskdb::health;
use crowdb_diskdb::liveness::keepalive::KeepAlive;
use crowdb_diskdb::liveness::lifecycle::StartupPhase;
use crowdb_diskdb::liveness::notify::NotifyHandler;
use crowdb_diskdb::metrics::{DiskdbMetrics, RecalcEngine, ReportingTask};
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crowdb_diskdb::recovery::compaction::{CompactionEngine, PreparatoryThread};
use crowdb_diskdb::recovery::ZoneLoader;
use crowdb_diskdb::scanner::{ScanState, ScannerTask};
use crowdb_diskdb::service::DiskdbRpcService;
use crowdb_kv_client::{
    ClientConfig, CrowdbKvClient, HardwareClient, ServiceRegistryClient, WatchNotifyClient,
};
use tracing::{error, info, warn};

/// CROWDB diskdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crowdb-diskdb", about = "CROWDB distributed disk-block allocator")]
struct Cli {
    /// Required: config file path (TOML). Example: `conf/crowdb_diskdb_config.toml`
    #[arg(long)]
    config: String,

    /// crowdb-rpc listen address (overrides config).
    #[arg(long)]
    listen_addr: Option<String>,

    /// HTTP management listen address (overrides config).
    #[arg(long)]
    http_addr: Option<String>,

    /// crowdb-rpc listener address (overrides config `rpc_listen_addr`).
    #[arg(long)]
    rpc_listen_addr: Option<String>,

    /// Main listener port (overrides the port in config `listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    listen_port: Option<u16>,

    /// HTTP management port (overrides the port in config `http_listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    http_port: Option<u16>,

    /// crowdb-rpc listener port (overrides the port in config `rpc_listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    rpc_port: Option<u16>,

    /// Number of crowdb-rpc I/O worker threads. Default: 2.
    #[arg(long, default_value_t = 2)]
    rpc_workers: u32,

    /// Log directory. Default: "log" (relative to CWD).
    #[arg(long)]
    log_dir: Option<String>,

    /// Log level for both Rust and C++ stacks. Default: "info"
    /// (or derived from `RUST_LOG`).
    #[arg(long)]
    log_level: Option<String>,

    /// Max log file size in MiB before rotation. Default: 30.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB)]
    log_max_file_mb: usize,

    /// Number of rotated log files to keep. Default: 5.
    #[arg(long, default_value_t = crowdb_common::logging::DEFAULT_LOG_MAX_FILES)]
    log_max_files: usize,

    /// Metrics flush interval in seconds. Zero disables metrics logging.
    #[arg(long, default_value_t = 5)]
    metrics_interval: u64,

    /// Also print logs to console (in addition to file logging).
    #[arg(short = 'l', long)]
    log: bool,

    /// Mirror C++ log lines at this level or above to stderr.
    #[arg(long)]
    log_stderr: Option<String>,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();

    let log_dir = args.log_dir.clone().unwrap_or_else(|| "log".to_string());
    let cpp_level = args
        .log_level
        .clone()
        .unwrap_or_else(|| crowdb_common::logging::cpp_level_from_rust_log("info"));

    // Layered logging: INFO+ to rotating file, WARN+ to console.
    // RUST_LOG overrides both sinks for debugging.
    let _log_guards = if args.log {
        crowdb_common::logging::init_file_and_console_logging_split(
            &log_dir,
            "crowdb-diskdb",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
            "warn",
        )
        .expect("failed to initialize crowdb-diskdb logging")
    } else {
        crowdb_common::logging::init_file_logging(
            &log_dir,
            "crowdb-diskdb",
            args.log_max_file_mb,
            args.log_max_files,
            "info",
        )
        .expect("failed to initialize crowdb-diskdb logging")
    };

    // Initialize the crowdb-rpc C++ spdlog logger (connection failures,
    // transport errors). No-op when the build has no spdlog.
    crowdb_rpc_ffi::init_logging(
        &log_dir,
        &cpp_level,
        args.log_max_file_mb,
        args.log_max_files,
        "crowdb-diskdb-rpc",
    );

    if let Some(ref stderr_level) = args.log_stderr {
        crowdb_rpc_ffi::add_log_stderr(stderr_level);
    }

    let config = load_config(&args);
    // `load_from_file` already validates, but CLI overrides may
    // invalidate — re-validate before proceeding.
    if let Err(e) = validate(&config) {
        panic!("invalid config: {e}");
    }
    let config = Arc::new(arc_swap::ArcSwap::from_pointee(config));
    info!(config = ?*config.load(), "crowdb-diskdb starting");

    // Spawn a config file watcher for live reload. Dynamic fields
    // (timer intervals, thresholds) are read from the shared handle
    // each tick; static fields log a diff but require restart.
    let watcher_config = Arc::clone(&config);
    let _config_watcher = match crowdb_common::config::watch::<crowdb_diskdb::ddb_config::DdbConfig, _>(
        std::path::Path::new(&args.config),
        move |new| {
            let old = (**watcher_config.load()).clone();
            crowdb_common::config::log_diff(&old, new);
            watcher_config.store(Arc::new(new.clone()));
        },
    ) {
        Ok(w) => Some(w),
        Err(e) => {
            tracing::warn!(error = %e, "config file watcher failed to start; live reload disabled");
            None
        }
    };

    // Resolve instance ID: config > persisted state file > generate new.
    // The state file ensures the same instance_id survives restart so
    // group-0 ownership-map entries (keyed by instance_id) remain valid.
    let instance_id = resolve_instance_id(&config);

    // Build the in-memory disk-group container. Phase = Init.
    let container = Arc::new(DdbDiskGroupContainer::new(instance_id));

    // Build one shared kv-client. The system group (store 0, group 0)
    // leader and data-group leaders are discovered lazily via the
    // kv-server HTTP management API seeds from config — no pre-seeding.
    // One client is shared across all service classes (hardware,
    // service-registry, data-group) since `CrowdbKvClient` is fully
    // interior-mutable; each service class takes it via `from_shared`.
    let kv_client = Arc::new(CrowdbKvClient::new(ClientConfig::new(
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
    let (metrics, mut metrics_runner) = create_metrics(
        args.metrics_interval,
        &log_dir,
        args.log_max_file_mb,
        args.log_max_files,
    );
    if let Some(runner) = &mut metrics_runner {
        runner.start();
        info!(interval_secs = args.metrics_interval, "metrics runner started");
    }

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
        .with_cas_retry_metric(Arc::clone(&metrics.allocate_retry_cas_bit))
        .with_config_handle(Arc::clone(&config))
        .with_rpc_endpoint(config.load().server.rpc_listen_addr.clone())
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
    let keepalive = keepalive.with_ddb_kv_client(dg_kv_sync);

    // Build the crowdb-rpc service + start serving immediately (before
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
    info!(%listen_addr, "RPC server listening (zone load pending)");

    // Build the crowdb-rpc server. The RpcServer listens on the RPC
    // port and dispatches to DiskdbRpcService handlers.
    let rpc_listen_addr: SocketAddr = config
        .load()
        .server
        .rpc_listen_addr
        .parse()
        .expect("valid rpc_listen_addr");
    let rpc_rt_handle = tokio::runtime::Handle::current();
    let rpc_service = Arc::new(DiskdbRpcService::new(
        container.clone(),
        Arc::clone(&dg_kv),
        config.load().storage.clone(),
        Arc::clone(&zone_loader),
        Arc::clone(&recalc_engine),
        scan_state.clone(),
        Arc::new(metrics.clone()),
        rpc_rt_handle,
    ));
    let rpc_server = Arc::new(crowdb_rpc_ffi::RpcServer::with_engines(None, 1, args.rpc_workers));
    rpc_server
        .listen(
            rpc_listen_addr.ip().to_string().as_str(),
            i32::from(rpc_listen_addr.port()),
        )
        .expect("rpc server listen");
    rpc_service.register_handlers(&rpc_server);
    rpc_server.start();
    info!(%rpc_listen_addr, "crowdb-rpc server listening (R115 migration)");

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
    let keepalive_task: Arc<dyn crowdb_diskdb::bg_task::BackgroundTask> = Arc::new(keepalive);
    let reporting_task: Arc<dyn crowdb_diskdb::bg_task::BackgroundTask> =
        Arc::new(ReportingTask::new(metrics.clone(), Arc::clone(&config)));
    let scanner_task: Arc<dyn crowdb_diskdb::bg_task::BackgroundTask> =
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
    let http_listener = match tokio::net::TcpListener::bind(http_listen_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(%http_listen_addr, error = %e, "HTTP health server bind failed");
            return;
        }
    };
    let http_stop = Arc::new(tokio::sync::Notify::new());
    let http_stop_signal = Arc::clone(&http_stop);
    let http_container = Arc::clone(&container);
    let http_handle = tokio::spawn(async move {
        let app = health::router(http_container);
        info!(%http_listen_addr, "HTTP health server listening");
        axum::serve(http_listener, app)
            .with_graceful_shutdown(async move {
                http_stop_signal.notified().await;
                info!("HTTP health server shutting down");
            })
            .await
            .expect("HTTP health server error");
    });

    // Wait for shutdown signal.
    let rpc_server_stop = Arc::clone(&rpc_server);
    shutdown_signal().await;
    info!("received shutdown signal");
    rpc_server_stop.stop();
    stop.notify_waiters();
    http_stop.notify_waiters();
    for h in bg_handles {
        let _ = h.await;
    }
    let _ = http_handle.await;
    let _ = load_handle.await;
    if let Some(h) = notify_handle {
        let _ = h.await;
    }
    if let Some(runner) = &mut metrics_runner {
        runner.stop().await;
        info!("metrics runner stopped");
    }
    info!("crowdb-diskdb stopped");
}

async fn shutdown_signal() {
    let mut term_stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            let _ = result;
        }
        _ = term_stream.recv() => {}
    }
}

fn create_metrics(
    interval_secs: u64,
    log_dir: &str,
    max_file_mb: usize,
    max_files: usize,
) -> (DiskdbMetrics, Option<MetricsRunner>) {
    if interval_secs == 0 {
        let mut registry = MetricsRegistry::new();
        return (DiskdbMetrics::register(&mut registry), None);
    }
    let file = crowdb_common::logging::open_metrics_log(log_dir, "crowdb-diskdb", max_file_mb, max_files)
        .expect("failed to open metrics log file");
    let runner = MetricsRunner::new(file, interval_secs);
    let metrics = {
        let mut registry = runner
            .registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        DiskdbMetrics::register(&mut registry)
    };
    (metrics, Some(runner))
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
        let bind = dg.bind();
        let group_status = dg.status();
        let disks: Vec<(
            crowdb_protocol::common::DiskId,
            crowdb_protocol::diskdb::rpc::DiskValue,
        )> = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard
                .iter()
                .map(|d| (d.disk_id, d.disk_value.clone()))
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
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(error) => {
                tracing::error!(dg_id, %error, "disk-group recovery failed; instance remains non-writable");
                container.enter_degraded_mode();
                return;
            }
        };
        loaded.set_status(group_status);
        loaded.rebuild_allocating_disks();
        if !container.replace_disk_group_if_current(&dg, bind, loaded) {
            info!(dg_id, "discarding stale disk-group load result");
            continue;
        }
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
    let mut config = crowdb_common::config::load_from_file::<DdbConfig>(std::path::Path::new(config_path))
        .unwrap_or_else(|e| {
            panic!("failed to load config file {config_path}: {e}");
        });

    if let Some(addr) = &args.listen_addr {
        config.server.listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.rpc_listen_addr {
        config.server.rpc_listen_addr.clone_from(addr);
    }
    if let Some(port) = args.listen_port {
        config.server.listen_addr = replace_port(&config.server.listen_addr, port);
    }
    if let Some(port) = args.http_port {
        config.server.http_listen_addr = replace_port(&config.server.http_listen_addr, port);
    }
    if let Some(port) = args.rpc_port {
        config.server.rpc_listen_addr = replace_port(&config.server.rpc_listen_addr, port);
    }

    config
}

/// Replace the port portion of a `host:port` address string.
fn replace_port(addr: &str, port: u16) -> String {
    if let Some(idx) = addr.rfind(':') {
        format!("{}:{port}", &addr[..idx])
    } else {
        format!("0.0.0.0:{port}")
    }
}

/// Resolve the diskdb instance ID.
///
/// Precedence: config `server.instance_id` > persisted state file
/// (`state/instance_id` relative to CWD) > newly generated.
///
/// When generating a new ID, it is persisted to `state/instance_id` so
/// subsequent restarts reuse it. This is critical because group-0
/// ownership-map entries (`/hw/dg_owner/...`) are keyed by
/// `instance_id` — a new ID on every restart would cause the diskdb
/// to lose ownership of all its disk-groups after restart.
fn resolve_instance_id(config: &arc_swap::ArcSwap<DdbConfig>) -> u64 {
    // 1. Config takes precedence (operator-set, never overwritten).
    if let Some(id) = config
        .load()
        .server
        .instance_id
        .as_ref()
        .and_then(|s| s.parse().ok())
    {
        info!(instance_id = id, source = "config", "instance ID resolved");
        return id;
    }

    // 2. Try the persisted state file.
    let state_dir = std::path::Path::new("state");
    let state_file = state_dir.join("instance_id");
    if let Ok(content) = std::fs::read_to_string(&state_file) {
        if let Ok(id) = content.trim().parse::<u64>() {
            info!(instance_id = id, source = "state file", "instance ID resolved");
            return id;
        }
    }

    // 3. Generate new and persist.
    let id = crowdb_kv_client::new_client_id();
    if let Err(e) = std::fs::create_dir_all(state_dir) {
        warn!(error = %e, "failed to create state dir; instance_id will not persist across restarts");
    } else if let Err(e) = std::fs::write(&state_file, id.to_string()) {
        warn!(error = %e, "failed to write state/instance_id; instance_id will not persist across restarts");
    }
    info!(
        instance_id = id,
        source = "generated",
        "instance ID resolved and persisted"
    );
    id
}
