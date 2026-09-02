// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-chunkdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crowdb_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crowdb_chunkdb::chunkdb_config::ChunkdbConfig;
use crowdb_chunkdb::lifecycle::{ChunkLockMap, LifecycleHandler};
use crowdb_chunkdb::metrics::LifecycleMetrics;
use crowdb_chunkdb::range_guard::RangeGuard;
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::service::ChunkdbRpcService;
use crowdb_chunkdb::storage::ChunkStore;
use crowdb_chunkdb::topology::{notify::NotifyHandler, refresh::run_refresh_loop, TopologyCache};
use crowdb_kv_client::{
    ClientConfig, CrowdbKvClient, HardwareClient, RangeBindingClient, ServiceRegistryClient,
    WatchNotifyClient,
};
use tracing::{info, warn};

/// CROWDB chunkdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crowdb-chunkdb", about = "CROWDB distributed chunk manager")]
struct Cli {
    /// Config file path (TOML).
    #[arg(long)]
    config: String,

    /// HTTP management listen address (overrides config).
    #[arg(long)]
    http_addr: Option<String>,

    /// crowdb-rpc listener address (overrides config `rpc_listen_addr`).
    #[arg(long)]
    rpc_listen_addr: Option<String>,

    /// HTTP management port (overrides the port in config `http_listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    http_port: Option<u16>,

    /// crowdb-rpc listener port (overrides the port in config `rpc_listen_addr`).
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    rpc_port: Option<u16>,

    /// Number of crowdb-rpc I/O worker threads. Default: 2.
    #[arg(long, default_value_t = 2)]
    rpc_workers: u32,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();
    // Layered logging: INFO+ to rotating file, WARN+ to console.
    // RUST_LOG overrides both sinks for debugging.
    let _log_guards = crowdb_common::logging::init_file_and_console_logging_split(
        "log",
        "crowdb-chunkdb",
        crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB,
        crowdb_common::logging::DEFAULT_LOG_MAX_FILES,
        "info",
        "warn",
    )
    .expect("failed to initialize crowdb-chunkdb logging");

    let config = load_config(&args);
    info!(config = ?config, "crowdb-chunkdb starting");

    let http_listen_addr: SocketAddr = config
        .server
        .http_listen_addr
        .parse()
        .expect("valid http_listen_addr");
    let rpc_listen_addr: SocketAddr = config
        .server
        .rpc_listen_addr
        .parse()
        .expect("valid rpc_listen_addr");

    // Build KV client for group-0 topology access.
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(
        config.server.kv_server_mgmt_seeds.clone(),
    )));
    let hw = HardwareClient::from_shared(Arc::clone(&kv));
    let refresh_hw = HardwareClient::from_shared(Arc::clone(&kv));
    let watch = WatchNotifyClient::from_shared(Arc::clone(&kv));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv));
    let svc_keepalive = ServiceRegistryClient::from_shared(Arc::clone(&kv));

    // Topology cache + refresh loop + notify handler.
    let cache = TopologyCache::new();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let refresh_cache = cache.clone();
    let refresh_interval = Duration::from_secs(u64::from(config.topology.refresh_interval_secs));
    let refresh_stop = stop_rx.clone();
    let refresh_handle = tokio::spawn(async move {
        run_refresh_loop(refresh_cache, refresh_hw, refresh_interval, refresh_stop).await;
    });

    let notify_handler = NotifyHandler::new(watch, hw, cache.clone());
    let notify_stop = stop_rx.clone();
    let notify_handle = tokio::spawn(async move {
        notify_handler.run(notify_stop).await;
    });

    // Binding cache + chunk store.
    let bindings = BindingCache::new();
    bindings.replace(default_binding_table(0, 0));
    let store = Arc::new(ChunkStore::new(Arc::clone(&kv), bindings));

    // Range guard (R99): load chunkdb instance binding from group-0.
    // Falls back to allow-all when no binding table exists (v1 compat).
    let range_binding = RangeBindingClient::from_shared(Arc::clone(&kv));
    let range_guard = Arc::new(RangeGuard::new(config.range_guard.allow_all_when_empty));
    if let Err(e) = range_binding.refresh().await {
        warn!(error = %e, "failed to load chunkdb range binding from group-0 (using allow-all fallback)");
    }
    if !range_binding.is_empty() {
        let instance_id = config
            .server
            .instance_id
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(iid) = instance_id {
            if let Err(e) = range_guard.load_from_group0(&kv, iid).await {
                warn!(error = %e, "failed to load owned ranges for instance {iid}");
            }
        }
    }
    // Spawn range binding notifier to keep the guard fresh.
    let _binding_notify_handle = match range_binding.spawn_notifier() {
        Ok(handle) => Some(handle),
        Err(e) => {
            warn!(error = %e, "failed to spawn range binding notifier");
            None
        }
    };

    // Service-registry keep-alive: register this chunkdb instance under
    // `/srv/chunkdb/<instance_id>` and heartbeat periodically. The
    // crowdb-kv-server group-0 leader's `BindingMonitor` reads these
    // entries to compute the chunkdb range binding table.
    let keepalive_handle = spawn_chunkdb_keepalive(
        svc_keepalive,
        config.server.instance_id.as_deref(),
        &config.server.rpc_listen_addr,
        Duration::from_secs(u64::from(config.server.keepalive_interval_secs)),
        stop_rx.clone(),
    );

    // Diskdb client pool + chunk allocator.
    let pool = Arc::new(DiskdbClientPool::new(svc));
    let allocator = Arc::new(ChunkAllocator::new(Arc::clone(&pool)));

    // Per-chunk lock map + payload cache (R100).
    let lifecycle_metrics = Arc::new(LifecycleMetrics::new());
    let hold_warn_threshold = Duration::from_millis(config.lifecycle.lock_hold_warn_threshold_ms);
    let lock_map = Arc::new(ChunkLockMap::new(
        config.lifecycle.cache_capacity,
        Arc::clone(&lifecycle_metrics),
        hold_warn_threshold,
    ));

    // Spawn sweep task for idle lock reaping.
    let sweep_interval = Duration::from_secs(u64::from(config.lifecycle.sweep_chunk_lock_interval_secs));
    let sweep_handle = tokio::spawn(run_sweep_loop(
        Arc::clone(&lock_map),
        sweep_interval,
        stop_rx.clone(),
    ));

    // Lifecycle handler.
    let handler = Arc::new(
        LifecycleHandler::new(Arc::clone(&store), allocator, cache)
            .with_range_guard(Arc::clone(&range_guard))
            .with_locks(Arc::clone(&lock_map)),
    );

    // Build the crowdb-rpc server. The RpcServer listens on the RPC
    // port and dispatches to ChunkdbRpcService handlers.
    let rpc_rt_handle = tokio::runtime::Handle::current();
    let rpc_service = Arc::new(ChunkdbRpcService::new(Arc::clone(&handler), rpc_rt_handle));
    let rpc_server = Arc::new(crowdb_rpc_ffi::RpcServer::with_engines(None, 1, args.rpc_workers));
    rpc_server
        .listen(
            rpc_listen_addr.ip().to_string().as_str(),
            i32::from(rpc_listen_addr.port()),
        )
        .expect("rpc server listen");
    rpc_service.register_handlers(&rpc_server);
    rpc_server.start();
    info!(%rpc_listen_addr, "crowdb-rpc server listening (R116 migration)");

    // Start HTTP health + metrics + cache invalidation server.
    let http_handle = tokio::spawn(run_http_server(http_listen_addr, Arc::clone(&lock_map)));

    let rpc_server_stop = Arc::clone(&rpc_server);
    let _ = tokio::signal::ctrl_c().await;
    info!("received shutdown signal");
    rpc_server_stop.stop();
    let _ = stop_tx.send(true);
    let _ = http_handle.await;
    let _ = refresh_handle.await;
    let _ = notify_handle.await;
    let _ = sweep_handle.await;
    if let Some(h) = keepalive_handle {
        let _ = h.await;
    }
    info!("crowdb-chunkdb stopped");
}

/// Periodic sweep loop — reaps idle chunk locks.
async fn run_sweep_loop(
    locks: Arc<ChunkLockMap>,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = timer.tick() => {
                locks.reap_idle();
            }
            _ = stop.changed() => {
                if *stop.borrow() {
                    info!("sweep task stopping");
                    break;
                }
            }
        }
    }
}

/// Spawn a chunkdb service-registry keep-alive loop. Registers the
/// instance under `/srv/chunkdb/<instance_id>` and heartbeats every
/// `interval`. Stops on the `stop` signal, unregistering on clean
/// shutdown. `instance_id_str` parses to a `u64`; if it is `None` or
/// unparseable, the loop is skipped with a warning (the binding
/// monitor will not see this instance).
fn spawn_chunkdb_keepalive(
    svc: ServiceRegistryClient,
    instance_id_str: Option<&str>,
    listen_addr: &str,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    let instance_id = instance_id_str?.parse::<u64>().ok()?;
    let rpc_endpoint = format!("http://{listen_addr}");
    let handle = tokio::spawn(async move {
        if let Err(e) = svc.register_chunkdb(instance_id, &rpc_endpoint).await {
            warn!(error = %e, "chunkdb keep-alive: initial register failed");
        } else {
            info!(instance_id, "chunkdb keep-alive: registered");
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = svc.heartbeat_chunkdb(instance_id, &rpc_endpoint).await {
                        warn!(error = %e, "chunkdb keep-alive: heartbeat failed");
                    }
                }
                _ = stop.changed() => {
                    if *stop.borrow() {
                        info!(instance_id, "chunkdb keep-alive: shutting down; unregistering");
                        let _ = tokio::time::timeout(
                            Duration::from_secs(1),
                            svc.unregister("chunkdb", instance_id),
                        ).await;
                        break;
                    }
                }
            }
        }
    });
    Some(handle)
}

/// HTTP server — health, metrics, cache invalidation endpoints.
async fn run_http_server(addr: SocketAddr, locks: Arc<ChunkLockMap>) {
    let app = axum::Router::new()
        .route("/ready", axum::routing::get(|| async { "ok" }))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .route(
            "/metrics",
            axum::routing::get({
                let locks = Arc::clone(&locks);
                move || async move {
                    let snap = locks.metrics_snapshot();
                    axum::Json(snap)
                }
            }),
        )
        .route(
            "/invalidate_chunk",
            axum::routing::post({
                let locks = Arc::clone(&locks);
                move |axum::Json(body): axum::Json<InvalidateChunkBody>| async move {
                    if let Some(id) = body.chunk_id {
                        let invalidated = locks.invalidate_chunk(&id);
                        axum::Json(serde_json::json!({ "invalidated": invalidated }))
                    } else {
                        axum::Json(serde_json::json!({ "invalidated": false }))
                    }
                }
            }),
        )
        .route(
            "/invalidate_range",
            axum::routing::post({
                let locks = Arc::clone(&locks);
                move |axum::Json(body): axum::Json<InvalidateRangeBody>| async move {
                    let count = locks.invalidate_range(body.bucket_start, body.bucket_end);
                    axum::Json(serde_json::json!({ "invalidated_count": count }))
                }
            }),
        );
    info!(%addr, "HTTP server listening");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind http addr");
    axum::serve(listener, app).await.expect("HTTP server error");
}

fn load_config(args: &Cli) -> ChunkdbConfig {
    let config_path = &args.config;
    let mut config =
        crowdb_common::config::load_from_file::<ChunkdbConfig>(std::path::Path::new(config_path))
            .unwrap_or_else(|e| panic!("failed to load config file {config_path}: {e}"));

    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.rpc_listen_addr {
        config.server.rpc_listen_addr.clone_from(addr);
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

/// Request body for `POST /invalidate_chunk`.
#[derive(serde::Deserialize)]
struct InvalidateChunkBody {
    chunk_id: Option<crowdb_protocol::common::ChunkId>,
}

/// Request body for `POST /invalidate_range`.
#[derive(serde::Deserialize)]
struct InvalidateRangeBody {
    bucket_start: u16,
    bucket_end: u16,
}
