// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-chunkdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crow_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crow_chunkdb::chunkdb_config::ChunkdbConfig;
use crow_chunkdb::lifecycle::LifecycleHandler;
use crow_chunkdb::range_guard::RangeGuard;
use crow_chunkdb::routing::{default_binding_table, BindingCache};
use crow_chunkdb::service::ChunkdbService;
use crow_chunkdb::storage::ChunkStore;
use crow_chunkdb::topology::{notify::NotifyHandler, refresh::run_refresh_loop, TopologyCache};
use crow_kv_client::{
    ClientConfig, CrowkvClient, HardwareClient, RangeBindingClient, ServiceRegistryClient, WatchNotifyClient,
};
use tracing::{info, warn};

/// CROW chunkdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crow-chunkdb", about = "CROW distributed chunk manager")]
struct Cli {
    /// Config file path (TOML).
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
async fn main() {
    let args = Cli::parse();
    tracing_subscriber::fmt().init();

    let config = load_config(&args);
    info!(config = ?config, "crow-chunkdb starting");

    let listen_addr: SocketAddr = config.server.listen_addr.parse().expect("valid listen_addr");
    let http_listen_addr: SocketAddr = config
        .server
        .http_listen_addr
        .parse()
        .expect("valid http_listen_addr");

    // Build KV client for group-0 topology access.
    let kv = Arc::new(CrowkvClient::new(ClientConfig::new(
        config.server.kv_server_mgmt_seeds.clone(),
    )));
    let hw = HardwareClient::from_shared(Arc::clone(&kv));
    let refresh_hw = HardwareClient::from_shared(Arc::clone(&kv));
    let watch = WatchNotifyClient::from_shared(Arc::clone(&kv));
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv));

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

    // Diskdb client pool + chunk allocator.
    let pool = Arc::new(DiskdbClientPool::new(svc));
    let allocator = Arc::new(ChunkAllocator::new(Arc::clone(&pool)));

    // Lifecycle handler + gRPC service.
    let handler = Arc::new(
        LifecycleHandler::new(Arc::clone(&store), allocator, cache)
            .with_range_guard(Arc::clone(&range_guard)),
    );
    let grpc_service = ChunkdbService::new(handler)
        .with_range_guard(range_guard)
        .into_server();

    // Start HTTP health server.
    let http_handle = tokio::spawn(async move {
        let app = axum::Router::new()
            .route("/ready", axum::routing::get(|| async { "ok" }))
            .route("/health", axum::routing::get(|| async { "ok" }));
        info!(%http_listen_addr, "HTTP health server listening");
        let listener = tokio::net::TcpListener::bind(http_listen_addr)
            .await
            .expect("bind http_listen_addr");
        axum::serve(listener, app)
            .await
            .expect("HTTP health server error");
    });

    info!(%listen_addr, "gRPC server listening");

    let grpc_result = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(listen_addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("received shutdown signal");
            let _ = stop_tx.send(true);
        })
        .await;

    if let Err(e) = grpc_result {
        warn!("gRPC server error: {e}");
    }
    let _ = http_handle.await;
    let _ = refresh_handle.await;
    let _ = notify_handle.await;
    info!("crow-chunkdb stopped");
}

fn load_config(args: &Cli) -> ChunkdbConfig {
    let config_path = &args.config;
    let mut config = crow_common::config::load_from_file::<ChunkdbConfig>(std::path::Path::new(config_path))
        .unwrap_or_else(|e| panic!("failed to load config file {config_path}: {e}"));

    if let Some(addr) = &args.listen_addr {
        config.server.listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }

    config
}
