// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` entry point.

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use crow_common::metrics::MetricsRegistry;
use crow_diskdb::config::{validate, DiskdbConfig};
use crow_diskdb::grpc::DiskdbService;
use crow_diskdb::metrics::DiskdbMetrics;
use crow_diskdb::node::NodeContainer;
use crow_diskdb::persistence::DataGroupClient;
use crow_diskdb::sync::{SyncConfig, SyncLoop};
use crow_kv_client::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient};
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

    // Build the in-memory node container.
    let container = Arc::new(NodeContainer::new(instance_id));

    // Build kv-client service classes (seeded with group-0 endpoint).
    // In a real deployment the group-0 endpoint comes from config or
    // discovery; for now we use a placeholder.
    let group0_endpoint = config
        .server
        .instance_id
        .as_ref()
        .map(|_| "http://127.0.0.1:28001".to_string())
        .unwrap_or_default();
    let kv_client = CrowkvClient::new(ClientConfig::new(vec![group0_endpoint.clone()]));
    kv_client.seed_leader(0, 0, group0_endpoint.clone());
    let kv_client2 = CrowkvClient::new(ClientConfig::new(vec![group0_endpoint.clone()]));
    kv_client2.seed_leader(0, 0, group0_endpoint.clone());
    let kv_client3 = CrowkvClient::new(ClientConfig::new(vec![group0_endpoint.clone()]));
    kv_client3.seed_leader(0, 0, group0_endpoint.clone());
    let kv_client4 = CrowkvClient::new(ClientConfig::new(vec![group0_endpoint.clone()]));
    kv_client4.seed_leader(0, 0, group0_endpoint.clone());
    let hw = HardwareClient::new(kv_client);
    let svc = ServiceRegistryClient::new(kv_client2);
    let dg_kv = Arc::new(DataGroupClient::new(kv_client3));
    let dg_kv_sync = DataGroupClient::new(kv_client4);

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

    // Serve gRPC.
    let listen_addr: SocketAddr = config.server.listen_addr.parse().expect("valid listen_addr");
    let grpc_service =
        DiskdbService::new(container.clone(), Arc::clone(&dg_kv), config.storage.clone()).into_server();
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
    info!("crow-diskdb stopped");
}

fn load_config(args: &Cli) -> DiskdbConfig {
    let mut config = if let Some(path) = &args.config {
        let data = std::fs::read_to_string(path).unwrap_or_else(|e| {
            panic!("failed to read config file {path}: {e}");
        });
        serde_json::from_str(&data).unwrap_or_else(|e| {
            panic!("failed to parse config file {path}: {e}");
        })
    } else {
        DiskdbConfig::default()
    };

    if let Some(addr) = &args.listen_addr {
        config.server.listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }

    config
}
