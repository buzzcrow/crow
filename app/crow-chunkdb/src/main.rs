// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-chunkdb` entry point.

use std::net::SocketAddr;

use clap::Parser;
use crow_chunkdb::chunkdb_config::ChunkdbConfig;
use crow_chunkdb::service::ChunkdbService;
use tracing::info;

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

    // Serve gRPC until shutdown.
    let grpc_service = ChunkdbService::new().into_server();
    info!(%listen_addr, "gRPC server listening (stub — RPCs return Unimplemented)");

    let grpc_result = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(listen_addr, async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("received shutdown signal");
        })
        .await;

    if let Err(e) = grpc_result {
        tracing::error!("gRPC server error: {e}");
    }
    let _ = http_handle.await;
    info!("crow-chunkdb stopped");
}

fn load_config(args: &Cli) -> ChunkdbConfig {
    let config_path = &args.config;
    let mut config = crow_common::config::load_from_file::<ChunkdbConfig>(std::path::Path::new(config_path))
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
