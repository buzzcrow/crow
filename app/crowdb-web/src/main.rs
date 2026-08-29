// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-web` binary entrypoint.

use std::net::SocketAddr;

use clap::Parser;
use crowdb_protocol::WEB_BASE;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[derive(Parser, Debug)]
    #[command(name = "crowdb-web")]
    struct Args {
        /// Bind address for the web server (default: 0.0.0.0)
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,

        /// Port for the web server (default: 9920)
        #[arg(long, default_value_t = WEB_BASE)]
        port: u16,

        /// Use an in-memory registry instead of the persisted console config.
        #[arg(long)]
        test_mode: bool,
    }

    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Open the per-session operation log file. Outbound HTTP/crowdb-rpc/SSH
    // calls append a JSON-Lines record carrying the correlation id and
    // a curl-reproducible summary. Best-effort: a filesystem failure
    // here drops logging but does not abort startup.
    crowdb_console_shared::ops_log::init_default("web");
    if let Some(log) = crowdb_console_shared::ops_log::current() {
        info!(path = %log.path().display(), "ops log initialised");
    }

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    info!(%addr, "crowdb-web starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Load the persisted registry; absence yields an empty default.
    // Mutating handlers (rack/node/server CRUD) write back to this path.
    let path = if args.test_mode {
        None
    } else {
        crowdb_console_shared::TomlFileEngine::default_path()
    };
    let cfg = match path.as_ref() {
        Some(p) => {
            let engine = crowdb_console_shared::TomlFileEngine::new(p.clone());
            crowdb_console_shared::ConsoleConfig::load_with_engine(&engine).unwrap_or_default()
        }
        None => crowdb_console_shared::ConsoleConfig::default(),
    };
    let server_count = cfg.servers.len();
    let state = crowdb_web::AppState::with_config(cfg, path);
    tracing::info!(servers = server_count, "loaded registry");
    crowdb_web::mgmt::startup_topology_check(&state).await;

    axum::serve(listener, crowdb_web::router(state)).await?;
    Ok(())
}
