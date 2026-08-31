// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-web` binary entrypoint.

use std::net::SocketAddr;

use clap::Parser;
use crowdb_common::logging::init_file_and_console_logging_split;
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

    // Layered logging: INFO+ to rotating file, WARN+ to console.
    // RUST_LOG overrides both sinks for debugging. The file layer uses
    // the same ~/.crowdb-kv/log/ dir as the ops log; the guard must
    // outlive the process so the non-blocking appender flushes on exit.
    let log_dir = dirs::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(".crowdb-kv")
        .join("log");
    let _log_guards = init_file_and_console_logging_split(
        &log_dir,
        "console-web",
        crowdb_common::logging::DEFAULT_LOG_MAX_FILE_MB,
        crowdb_common::logging::DEFAULT_LOG_MAX_FILES,
        "info",
        "warn",
    )
    .map_err(|e| {
        eprintln!("failed to initialize logging: {e}");
        e
    })?;

    // Initialize the crowdb-rpc C++ spdlog logger so transport info/debug
    // messages go to rotating files instead of spdlog's default stderr
    // logger (which floods the console with per-connection noise). Only
    // warn/error reach the console via the stderr sink. No-op without spdlog.
    crowdb_rpc_ffi::init_logging("log", "info", 30, 5, "crowdb-web-rpc");
    crowdb_rpc_ffi::add_log_stderr("warn");

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
