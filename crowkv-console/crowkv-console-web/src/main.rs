//! `crowkv-console-web` binary entrypoint. Listens on `:9920` by default.

use std::net::SocketAddr;

use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let addr: SocketAddr = "127.0.0.1:9920".parse()?;
    info!(%addr, "crowkv-console-web starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Load the persisted registry; absence yields an empty default.
    let cfg = crowkv_console_core::ConsoleConfig::default_path()
        .map(|p| crowkv_console_core::ConsoleConfig::load(&p))
        .transpose()
        .unwrap_or_default()
        .unwrap_or_default();
    let state = crowkv_console_web::AppState::new(cfg.server_urls());
    tracing::info!(servers = state.default_servers.len(), "loaded registry");

    axum::serve(listener, crowkv_console_web::router(state)).await?;
    Ok(())
}
