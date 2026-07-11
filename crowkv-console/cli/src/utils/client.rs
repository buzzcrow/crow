use std::process::ExitCode;

use crowkv_console_shared::clients::console::ConsoleClient;
use crowkv_console_shared::clients::http::ServerClient;

use super::config::load_config;
use crate::Cli;

/// Build a [`ConsoleClient`] pointed at the `--console` URL. Every
/// new (post-A12) CLI verb uses this instead of `mgmt_client`.
pub fn console_client(cli: &Cli) -> Result<ConsoleClient, ExitCode> {
    ConsoleClient::new(cli.console.clone()).map_err(|e| {
        eprintln!("error: build console client: {e}");
        ExitCode::from(2)
    })
}

/// Resolve a single management target URL for `store / paxos / replica`
/// verbs. Priority: `--server` override (URL or registry id), then the
/// sole entry in the registry. Errors if the registry is empty or has
/// multiple entries and `--server` wasn't supplied.
pub fn resolve_single_target(cli: &Cli) -> Result<String, ExitCode> {
    if let Some(raw) = &cli.server {
        // Allow either a bare URL or a registry id.
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(raw.clone());
        }
        let cfg = load_config(cli)?;
        return cfg.server(raw).map(|s| s.url.clone()).ok_or_else(|| {
            eprintln!("error: --server {raw}: not a URL and no registry entry with that id");
            ExitCode::from(1)
        });
    }
    let cfg = load_config(cli)?;
    match cfg.servers.len() {
        0 => {
            eprintln!("error: no servers registered. Use `crowkv server add ...` or pass --server <url|id>.");
            Err(ExitCode::from(1))
        }
        1 => Ok(cfg.servers[0].url.clone()),
        _ => {
            eprintln!("error: multiple servers registered; pass --server <url|id> to pick one");
            Err(ExitCode::from(1))
        }
    }
}

#[allow(dead_code)]
pub fn mgmt_client(cli: &Cli) -> Result<ServerClient, ExitCode> {
    let url = resolve_single_target(cli)?;
    ServerClient::new(url).map_err(|e| {
        eprintln!("error: build client: {e}");
        ExitCode::from(2)
    })
}
