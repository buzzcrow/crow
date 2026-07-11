use std::path::PathBuf;
use std::process::ExitCode;

use crowkv_console_shared::ConsoleConfig;

use crate::Cli;

pub fn config_path(cli: &Cli) -> Result<PathBuf, ExitCode> {
    if let Some(p) = &cli.config {
        return Ok(p.clone());
    }
    ConsoleConfig::default_path().ok_or_else(|| {
        eprintln!("error: cannot determine config path (no $HOME, no --config / $CROWKV_CONSOLE_CONFIG)");
        ExitCode::from(1)
    })
}

pub fn load_config(cli: &Cli) -> Result<ConsoleConfig, ExitCode> {
    let path = config_path(cli)?;
    ConsoleConfig::load(&path).map_err(|e| {
        eprintln!("error: load config {}: {e}", path.display());
        ExitCode::from(1)
    })
}

#[allow(dead_code)]
pub fn save_config(cli: &Cli, cfg: &ConsoleConfig) -> Result<(), ExitCode> {
    let path = config_path(cli)?;
    cfg.save(&path).map_err(|e| {
        eprintln!("error: save config {}: {e}", path.display());
        ExitCode::from(2)
    })
}

/// Resolve which servers to poll for `cluster *` commands:
/// 1. `--server <url>` / `CROWKV_SERVER` env (single URL).
/// 2. Persisted registry.
pub fn resolve_targets(cli: &Cli) -> Result<Vec<String>, ExitCode> {
    if let Some(url) = &cli.server {
        return Ok(vec![url.clone()]);
    }
    let cfg = load_config(cli)?;
    if cfg.servers.is_empty() {
        eprintln!("error: no servers registered. Use `crowkv server add --id <id> --url <url>` or pass --server.");
        return Err(ExitCode::from(1));
    }
    Ok(cfg.server_urls())
}
