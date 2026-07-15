// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process::ExitCode;

use crowkv_console_shared::{ConsoleConfig, TomlFileEngine};

use crate::Cli;

// `load_config` / `config_path` remain: the `bench stress` path reads
// `[bench.stress.*]` overlays from the console config file. There is no
// server registry resolution here anymore — every verb addresses the
// cluster through `--ip` / `--port`.

pub fn config_path(cli: &Cli) -> Result<PathBuf, ExitCode> {
    if let Some(p) = &cli.config {
        return Ok(p.clone());
    }
    TomlFileEngine::default_path().ok_or_else(|| {
        eprintln!("error: cannot determine config path (no $HOME, no --config / $CROWKV_CONSOLE_CONFIG)");
        ExitCode::from(1)
    })
}

pub fn load_config(cli: &Cli) -> Result<ConsoleConfig, ExitCode> {
    let path = config_path(cli)?;
    let engine = TomlFileEngine::new(path.clone());
    ConsoleConfig::load_with_engine(&engine).map_err(|e| {
        eprintln!("error: load config {}: {e}", path.display());
        ExitCode::from(1)
    })
}
