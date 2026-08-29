// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::process::ExitCode;

use crowdb_console_shared::clients::console::ConsoleClient;

use crate::Cli;

/// Build a [`ConsoleClient`] pointed at `http://{ip}:{port}`. Every CLI
/// verb routes through this; there is no direct `crowdb-kv-server` client.
pub fn console_client(cli: &Cli) -> Result<ConsoleClient, ExitCode> {
    let url = format!("http://{}:{}", cli.ip, cli.port);
    ConsoleClient::new(url).map_err(|e| {
        eprintln!("error: build console client: {e}");
        ExitCode::from(2)
    })
}
