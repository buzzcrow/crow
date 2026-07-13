// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::process::ExitCode;

use crowkv_console_shared::clients::console::ConsoleClient;

use crate::Cli;

/// Build a [`ConsoleClient`] pointed at the `--console` URL. Every CLI
/// verb routes through this; there is no direct `crowkv-server` client.
pub fn console_client(cli: &Cli) -> Result<ConsoleClient, ExitCode> {
    ConsoleClient::new(cli.console.clone()).map_err(|e| {
        eprintln!("error: build console client: {e}");
        ExitCode::from(2)
    })
}
