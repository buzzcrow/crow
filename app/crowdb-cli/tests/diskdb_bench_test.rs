// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskDB` benchmark command-surface tests.

mod common;

use std::process::Command;

use common::direct::crowdb_cli_bin;

#[test]
fn allocate_and_mix_commands_expose_supported_modes() {
    for workload in ["allocate", "mix"] {
        let output = Command::new(crowdb_cli_bin())
            .args(["bench", "diskdb", workload, "--help"])
            .output()
            .expect("run crowdb-cli benchmark help");
        assert!(output.status.success(), "{workload} help must succeed");
        let stdout = String::from_utf8(output.stdout).expect("help is UTF-8");
        assert!(stdout.contains("--mode <MODE>"));
        assert!(stdout.contains("possible values: mem, block"));
        assert!(stdout.contains("--duration-secs"));
        assert!(stdout.contains("--concurrency"));
        assert!(stdout.contains("--diskdb-connections"));
        assert!(stdout.contains("--diskdb-client-rpc-workers"));
    }
}
