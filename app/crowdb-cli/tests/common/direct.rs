// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared CLI e2e harness (R126 direct-to-group-0): spawn a real
//! `crowdb-kv-server`, initialize group 0 on it, and run the compiled
//! `crowdb-cli` binary against it with `--sysmd-ip` / `--sysmd-port`.
//!
//! Unlike the old `console.rs` harness, this does NOT spawn a
//! `crowdb-web` intermediary — the CLI talks directly to group-0
//! sysdata and the kv-server mgmt API.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::config::{NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::lifecycle::{self, crowdb_kv_server_bin, DeployRequest};
use crowdb_console_shared::{ConsoleConfig, ConsoleConfigEngine};
use crowdb_test_harness::test_dirs;

/// Grab an ephemeral TCP port by binding and immediately dropping.
#[must_use]
pub fn pick_free_port() -> u16 {
    crowdb_console_shared::test_ports::unique_test_port()
}

/// Grab two distinct ephemeral TCP ports.
#[must_use]
pub fn pick_two_distinct_free_ports() -> (u16, u16) {
    let first = pick_free_port();
    let mut second = pick_free_port();
    while second == first {
        second = pick_free_port();
    }
    (first, second)
}

/// Locate the compiled `crowdb-cli` binary next to the test runner.
#[must_use]
pub fn crowdb_cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crowdb-cli") {
        return PathBuf::from(path);
    }
    let mut p = std::env::current_exe().expect("current_exe");
    while p.file_name().is_some_and(|n| n != "debug" && n != "release") {
        p.pop();
    }
    p.push("crowdb-cli");
    p
}

/// A real `crowdb-kv-server` process forked on loopback, with group 0
/// initialized so the CLI can talk to it directly.
pub struct Group0 {
    pub pid: u32,
    pub mgmt_url: String,
    pub rpc_url: String,
    pub mgmt_port: u16,
    pub rpc_port: u16,
    pub config_path: PathBuf,
    workspace: std::path::PathBuf,
}

impl Drop for Group0 {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(self.pid.to_string())
            .status();
        let _ = std::fs::remove_dir_all(&self.workspace);
    }
}

/// A local-fork (no-SSH) node entry on `127.0.0.1`.
#[must_use]
pub fn local_node(id: u64, rack: u64) -> NodeEntry {
    NodeEntry {
        id,
        rack_id: rack,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    }
}

/// Fork a real `crowdb-kv-server` for node 1, initialize group 0 on it,
/// and write a console config with the rack/node/server entries. Returns
/// `None` when the server binary has not been built.
pub async fn spawn_group0() -> Option<Group0> {
    let bin = crowdb_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let workspace = test_dirs::test_data_dir().join(format!(
        "crowdb-cli-direct-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        pick_free_port()
    ));
    std::fs::create_dir_all(&workspace).ok()?;
    std::fs::create_dir_all(workspace.join("bin")).ok()?;
    std::fs::create_dir_all(workspace.join("log")).ok()?;

    let (rest_port, rpc_port) = pick_two_distinct_free_ports();
    let req = DeployRequest {
        server_id: "1".into(),
        rest_port,
        rpc_port,
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local_in_dir(&req, &local_node(1, 1), &workspace)
        .await
        .expect("deploy_local_in_dir");

    // Write console config with rack/node/server entries.
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: 1,
        name: "rack-1".into(),
    });
    cfg.nodes.push(local_node(1, 1));
    cfg.add_server(ServerEntry {
        id: "1".to_string(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(1),
        rpc_url: Some(deployed.rpc_url.clone()),
        rest_port: Some(rest_port),
        rpc_port: Some(rpc_port),
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: Some(deployed.pid),
        service_type: ServiceType::Kv,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();

    let config_path = workspace.join("console.toml");
    let engine = crowdb_console_shared::TomlFileEngine::new(config_path.clone());
    engine.save(&cfg).expect("save config");

    // Initialize group 0 on the server (single-node, self-elect).
    let client = ServerClient::new(deployed.mgmt_url.clone()).unwrap();
    client
        .system_init(&crowdb_protocol::mgmt::SystemInitRequest {
            replica_id: 1,
            start_election: true,
        })
        .await
        .expect("system_init");

    // Wait for the leader to be elected.
    wait_for_group0_leader(&client, Duration::from_secs(5)).await;

    Some(Group0 {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
        mgmt_port: rest_port,
        rpc_port,
        config_path,
        workspace,
    })
}

/// Poll the server's topology until group (0, 0) reports a leader.
async fn wait_for_group0_leader(client: &ServerClient, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(stores) = client.topology().await {
            for s in &stores {
                if s.store_id == 0 {
                    for g in &s.groups {
                        if g.group_id == 0 && g.local_replica_id > 0 {
                            return;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("group 0 leader not elected within {timeout:?}");
}

/// Run the CLI with `--sysmd-ip 127.0.0.1 --sysmd-port <mgmt_port>
/// --config <config_path>` plus `args`, capturing `(exit_code, stdout,
/// stderr)`.
pub fn run(cli: &PathBuf, mgmt_port: u16, config_path: &PathBuf, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(cli)
        .arg("--sysmd-ip")
        .arg("127.0.0.1")
        .arg("--sysmd-port")
        .arg(mgmt_port.to_string())
        .arg("--config")
        .arg(config_path)
        .args(args)
        .output()
        .expect("spawn cli");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Unique temp directory for a test.
#[must_use]
pub fn tempdir(tag: &str) -> PathBuf {
    let base = test_dirs::test_data_dir();
    let unique = format!(
        "crowdb-cli-direct-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = base.join(unique);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
