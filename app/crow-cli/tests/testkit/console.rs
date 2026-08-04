// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Shared CLI e2e harness: spawn a real `crow-kv-server` (local fork),
//! an in-process `crow-web` service bound to a random port, and run
//! the compiled `crow-cli` binary against it with `--ip` / `--port`.
//!
//! Every helper here is consumed by a subset of the `tests/*_cli*`
//! binaries, so the module-level `dead_code` allow keeps each binary
//! from warning about the helpers it does not happen to use.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use crow_console_shared::clients::http::ServerClient;
use crow_console_shared::cluster::NodeHealth;
use crow_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crow_console_shared::lifecycle::{self, crow_kv_server_bin, DeployRequest};
use crow_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crow_console_shared::ConsoleConfig;
use crow_web::{router, AppState};

/// Grab an ephemeral TCP port by binding and immediately dropping.
#[must_use]
pub fn pick_free_port() -> u16 {
    crow_console_shared::test_ports::unique_test_port()
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

/// Locate the compiled `crow-cli` binary next to the test runner.
/// Cargo exposes its path via `CARGO_BIN_EXE_crow-cli`; the fallback
/// walks up to the `debug`/`release` dir for `cargo test` invocations
/// that don't set it.
#[must_use]
pub fn crow_cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crow-cli") {
        return PathBuf::from(path);
    }
    let mut p = std::env::current_exe().expect("current_exe");
    while p.file_name().is_some_and(|n| n != "debug" && n != "release") {
        p.pop();
    }
    p.push("crow-cli");
    p
}

/// A real `crow-kv-server` process forked on loopback.
pub struct Upstream {
    pub pid: u32,
    pub mgmt_url: String,
    pub grpc_url: String,
    workspace: std::path::PathBuf,
}

impl Drop for Upstream {
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
pub fn local_node(id: &str, rack: &str) -> NodeEntry {
    NodeEntry {
        id: id.into(),
        rack_id: rack.into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    }
}

/// Fork a real `crow-kv-server` for node `n1`. Returns `None` (so the
/// caller can skip) when the server binary has not been built.
pub async fn spawn_upstream() -> Option<Upstream> {
    let bin = crow_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let workspace = std::env::temp_dir().join(format!(
        "crow-cli-test-{}-{}-{}",
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
    let (mgmt_port, grpc_port) = pick_two_distinct_free_ports();
    let req = DeployRequest {
        server_id: "n1".into(),
        mgmt_port,
        grpc_port,
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local_in_dir(&req, &local_node("n1", "r1"), &workspace)
        .await
        .expect("deploy_local_in_dir");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
        workspace,
    })
}

/// Spawn an in-process console seeded with rack `r1` + node `n1` + the
/// `upstream` server, priming the monitor cache from the upstream's
/// topology so logical reads resolve immediately.
pub async fn spawn_console(upstream: &Upstream) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: "r1".into(),
        name: "r1".into(),
    });
    cfg.nodes.push(local_node("n1", "r1"));
    cfg.add_server(ServerEntry {
        id: "n1".into(),
        url: upstream.mgmt_url.clone(),
        node_id: Some("n1".into()),
        grpc_url: Some(upstream.grpc_url.clone()),
        mgmt_port: None,
        grpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);

    let client = ServerClient::new(upstream.mgmt_url.clone()).unwrap();
    if let Ok(stores) = client.topology().await {
        let rec = NodeRecord {
            health: NodeHealth::Up,
            last_seen_ms: 1,
            stores: legacy_topology_to_node_stores("n1", &stores),
            last_error: None,
        };
        state.monitor_cache.set_node_report("n1".to_string(), rec).await;
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// Spawn an in-process console with an empty, persisted config rooted
/// under a fresh tempdir, so CLI-driven `rack/node/server` lifecycle
/// (including local `server deploy`) writes its workspace there.
/// Returns the bound address and the tempdir (caller removes it).
pub async fn spawn_console_empty() -> (SocketAddr, PathBuf) {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let dir = tempdir("console");
    let state = AppState::with_config(ConsoleConfig::default(), Some(dir.join("console.toml")));
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, dir)
}

/// Run the CLI with `--ip <ip> --port <port>` plus `args`, capturing
/// `(exit_code, stdout, stderr)`.
pub fn run(cli: &PathBuf, ip: &str, port: u16, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(cli)
        .arg("--ip")
        .arg(ip)
        .arg("--port")
        .arg(port.to_string())
        .args(args)
        .output()
        .expect("spawn cli");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Poll the service until group `(sid, gid)` reports a leader, or the
/// timeout elapses. Returns `true` if a leader was observed.
pub async fn wait_for_leader(ip: &str, port: u16, sid: u64, gid: u64, timeout: Duration) -> bool {
    let http = reqwest::Client::new();
    let url = format!("http://{ip}:{port}/api/stores/{sid}/groups/{gid}");
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(resp) = http.get(&url).send().await {
            if let Ok(v) = resp.json::<serde_json::Value>().await {
                let has_leader = v["replicas"]
                    .as_array()
                    .is_some_and(|rs| rs.iter().any(|r| r["role"] == "leader"));
                if has_leader {
                    return true;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// Unique temp directory for a test.
#[must_use]
pub fn tempdir(tag: &str) -> PathBuf {
    let base = std::env::temp_dir();
    let unique = format!(
        "crow-cli-{tag}-{}-{}",
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
