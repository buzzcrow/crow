// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunkdb test harness: subprocess management, binary discovery, and
//! client construction for chunkdb E2E tests.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crow_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport, RetryConfig};

use crate::hardware::INSTANCE_ID;

// Re-export hardware helpers for convenience.
pub use crate::cluster::crow_kv_server_bin;
pub use crate::hardware::{seed_hardware, standard_disk_ids_4};

/// Find the crow-chunkdb binary.
pub fn crow_chunkdb_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROW_CHUNKDB_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crow-chunkdb");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}

/// Bind a TCP socket to `127.0.0.1:0`, read the assigned port, then
/// close the socket.
pub fn find_free_port() -> i32 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind 0")
        .local_addr()
        .expect("local_addr")
        .port()
        .into()
}

/// Find a pair of free ports `(grpc_port, rpc_port)` such that
/// `rpc_port = grpc_port - offset`. The chunkdb client derives the
/// crow-rpc port from the gRPC port using a fixed offset
/// (`CHUNKDB_RPC_BASE - CHUNKDB_GRPC_BASE`), so the harness must pick a
/// pair satisfying that constraint — otherwise the subprocess falls
/// back to the hardcoded default `0.0.0.0:9961` and collides across
/// tests. Tries up to 100 random ports.
fn find_port_pair_with_offset(offset: i32) -> (i32, i32) {
    for _ in 0..100 {
        let grpc_port = find_free_port();
        let rpc_port = grpc_port - offset;
        if rpc_port > 1024 && is_port_free(rpc_port) {
            return (grpc_port, rpc_port);
        }
    }
    panic!("could not find a free port pair with offset {offset}");
}

fn is_port_free(port: i32) -> bool {
    let addr = format!("127.0.0.1:{port}");
    std::net::TcpListener::bind(addr.as_str()).is_ok()
}

// ── chunkdb subprocess ───────────────────────────────────────────

pub struct ChunkdbProcess {
    pub child: std::process::Child,
    pub grpc_port: i32,
    pub http_port: i32,
    pub config_file: tempfile::NamedTempFile,
    pub log_path: std::path::PathBuf,
}

impl ChunkdbProcess {
    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crow-chunkdb with a generated config pointing at the
    /// kv-server management seeds.
    pub fn start(kv_seeds: &[String]) -> Self {
        let bin = crow_chunkdb_bin().unwrap_or_else(|| {
            panic!("crow-chunkdb binary not found; set CROW_CHUNKDB_BIN or build app/crow-chunkdb")
        });

        // The client derives rpc_port from grpc_port using a fixed
        // offset (CHUNKDB_GRPC_BASE - CHUNKDB_RPC_BASE = 10). Pick a
        // port pair that satisfies this constraint so the subprocess
        // binds the crow-rpc listener on the port the client will
        // connect to (instead of the hardcoded default 0.0.0.0:9961,
        // which collides across tests).
        let rpc_port_offset =
            i32::from(crow_protocol::CHUNKDB_GRPC_BASE) - i32::from(crow_protocol::CHUNKDB_RPC_BASE);
        let (grpc_port, rpc_port) = find_port_pair_with_offset(rpc_port_offset);
        let http_port = find_free_port();

        let config_content = format!(
            r#"[server]
listen_addr = "127.0.0.1:{grpc_port}"
rpc_listen_addr = "127.0.0.1:{rpc_port}"
http_listen_addr = "127.0.0.1:{http_port}"
instance_id = "{INSTANCE_ID}"
kv_server_mgmt_seeds = [{seeds}]
keepalive_interval_secs = 2

[topology]
refresh_interval_secs = 2

[range_guard]
allow_all_when_empty = true

[lifecycle]
cache_capacity = 1000
sweep_chunk_lock_interval_secs = 10
lock_hold_warn_threshold_ms = 1000
"#,
            seeds = kv_seeds
                .iter()
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<_>>()
                .join(", "),
        );

        let config_file = tempfile::NamedTempFile::new().expect("create temp config");
        std::fs::write(config_file.path(), &config_content).expect("write config");

        let log_path = std::env::temp_dir().join(format!("crow-chunkdb-e2e-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        cmd.args(["--config", config_file.path().to_str().unwrap()])
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file2));

        let child = cmd.spawn().expect("start crow-chunkdb");
        eprintln!("crow-chunkdb log: {}", log_path.display());

        Self {
            child,
            grpc_port,
            http_port,
            config_file,
            log_path,
        }
    }

    /// Wait for the chunkdb HTTP `/ready` endpoint to return 200.
    pub async fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/ready", self.http_port);
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    eprintln!("crow-chunkdb ready (phase=up)");
                    return;
                }
            }
            if Instant::now() > deadline {
                let log = self.log_content();
                panic!("crow-chunkdb did not become ready within 30s. Log:\n{log}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Drop for ChunkdbProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Check that all required binaries are available for E2E tests.
pub fn check_binaries() -> bool {
    let bin = crow_chunkdb_bin();
    crate::hardware::check_binaries(bin.as_deref())
}

/// Build a `ChunkdbClient` with standard retry config.
pub fn make_client(svc: crow_kv_client::ServiceRegistryClient) -> Arc<ChunkdbClient> {
    let transport = Arc::new(ChunkdbRpcTransport::new());
    Arc::new(ChunkdbClient::with_retry_config(
        svc,
        RetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
        },
        transport,
    ))
}
