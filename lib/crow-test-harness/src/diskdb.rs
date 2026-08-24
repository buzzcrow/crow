// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb test harness: subprocess management, binary discovery, and
//! concurrent benchmark for diskdb-client E2E tests.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crow_diskdb_client::{DiskdbClient, RetryConfig};
use crow_protocol::common::ChunkId;

use crate::hardware::{DG_ID, INSTANCE_ID, STORE_ID, UNIT_SIZE_BYTES, ZONE_SIZE_UNITS};

// Re-export hardware helpers for convenience.
pub use crate::cluster::crow_kv_server_bin;
pub use crate::hardware::{seed_hardware, standard_disk_ids_3};

pub fn make_chunk_id(high: u64, low: u64) -> ChunkId {
    ChunkId { high, low }
}

/// Find the crow-diskdb binary.
pub fn crow_diskdb_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROW_DISKDB_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crow-diskdb");
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
/// close the socket. The port may be reused by the OS before the
/// caller binds to it, but this is sufficient for test isolation.
pub fn find_free_port() -> i32 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind 0")
        .local_addr()
        .expect("local_addr")
        .port()
        .into()
}

/// Find a pair of free ports `(grpc_port, rpc_port)` such that
/// `rpc_port = grpc_port - offset`. Tries up to 100 random ports.
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

// ── diskdb subprocess ────────────────────────────────────────────

pub struct DiskdbProcess {
    pub child: std::process::Child,
    pub grpc_port: i32,
    pub rpc_port: i32,
    pub http_port: i32,
    pub config_file: tempfile::NamedTempFile,
    pub log_path: std::path::PathBuf,
}

impl DiskdbProcess {
    pub fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crow-diskdb with a generated config pointing at the
    /// kv-server management seeds. When `validate_owner` is true, the
    /// `[storage]` section sets `validate_owner_on_free = true`.
    pub fn start(kv_seeds: &[String], validate_owner: bool) -> Self {
        let bin = crow_diskdb_bin().unwrap_or_else(|| {
            panic!("crow-diskdb binary not found; set CROW_DISKDB_BIN or build app/crow-diskdb")
        });

        // The client derives rpc_port from grpc_port using a fixed
        // offset (DISKDB_GRPC_BASE - DISKDB_RPC_BASE = 10). Pick a
        // port pair that satisfies this constraint.
        let rpc_port_offset =
            i32::from(crow_protocol::DISKDB_GRPC_BASE) - i32::from(crow_protocol::DISKDB_RPC_BASE);
        let (grpc_port, rpc_port) = find_port_pair_with_offset(rpc_port_offset);
        let http_port = find_free_port();

        let zone_size_bytes = ZONE_SIZE_UNITS * u64::from(UNIT_SIZE_BYTES);
        let storage_section = if validate_owner {
            format!(
                "\n[storage]\nzone_size_bytes = {zone_size_bytes}\nblock_size_bytes = {UNIT_SIZE_BYTES}\nallocate_granularity = {UNIT_SIZE_BYTES}\nzone_rotate_count = 4\ncas_retry_limit = 100\nvalidate_owner_on_free = true\n"
            )
        } else {
            String::new()
        };
        let config_content = format!(
            r#"[server]
listen_addr = "127.0.0.1:{grpc_port}"
rpc_listen_addr = "127.0.0.1:{rpc_port}"
http_listen_addr = "127.0.0.1:{http_port}"
instance_id = "{INSTANCE_ID}"
kv_server_mgmt_seeds = [{seeds}]
{storage_section}
[sync]
group0_store_id = {STORE_ID}
group0_group_id = 0
sync_interval_secs = 2

[heartbeat]
interval_secs = 2
miss_threshold = 3
temp_failure_timeout_secs = 900

[reporting]
interval_secs = 2
"#,
            seeds = kv_seeds
                .iter()
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<_>>()
                .join(", "),
        );

        let config_file = tempfile::NamedTempFile::new().expect("create temp config");
        std::fs::write(config_file.path(), &config_content).expect("write config");

        let log_path = std::env::temp_dir().join(format!("crow-diskdb-e2e-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        cmd.args(["--config", config_file.path().to_str().unwrap()])
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file2));

        let child = cmd.spawn().expect("start crow-diskdb");
        eprintln!("crow-diskdb log: {}", log_path.display());

        Self {
            child,
            grpc_port,
            rpc_port,
            http_port,
            config_file,
            log_path,
        }
    }

    /// Wait for the diskdb HTTP `/ready` endpoint to return 200.
    pub async fn wait_for_ready(&self) {
        let url = format!("http://127.0.0.1:{}/ready", self.http_port);
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    eprintln!("crow-diskdb ready (phase=up)");
                    return;
                }
            }
            if Instant::now() > deadline {
                let log = self.log_content();
                panic!("crow-diskdb did not become ready within 30s. Log:\n{log}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Drop for DiskdbProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── concurrent benchmark ─────────────────────────────────────────

const BENCH_THREADS: usize = 4;
const BENCH_CYCLES: usize = 100;

/// Run a concurrent allocate/free benchmark: `BENCH_THREADS` tasks
/// each doing `BENCH_CYCLES` allocate-1-block + free-1-block cycles.
#[allow(clippy::cast_precision_loss)]
pub async fn run_concurrent_benchmark(client: &Arc<DiskdbClient>) {
    use crow_protocol::diskdb::rpc::{AllocateBlocksRequest, FreeBlocksRequest};

    let start = Instant::now();
    let mut handles = Vec::with_capacity(BENCH_THREADS);

    for tid in 0..BENCH_THREADS {
        let client = Arc::clone(client);
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            let mut errors = 0usize;
            for i in 0..BENCH_CYCLES {
                let owner = make_chunk_id(u64::try_from(tid).unwrap(), u64::try_from(i).unwrap());

                let alloc_req = AllocateBlocksRequest {
                    disk_group_id: DG_ID,
                    unit_count: 1,
                    count: 1,
                    exclude_disk_ids: vec![],
                    owner_chunk: Some(owner),
                };
                let alloc_resp = match client.allocate_blocks(alloc_req).await {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("  bench tid={tid} cycle={i}: allocate error: {e}");
                        errors += 1;
                        continue;
                    }
                };
                if alloc_resp.segments.is_empty() {
                    eprintln!("  bench tid={tid} cycle={i}: allocate returned 0 segments");
                    errors += 1;
                    continue;
                }

                let free_req = FreeBlocksRequest {
                    segments: alloc_resp.segments,
                };
                match client.free_blocks(free_req).await {
                    Ok(r) => {
                        if r.freed_count > 0 {
                            ok += 1;
                        } else {
                            errors += 1;
                        }
                    }
                    Err(e) => {
                        eprintln!("  bench tid={tid} cycle={i}: free error: {e}");
                        errors += 1;
                    }
                }
            }
            (ok, errors)
        }));
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;
    for h in handles {
        let (ok, err) = h.await.expect("benchmark task panicked");
        total_ok += ok;
        total_err += err;
    }

    let elapsed = start.elapsed();
    let total_ops = total_ok + total_err;
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = if secs > 0.0 { total_ops as f64 / secs } else { 0.0 };

    eprintln!(
        "  concurrent benchmark: {BENCH_THREADS} threads × {BENCH_CYCLES} cycles — {total_ops} ops in {elapsed:.2?} ({ops_per_sec:.0} ops/s, {total_ok} ok, {total_err} errors)"
    );
    assert_eq!(total_err, 0, "benchmark should have 0 errors");
    assert_eq!(
        total_ok,
        BENCH_THREADS * BENCH_CYCLES,
        "all benchmark ops should succeed"
    );
}

/// Check if the test can run (both kv-server and diskdb binaries available).
pub fn check_binaries() -> bool {
    let bin = crow_diskdb_bin();
    crate::hardware::check_binaries(bin.as_deref())
}

/// Build a `DiskdbClient` with standard retry config.
pub fn make_client(svc: crow_kv_client::ServiceRegistryClient) -> Arc<DiskdbClient> {
    Arc::new(DiskdbClient::new(svc).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
    }))
}
