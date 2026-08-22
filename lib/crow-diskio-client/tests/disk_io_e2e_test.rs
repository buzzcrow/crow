// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! End-to-end disk IO test suite: kv-server → seed hardware → diskio →
//! write/read/fsync via `DiskioClient`.
//!
//! Starts a real crow-kv-server, seeds hardware metadata into
//! group-0, starts crow-diskio (C++ binary) with --auto-discover-disks
//! so it fetches its disk list from group-0, then writes and reads
//! data through the diskio RPC server directly — no diskdb needed.
//!
//! Four test functions:
//!
//! - **`disk_io_e2e_full_flow`** — smoke test for both backends
//!   (`MemDisk` data integrity, `NullDisk` pattern verification) across
//!   multiple IO sizes (including 2 MB max and 0-byte boundary), plus
//!   read-before-write, overwrite, non-zero `zone_offset`, non-zero
//!   `DiskId.high`, and a concurrent benchmark with content verification.
//!
//! - **`disk_io_e2e_error_paths`** — verifies client error decoding for
//!   `DiskNotExist` (write/read/fsync), `ZoneNotExist`, and `IoError`
//!   (via fault injection with `--fault-error-rate 1.0`).
//!
//! - **`disk_io_e2e_durability`** — write + fsync + process restart +
//!   read with `BlockDisk` on a temp file (I2 durability invariant).
//!
//! - **`disk_io_e2e_group0_sync`** — verifies periodic group-0 heartbeat
//!   (service registry entry appears) and disk-list reconciliation
//!   (new disk added to group-0 after startup becomes writable).

mod common;

use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Monotonic counter for unique log file names when multiple diskio
/// processes start in parallel (tests run concurrently by default).
static DISKIO_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

use common::cluster::KvCluster;
use crow_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, DiskioClient, DiskioError};
use crow_kv_client::HardwareClient;
use crow_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crow_rpc_ffi::RpcServer;

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: u64 = 100;
const INSTANCE_ID: u64 = 999;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;

// Small disks: 4 zones × 128 units × 1 MB = 512 MB per disk.
const ZONE_SIZE_UNITS: u64 = 128;
const UNIT_SIZE_BYTES: u32 = 1024 * 1024; // 1 MB
const ZONE_COUNT: u32 = 4;
const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * ZONE_COUNT as u64;

fn make_disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

// ── NullDisk pattern generator (mirrors C++ DummyDiskEngine::fill_pattern) ──

fn xorshift64(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

fn hash_seed(disk_id: DioDiskId) -> u64 {
    disk_id.high ^ disk_id.low.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Compute the deterministic pattern `NullDisk` returns for a read at
/// (`disk_id`, `phys_offset`, size). Mirrors the C++ `fill_pattern`.
fn null_disk_pattern(disk_id: DioDiskId, phys_offset: u64, size: usize) -> Vec<u8> {
    let mut state = hash_seed(disk_id);
    let skip = phys_offset / 8;
    for _ in 0..skip {
        state = xorshift64(state);
    }
    let mut buf = vec![0u8; size];
    let mut pos = 0;
    while pos < size {
        state = xorshift64(state);
        let val_bytes = state.to_le_bytes();
        let n = std::cmp::min(8, size - pos);
        buf[pos..pos + n].copy_from_slice(&val_bytes[..n]);
        pos += n;
    }
    buf
}

// ── binary discovery ─────────────────────────────────────────────

fn crow_diskio_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROW_DISKIO_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    let candidates = [
        "../../app/crow-diskio/build/crow-diskio",
        "../../../app/crow-diskio/build/crow-diskio",
        "../../../../app/crow-diskio/build/crow-diskio",
    ];
    for c in &candidates {
        let p = std::path::PathBuf::from(c);
        if p.exists() {
            return p.canonicalize().ok();
        }
    }
    None
}

fn crow_lib_dir() -> Option<std::path::PathBuf> {
    let candidates = [
        "../../target/debug",
        "../../target/release",
        "../../../target/debug",
        "../../../target/release",
        "../../../../target/debug",
        "../../../../target/release",
    ];
    for c in &candidates {
        let p = std::path::PathBuf::from(c);
        if p.join("libcrow_kv_client.so").exists() {
            return p.canonicalize().ok();
        }
    }
    None
}

fn crow_kv_server_bin() -> Option<std::path::PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crow-kv-server");
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

// ── hardware seeding ─────────────────────────────────────────────

async fn seed_hardware(hw: &HardwareClient) {
    hw.add_rack(
        RACK_ID,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![NODE_ID],
        },
    )
    .await
    .expect("add rack");

    hw.add_node(
        RACK_ID,
        NODE_ID,
        &NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: vec![DG_ID],
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        },
    )
    .await
    .expect("add node");

    let disk_ids = vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0xAB, 4),
    ];
    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: disk_ids.clone(),
        },
    )
    .await
    .expect("add disk-group");

    for did in &disk_ids {
        hw.add_disk(
            RACK_ID,
            NODE_ID,
            DG_ID,
            did,
            &DiskValue {
                disk_type: DiskType::BlockSsd as i32,
                capacity_units: CAPACITY_UNITS,
                zone_size_units: ZONE_SIZE_UNITS,
                unit_size_bytes: UNIT_SIZE_BYTES,
                zone_count: ZONE_COUNT,
                status: HwStatus::Up as i32,
                device_path: String::new(),
            },
        )
        .await
        .expect("add disk");
    }

    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    hw.set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID, lease_ms)
        .await
        .expect("set owner");

    hw.set_bind(RACK_ID, NODE_ID, DG_ID, STORE_ID, DATA_GROUP_ID)
        .await
        .expect("set bind");
}

// ── diskio subprocess ────────────────────────────────────────────

/// Options for starting a diskio subprocess.
struct DiskioStartOpts<'a> {
    /// Dummy disk type ("null" or "mem"). Ignored if `disks` is non-empty.
    dummy_disk: &'a str,
    /// KV-server management seeds for group-0 sync. Empty = no sync.
    kv_seeds: &'a [String],
    /// Explicit disk list (`--disk` args). Empty = use auto-discover.
    disks: &'a [DiskArg],
    /// Fault error rate (0.0 = none). Injects `--fault-error-rate`.
    fault_error_rate: f64,
    /// Disable `O_DIRECT` for `BlockDisk`.
    no_o_direct: bool,
}

/// A `--disk` argument: hex id + path + zone capacity bytes.
#[derive(Clone)]
struct DiskArg {
    id_high: u64,
    id_low: u64,
    path: String,
    zone_capacity: i64,
}

struct DiskioProcess {
    child: std::process::Child,
    port: i32,
    log_path: std::path::PathBuf,
}

impl DiskioProcess {
    fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crow-diskio with the given options.
    fn start(opts: &DiskioStartOpts<'_>) -> Self {
        let bin = crow_diskio_bin().unwrap_or_else(|| {
            panic!("crow-diskio binary not found; set CROW_DISKIO_BIN or build app/crow-diskio")
        });
        let lib_dir = crow_lib_dir().unwrap_or_else(|| {
            panic!("libcrow_kv_client.so not found; build with cargo build -p crow-kv-client --features ffi")
        });

        let inst = DISKIO_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let log_path = std::env::temp_dir().join(format!(
            "crow-diskio-e2e-{}-{}-{}.log",
            opts.dummy_disk,
            std::process::id(),
            inst,
        ));
        let log_file = std::fs::File::create(&log_path).expect("create log file");
        let log_file2 = log_file.try_clone().expect("clone log file");

        let mut cmd = Command::new(&bin);
        cmd.args([
            "--port",
            "0",
            "--bind",
            "127.0.0.1",
            "--dummy-disk",
            opts.dummy_disk,
        ])
        .env("LD_LIBRARY_PATH", lib_dir.to_str().unwrap())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file2));

        if opts.no_o_direct {
            cmd.arg("--no-o-direct");
        }

        if opts.fault_error_rate > 0.0 {
            cmd.args(["--fault-error-rate", &opts.fault_error_rate.to_string()]);
        }

        if !opts.disks.is_empty() {
            for d in opts.disks {
                // --disk format: <hex_id>:<path>[:<zone_capacity>]
                // The parser splits on the first colon, so hex_id must
                // be a single hex value (low only, high=0).
                assert_eq!(
                    d.id_high, 0,
                    "--disk arg only supports id_high=0 (single hex value)"
                );
                let id_str = format!("{:x}", d.id_low);
                let disk_arg = format!("{}:{}:{}", id_str, d.path, d.zone_capacity);
                cmd.args(["--disk", &disk_arg]);
            }
        } else if !opts.kv_seeds.is_empty() {
            let seeds_arg = opts.kv_seeds.join(",");
            cmd.args([
                "--kv-seeds",
                &seeds_arg,
                "--instance-id",
                &INSTANCE_ID.to_string(),
                "--rack-id",
                &RACK_ID.to_string(),
                "--node-id",
                &NODE_ID.to_string(),
                "--dg-id",
                &DG_ID.to_string(),
                "--sync-interval-ms",
                "2000",
                "--auto-discover-disks",
            ]);
        }

        let mut child = cmd.spawn().expect("start crow-diskio");
        eprintln!("crow-diskio ({}) log: {}", opts.dummy_disk, log_path.display());

        let port = {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut port = None;
            while std::time::Instant::now() < deadline && port.is_none() {
                std::thread::sleep(Duration::from_millis(50));
                if let Ok(content) = std::fs::read_to_string(&log_path) {
                    for line in content.lines() {
                        if let Some(idx) = line.find("listening on ") {
                            let after = &line[idx + "listening on ".len()..];
                            if let Some(colon) = after.find(':') {
                                let rest = &after[colon + 1..];
                                let port_str: String =
                                    rest.chars().take_while(char::is_ascii_digit).collect();
                                if let Ok(p) = port_str.parse::<i32>() {
                                    port = Some(p);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            port.unwrap_or_else(|| {
                let _ = child.kill();
                let log = std::fs::read_to_string(&log_path).unwrap_or_default();
                panic!("crow-diskio did not start. Log:\n{log}");
            })
        };

        eprintln!("crow-diskio ({}) started on port {port}", opts.dummy_disk);
        Self {
            child,
            port,
            log_path,
        }
    }

    /// Wait for diskio to discover disks by retrying a write.
    async fn wait_for_disks(
        &self,
        dio_client: &DiskioClient,
        server: &RpcServer,
        conn: &crow_rpc_ffi::Connection,
    ) {
        let test_disk = DioDiskId::new(0, 1);
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let write_result = dio_client.write(server, conn, test_disk, 0, 0, vec![0xAB; 4096]);
            match write_result {
                Ok(fut) => match DiskioClient::await_write_response(fut).await {
                    Ok(_) => {
                        eprintln!("diskio disks ready");
                        return;
                    }
                    Err(DiskioError::IoError(code)) => {
                        if code == DiskIoRetCode::DiskNotExist {
                            // Disks not yet discovered — keep waiting.
                        } else {
                            eprintln!("diskio disks ready (io error: {code:?})");
                            return;
                        }
                    }
                    Err(e) => eprintln!("diskio write attempt error: {e:?}"),
                },
                Err(e) => eprintln!("diskio write send error: {e:?}"),
            }
            if std::time::Instant::now() > deadline {
                let log = self.log_content();
                panic!("diskio did not discover disks within 15s. Log:\n{log}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

impl Drop for DiskioProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── IO test helper ───────────────────────────────────────────────

/// Which backend to test.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Backend {
    Mem,
    Null,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl Backend {
    fn cli_arg(self) -> &'static str {
        match self {
            Self::Mem => "mem",
            Self::Null => "null",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Mem => "MemDisk",
            Self::Null => "NullDisk",
        }
    }
}

/// Run a write+read+verify + overwrite cycle at the given size.
///
/// For `MemDisk`: read after write returns the same bytes that were
/// written; read after overwrite returns the second write's bytes.
///
/// For `NullDisk`: read always returns the deterministic pattern
/// regardless of what was written.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn test_io_round(
    dio_client: &DiskioClient,
    server: &RpcServer,
    conn: &crow_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
    zone_index: u32,
    zone_offset: u64,
    size: usize,
    label: &str,
) {
    // phys_offset = zone_base_offset + zone_offset.
    // zone_base_offset = zone_index * (zone_size_units * unit_size_bytes).
    let phys_offset = u64::from(zone_index) * (ZONE_SIZE_UNITS * u64::from(UNIT_SIZE_BYTES)) + zone_offset;
    let read_size = u32::try_from(size).unwrap();

    // ── Write + read + verify ──
    let write_data: Vec<u8> = (0..size)
        .map(|i| u8::try_from((i * 7 + 13) % 256).unwrap())
        .collect();
    let write_fut = dio_client
        .write(server, conn, disk_id, zone_index, zone_offset, write_data.clone())
        .expect("write send");
    let write_code = DiskioClient::await_write_response(write_fut)
        .await
        .expect("write IO");
    assert_eq!(
        write_code,
        DiskIoRetCode::Success,
        "{label}: {backend} write should succeed"
    );

    let read_fut = dio_client
        .read(server, conn, disk_id, zone_index, zone_offset, read_size)
        .expect("read send");
    let (read_code, read_data) = DiskioClient::await_read_response(read_fut)
        .await
        .expect("read IO");
    assert_eq!(
        read_code,
        DiskIoRetCode::Success,
        "{label}: {backend} read should succeed"
    );
    // For 0-byte reads the data buffer may be absent (None) or empty.
    let read_data = read_data.unwrap_or_default();
    assert_eq!(read_data.len(), size, "{label}: {backend} read length mismatch");

    if size > 0 {
        match backend {
            Backend::Mem => {
                assert_eq!(
                    read_data, write_data,
                    "{label}: {backend} read data must match written data"
                );
            }
            Backend::Null => {
                let expected = null_disk_pattern(disk_id, phys_offset, size);
                assert_eq!(
                    read_data, expected,
                    "{label}: {backend} read data must match computed pattern"
                );
            }
        }
    }

    // ── Overwrite: write different data, read back, verify ──
    if size > 0 {
        let overwrite_data: Vec<u8> = (0..size)
            .map(|i| u8::try_from((i * 3 + 99) % 256).unwrap())
            .collect();
        let ow_fut = dio_client
            .write(
                server,
                conn,
                disk_id,
                zone_index,
                zone_offset,
                overwrite_data.clone(),
            )
            .expect("overwrite send");
        let ow_code = DiskioClient::await_write_response(ow_fut)
            .await
            .expect("overwrite IO");
        assert_eq!(
            ow_code,
            DiskIoRetCode::Success,
            "{label}: {backend} overwrite should succeed"
        );

        let ow_read_fut = dio_client
            .read(server, conn, disk_id, zone_index, zone_offset, read_size)
            .expect("overwrite read send");
        let (ow_read_code, ow_read_data) = DiskioClient::await_read_response(ow_read_fut)
            .await
            .expect("overwrite read IO");
        assert_eq!(
            ow_read_code,
            DiskIoRetCode::Success,
            "{label}: {backend} overwrite read should succeed"
        );
        let ow_read_data = ow_read_data.unwrap_or_default();
        assert_eq!(
            ow_read_data.len(),
            size,
            "{label}: {backend} overwrite read length mismatch"
        );
        match backend {
            Backend::Mem => {
                assert_eq!(
                    ow_read_data, overwrite_data,
                    "{label}: {backend} overwrite read must match second write"
                );
            }
            Backend::Null => {
                let expected = null_disk_pattern(disk_id, phys_offset, size);
                assert_eq!(
                    ow_read_data, expected,
                    "{label}: {backend} overwrite read should still match pattern"
                );
            }
        }
    }

    // ── Fsync ──
    let fsync_fut = dio_client.fsync(server, conn, disk_id).expect("fsync send");
    let fsync_code = DiskioClient::await_fsync_response(fsync_fut)
        .await
        .expect("fsync IO");
    assert_eq!(
        fsync_code,
        DiskIoRetCode::Success,
        "{label}: {backend} fsync should succeed"
    );

    eprintln!(
        "  {label}: {backend} write+read+overwrite+fsync OK ({size} bytes, zone {zone_index} offset {zone_offset})"
    );
}

/// Test read-before-write: reading from an uninitialized area of the
/// disk. Uses a fresh disk (not written to by `wait_for_disks`) and a
/// zone-0 offset within the memfd's initial ftruncate size.
///
/// For `MemDisk`: the memfd is zero-filled, so reads return zeros.
/// For `NullDisk`: reads return the deterministic pattern.
async fn test_read_before_write(
    dio_client: &DiskioClient,
    server: &RpcServer,
    conn: &crow_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
) {
    // Use disk {0,2} (not written to by wait_for_disks which uses {0,1}).
    // Read 4 KB at zone 0, offset 1 MB — within the memfd's 128 MB
    // initial size, and not yet written to.
    let zone_index = 0u32;
    let zone_offset = 1024 * 1024u64; // 1 MB
    let size = 4096usize;
    let phys_offset = u64::from(zone_index) * (ZONE_SIZE_UNITS * u64::from(UNIT_SIZE_BYTES)) + zone_offset;
    let read_size = u32::try_from(size).unwrap();

    let rf = dio_client
        .read(server, conn, disk_id, zone_index, zone_offset, read_size)
        .expect("read-before-write send");
    let (code, data) = DiskioClient::await_read_response(rf)
        .await
        .expect("read-before-write IO");
    assert_eq!(
        code,
        DiskIoRetCode::Success,
        "{backend}: read-before-write should succeed"
    );
    let data = data.expect("read-before-write data should be present");
    assert_eq!(data.len(), size, "{backend}: read-before-write length mismatch");
    match backend {
        Backend::Mem => {
            assert!(
                data.iter().all(|&b| b == 0),
                "{backend}: read-before-write should return zeros (uninitialized memfd)"
            );
        }
        Backend::Null => {
            let expected = null_disk_pattern(disk_id, phys_offset, size);
            assert_eq!(
                data, expected,
                "{backend}: read-before-write should match pattern"
            );
        }
    }
    eprintln!("  {backend} read-before-write OK (4 KB at zone 0 offset 1 MB, uninitialized)");
}

// ── concurrent benchmark ─────────────────────────────────────────

/// Number of concurrent tasks for the small benchmark.
const BENCH_THREADS: usize = 4;
/// Write+read cycles per task.
const BENCH_CYCLES: usize = 100;
/// IO size for the benchmark.
const BENCH_SIZE: usize = 4096;

/// Run a concurrent write/read benchmark: `BENCH_THREADS` tasks each
/// doing `BENCH_CYCLES` write+read cycles on different offsets of the
/// same disk. Reports throughput and verifies read data content under
/// concurrent load.
#[allow(clippy::cast_precision_loss)]
async fn run_concurrent_benchmark(
    dio_client: &Arc<DiskioClient>,
    server: &Arc<RpcServer>,
    conn: &crow_rpc_ffi::Connection,
    backend: Backend,
    disk_id: DioDiskId,
) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(BENCH_THREADS);

    for tid in 0..BENCH_THREADS {
        let client = Arc::clone(dio_client);
        let server = Arc::clone(server);
        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            let mut errors = 0usize;
            for i in 0..BENCH_CYCLES {
                // Each thread writes to its own offset range to avoid
                // overwriting each other: tid * BENCH_CYCLES + i.
                let offset = u64::try_from((tid * BENCH_CYCLES + i) * BENCH_SIZE).unwrap();
                let data = vec![u8::try_from((tid + i) % 256).unwrap(); BENCH_SIZE];

                let Ok(wf) = client.write(&server, &conn, disk_id, 0, offset, data.clone()) else {
                    errors += 1;
                    continue;
                };
                let Ok(wc) = DiskioClient::await_write_response(wf).await else {
                    errors += 1;
                    continue;
                };
                if wc != DiskIoRetCode::Success {
                    errors += 1;
                    continue;
                }

                let Ok(rf) = client.read(
                    &server,
                    &conn,
                    disk_id,
                    0,
                    offset,
                    u32::try_from(BENCH_SIZE).unwrap(),
                ) else {
                    errors += 1;
                    continue;
                };
                let Ok((rc, rd)) = DiskioClient::await_read_response(rf).await else {
                    errors += 1;
                    continue;
                };
                if rc != DiskIoRetCode::Success {
                    errors += 1;
                    continue;
                }
                let Some(rd) = rd else {
                    errors += 1;
                    continue;
                };
                if rd.len() != BENCH_SIZE {
                    errors += 1;
                    continue;
                }
                // Verify read content under concurrent load.
                let phys_offset = offset;
                let content_ok = match backend {
                    Backend::Mem => rd == data,
                    Backend::Null => rd == null_disk_pattern(disk_id, phys_offset, BENCH_SIZE),
                };
                if !content_ok {
                    errors += 1;
                    continue;
                }
                ok += 1;
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
    let data_bytes = u64::try_from(total_ok * BENCH_SIZE * 2).unwrap_or(u64::MAX);
    let data_mb = data_bytes as f64 / (1024.0 * 1024.0);
    let mb_per_sec = if secs > 0.0 { data_mb / secs } else { 0.0 };

    eprintln!(
        "  {backend} benchmark: {BENCH_THREADS} threads × {BENCH_CYCLES} cycles, {BENCH_SIZE}B — {total_ops} ops in {elapsed:.2?} ({ops_per_sec:.0} ops/s, {mb_per_sec:.1} MB/s, {total_ok} ok, {total_err} errors)"
    );
    assert_eq!(total_err, 0, "{backend}: benchmark should have 0 errors");
    assert_eq!(
        total_ok,
        BENCH_THREADS * BENCH_CYCLES,
        "{backend}: all ops should succeed"
    );
}

// IO sizes to test: zero, small, middle, large, max (2 MB).
const IO_SIZES: &[(usize, &str)] = &[
    (0, "zero"),
    (100, "small"),
    (4096, "middle"),
    (1024 * 1024, "large"),   // 1 MB
    (2 * 1024 * 1024, "max"), // 2 MB (default max block size)
];

#[tokio::test]
#[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
async fn disk_io_e2e_full_flow() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    if crow_diskio_bin().is_none() {
        eprintln!("skipping: crow-diskio binary not found");
        return;
    }

    // 1. Start the kv cluster.
    eprintln!("=== starting kv cluster ===");
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0={}, group1={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware metadata into group 0.
    eprintln!("=== seeding hardware metadata ===");
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware metadata seeded (rack={RACK_ID}, node={NODE_ID}, dg={DG_ID}, 4 disks)");

    // 3. Test each backend.
    for backend in [Backend::Null, Backend::Mem] {
        eprintln!();
        eprintln!("=== testing {} backend ===", backend.name());

        // Start crow-diskio with this backend.
        let diskio = DiskioProcess::start(&DiskioStartOpts {
            dummy_disk: backend.cli_arg(),
            kv_seeds: &cluster.mgmt_endpoints,
            disks: &[],
            fault_error_rate: 0.0,
            no_o_direct: false,
        });

        // Connect diskio-client.
        let rpc_server = Arc::new(RpcServer::new(None));
        rpc_server.listen("127.0.0.1", 0).expect("listen for rpc client");
        rpc_server.start();
        std::thread::sleep(Duration::from_millis(50));

        let conn = rpc_server
            .connect("127.0.0.1", diskio.port)
            .expect("connect to diskio");
        let dio_client = Arc::new(DiskioClient::new());
        dio_client.attach(&conn);

        // Wait for disks to be discovered.
        diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

        // Read-before-write on a fresh disk (not written by wait_for_disks).
        // Uses disk {0,2} zone 0 offset 1 MB — uninitialized area.
        let disk2 = DioDiskId::new(0, 2);
        test_read_before_write(&dio_client, &rpc_server, &conn, backend, disk2).await;

        // Test each IO size on disk {0,1} zone 0 offset 0.
        let disk1 = DioDiskId::new(0, 1);
        for &(size, label) in IO_SIZES {
            test_io_round(&dio_client, &rpc_server, &conn, backend, disk1, 0, 0, size, label).await;
        }

        // Test on a second disk {0,2} zone 1 with the middle size.
        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            backend,
            disk2,
            1,
            0,
            4096,
            "middle-disk2",
        )
        .await;

        // Test non-zero zone_offset: zone 0, offset 8192 (2 units in).
        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            backend,
            disk1,
            0,
            8192,
            4096,
            "middle-nz-offset",
        )
        .await;

        // Test non-zero DiskId.high: disk {0xAB, 4} (128-bit round-trip).
        let disk_hi = DioDiskId::new(0xAB, 4);
        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            backend,
            disk_hi,
            0,
            0,
            4096,
            "middle-high-disk",
        )
        .await;

        // Concurrent benchmark: 4 threads × 100 write+read cycles.
        eprintln!("=== {} concurrent benchmark ===", backend.name());
        run_concurrent_benchmark(&dio_client, &rpc_server, &conn, backend, disk1).await;

        // Shutdown this diskio instance.
        eprintln!("=== shutting down {} backend ===", backend.name());
        drop(diskio);
        rpc_server.stop();
    }

    eprintln!();
    eprintln!("disk_io_e2e_full_flow: ALL CHECKS PASSED");
}

// ── helper: connect to a diskio process ──────────────────────────

/// Connect a `DiskioClient` to a running `DiskioProcess`. Returns
/// (`rpc_server`, `connection`, `dio_client`) ready for I/O.
fn connect_to_diskio(
    diskio: &DiskioProcess,
) -> (Arc<RpcServer>, crow_rpc_ffi::Connection, Arc<DiskioClient>) {
    let rpc_server = Arc::new(RpcServer::new(None));
    rpc_server.listen("127.0.0.1", 0).expect("listen for rpc client");
    rpc_server.start();
    std::thread::sleep(Duration::from_millis(50));

    let conn = rpc_server
        .connect("127.0.0.1", diskio.port)
        .expect("connect to diskio");
    let dio_client = Arc::new(DiskioClient::new());
    dio_client.attach(&conn);
    (rpc_server, conn, dio_client)
}

// ── error paths test ─────────────────────────────────────────────

/// Verify client error decoding for `DiskNotExist`, `ZoneNotExist`,
/// and `IoError` (via fault injection). The client's
/// `await_*_response` must return `Err(DiskioError::IoError(code))`
/// with the correct code for each server-side error.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_error_paths() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    if crow_diskio_bin().is_none() {
        eprintln!("skipping: crow-diskio binary not found");
        return;
    }

    // 1. Start kv cluster + seed hardware.
    eprintln!("=== error-paths: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 2. Start diskio with MemDisk + auto-discover.
    eprintln!("=== error-paths: starting diskio (mem) ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        no_o_direct: false,
    });
    let (rpc_server, conn, dio_client) = connect_to_diskio(&diskio);
    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

    let valid_disk = DioDiskId::new(0, 1);
    let bad_disk = DioDiskId::new(99, 99);

    // 3. DiskNotExist: write to unknown disk.
    eprintln!("=== error-paths: DiskNotExist (write) ===");
    let wf = dio_client
        .write(&rpc_server, &conn, bad_disk, 0, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for write, got {result:?}"
    );
    eprintln!("  write to bad disk: DiskNotExist OK");

    // 4. DiskNotExist: read from unknown disk.
    eprintln!("=== error-paths: DiskNotExist (read) ===");
    let rf = dio_client
        .read(&rpc_server, &conn, bad_disk, 0, 0, 4096)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for read, got {result:?}"
    );
    eprintln!("  read from bad disk: DiskNotExist OK");

    // 5. DiskNotExist: fsync unknown disk.
    eprintln!("=== error-paths: DiskNotExist (fsync) ===");
    let ff = dio_client
        .fsync(&rpc_server, &conn, bad_disk)
        .expect("fsync send");
    let result = DiskioClient::await_fsync_response(ff).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for fsync, got {result:?}"
    );
    eprintln!("  fsync bad disk: DiskNotExist OK");

    // 6. ZoneNotExist: write to valid disk + invalid zone.
    eprintln!("=== error-paths: ZoneNotExist (write) ===");
    let wf = dio_client
        .write(&rpc_server, &conn, valid_disk, 99, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::ZoneNotExist))),
        "expected ZoneNotExist for write, got {result:?}"
    );
    eprintln!("  write to bad zone: ZoneNotExist OK");

    // 7. ZoneNotExist: read from valid disk + invalid zone.
    eprintln!("=== error-paths: ZoneNotExist (read) ===");
    let rf = dio_client
        .read(&rpc_server, &conn, valid_disk, 99, 0, 4096)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::ZoneNotExist))),
        "expected ZoneNotExist for read, got {result:?}"
    );
    eprintln!("  read from bad zone: ZoneNotExist OK");

    // Shutdown the clean diskio.
    drop(diskio);
    rpc_server.stop();

    // 8. IoError: start diskio with fault-error-rate=1.0 (all I/O fails).
    eprintln!("=== error-paths: IoError (fault injection) ===");
    let diskio_fault = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 1.0,
        no_o_direct: false,
    });
    let (rpc_server2, conn2, dio_client2) = connect_to_diskio(&diskio_fault);
    diskio_fault
        .wait_for_disks(&dio_client2, &rpc_server2, &conn2)
        .await;

    // Write should fail with IoError (fault injection returns -EIO).
    let wf = dio_client2
        .write(&rpc_server2, &conn2, valid_disk, 0, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::IoError))),
        "expected IoError for fault-injected write, got {result:?}"
    );
    eprintln!("  fault-injected write: IoError OK");

    // Read should also fail with IoError.
    let rf = dio_client2
        .read(&rpc_server2, &conn2, valid_disk, 0, 0, 4096)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::IoError))),
        "expected IoError for fault-injected read, got {result:?}"
    );
    eprintln!("  fault-injected read: IoError OK");

    drop(diskio_fault);
    rpc_server2.stop();

    eprintln!();
    eprintln!("disk_io_e2e_error_paths: ALL CHECKS PASSED");
}

// ── durability test ──────────────────────────────────────────────

/// Verify I2 (durability): write + fsync + process restart + read
/// returns the same data. Uses `BlockDisk` on a temp file with
/// `--no-o-direct` (so unaligned small writes work).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_durability() {
    if crow_diskio_bin().is_none() {
        eprintln!("skipping: crow-diskio binary not found");
        return;
    }

    // 1. Create a temp file and truncate it to 16 MB.
    let temp_dir = std::env::temp_dir();
    let data_path = temp_dir.join(format!("crow-diskio-durability-{}.dat", std::process::id()));
    eprintln!("=== durability: temp file {} ===", data_path.display());
    {
        let file = std::fs::File::create(&data_path).expect("create temp file");
        file.set_len(16 * 1024 * 1024).expect("truncate to 16 MB");
    }

    // Disk ID for the test: {0, 1}. Zone 0 covers the full 16 MB.
    let disk_id = DioDiskId::new(0, 1);
    let disk_arg = DiskArg {
        id_high: 0,
        id_low: 1,
        path: data_path.to_string_lossy().to_string(),
        zone_capacity: 16 * 1024 * 1024,
    };

    // 2. Start diskio with BlockDisk (no O_DIRECT, no kv-seeds).
    eprintln!("=== durability: starting diskio (first run) ===");
    let diskio1 = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "null",
        kv_seeds: &[],
        disks: std::slice::from_ref(&disk_arg),
        fault_error_rate: 0.0,
        no_o_direct: true,
    });
    let (rpc_server1, conn1, dio_client1) = connect_to_diskio(&diskio1);

    // 3. Write 4 KB of known data + fsync.
    let write_data: Vec<u8> = (0..4096u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    let wf = dio_client1
        .write(&rpc_server1, &conn1, disk_id, 0, 0, write_data.clone())
        .expect("write send");
    let wc = DiskioClient::await_write_response(wf).await.expect("write IO");
    assert_eq!(wc, DiskIoRetCode::Success, "durability write should succeed");

    let ff = dio_client1
        .fsync(&rpc_server1, &conn1, disk_id)
        .expect("fsync send");
    let fc = DiskioClient::await_fsync_response(ff).await.expect("fsync IO");
    assert_eq!(fc, DiskIoRetCode::Success, "durability fsync should succeed");
    eprintln!("  wrote + fsync'd 4 KB");

    // 4. Kill the process (simulating crash/restart).
    eprintln!("=== durability: killing diskio ===");
    drop(diskio1);
    rpc_server1.stop();

    // 5. Restart diskio with the same file.
    eprintln!("=== durability: restarting diskio (second run) ===");
    let diskio2 = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "null",
        kv_seeds: &[],
        disks: &[disk_arg],
        fault_error_rate: 0.0,
        no_o_direct: true,
    });
    let (rpc_server2, conn2, dio_client2) = connect_to_diskio(&diskio2);

    // 6. Read back and verify data matches.
    let rf = dio_client2
        .read(&rpc_server2, &conn2, disk_id, 0, 0, 4096)
        .expect("read send");
    let (rc, rd) = DiskioClient::await_read_response(rf).await.expect("read IO");
    assert_eq!(rc, DiskIoRetCode::Success, "durability read should succeed");
    let rd = rd.expect("read data should be present");
    assert_eq!(rd.len(), 4096, "durability read length mismatch");
    assert_eq!(
        rd, write_data,
        "durability: read after restart must match written data"
    );
    eprintln!("  read after restart: data matches");

    drop(diskio2);
    rpc_server2.stop();

    // Clean up the temp file.
    let _ = std::fs::remove_file(&data_path);

    eprintln!();
    eprintln!("disk_io_e2e_durability: ALL CHECKS PASSED");
}

// ── group-0 sync test ────────────────────────────────────────────

/// Verify group-0 periodic sync: (a) heartbeat registers the diskio
/// instance in the service registry, and (b) disk-list reconciliation
/// picks up a new disk added to group-0 after startup.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_group0_sync() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    if crow_diskio_bin().is_none() {
        eprintln!("skipping: crow-diskio binary not found");
        return;
    }

    // 1. Start kv cluster + seed hardware (3 disks only — we'll add a
    //    4th after diskio starts to test reconciliation).
    eprintln!("=== group0-sync: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware metadata seeded (3 initial disks)");

    // 2. Start diskio with auto-discover.
    eprintln!("=== group0-sync: starting diskio ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        no_o_direct: false,
    });
    let (rpc_server, conn, dio_client) = connect_to_diskio(&diskio);
    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

    // 3. Verify heartbeat: the service registry should have a "diskdb"
    //    entry for our instance_id with the diskio's gRPC endpoint.
    eprintln!("=== group0-sync: verifying heartbeat ===");
    let svc = cluster.make_service_registry_client();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut heartbeat_ok = false;
    while Instant::now() < deadline {
        if let Ok(Some(instance)) = svc.read_instance("diskdb", INSTANCE_ID).await {
            eprintln!(
                "  service registry: found diskdb instance {} at {}",
                instance.instance_id, instance.grpc_endpoint
            );
            assert!(
                !instance.grpc_endpoint.is_empty(),
                "heartbeat should register a non-empty grpc_endpoint"
            );
            heartbeat_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        heartbeat_ok,
        "diskio should heartbeat to the service registry within 15s"
    );
    eprintln!("  heartbeat verified");

    // 4. Disk-list reconciliation: add a new disk to group-0 after
    //    diskio is running. The next sync cycle should discover it.
    eprintln!("=== group0-sync: adding new disk to group-0 ===");
    let new_disk_id = make_disk_id(0, 42);
    hw.add_disk(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &new_disk_id,
        &DiskValue {
            disk_type: DiskType::BlockSsd as i32,
            capacity_units: CAPACITY_UNITS,
            zone_size_units: ZONE_SIZE_UNITS,
            unit_size_bytes: UNIT_SIZE_BYTES,
            zone_count: ZONE_COUNT,
            status: HwStatus::Up as i32,
            device_path: String::new(),
        },
    )
    .await
    .expect("add new disk");

    // Update the disk-group to include the new disk.
    let all_disk_ids = vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0xAB, 4),
        new_disk_id,
    ];
    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: all_disk_ids,
        },
    )
    .await
    .expect("update disk-group");
    eprintln!("  added disk {new_disk_id:?} to group-0");

    // 5. Wait for the sync interval (2s) + margin, then write to the
    //    new disk to verify it was reconciled.
    eprintln!("=== group0-sync: waiting for reconciliation ===");
    let new_disk_dio = DioDiskId::new(0, 42);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut reconciled = false;
    while Instant::now() < deadline {
        let wf = dio_client
            .write(&rpc_server, &conn, new_disk_dio, 0, 0, vec![0xCD; 4096])
            .expect("reconcile write send");
        match DiskioClient::await_write_response(wf).await {
            Ok(DiskIoRetCode::Success) => {
                reconciled = true;
                break;
            }
            Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist)) => {
                // Not yet reconciled — keep waiting.
            }
            other => {
                eprintln!("  reconcile write unexpected result: {other:?}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        reconciled,
        "diskio should reconcile the new disk within 20s. Log:\n{}",
        diskio.log_content()
    );
    eprintln!("  new disk reconciled and writable");

    // 6. Verify read-back on the reconciled disk.
    let rf = dio_client
        .read(&rpc_server, &conn, new_disk_dio, 0, 0, 4096)
        .expect("reconcile read send");
    let (rc, rd) = DiskioClient::await_read_response(rf)
        .await
        .expect("reconcile read IO");
    assert_eq!(rc, DiskIoRetCode::Success, "reconciled disk read should succeed");
    let rd = rd.expect("reconciled read data should be present");
    assert_eq!(rd, vec![0xCD; 4096], "reconciled disk data should match");
    eprintln!("  reconciled disk read-back verified");

    drop(diskio);
    rpc_server.stop();

    eprintln!();
    eprintln!("disk_io_e2e_group0_sync: ALL CHECKS PASSED");
}
