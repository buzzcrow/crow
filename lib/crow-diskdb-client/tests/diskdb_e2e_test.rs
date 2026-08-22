// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! End-to-end diskdb client test suite.
//!
//! Starts a real single-node `crow-kv-server` (store 0, groups 0 and
//! 1), seeds hardware metadata into group 0, starts `crow-diskdb` as a
//! subprocess (syncs from group 0, loads zones, registers with the
//! service registry), then exercises the full gRPC client path via
//! `DiskdbClient`.
//!
//! Two test functions:
//!
//! - **`diskdb_client_e2e_full_flow`** — smoke test (allocate / free /
//!   query drill-down) + concurrent benchmark + compact-and-reclaim
//!   (persist-only model: free keeps bitmap set, compaction reclaims).
//!
//! - **`diskdb_client_e2e_validate_owner`** — starts diskdb with
//!   `validate_owner_on_free = true`, verifies that freeing with a
//!   wrong `owner_chunk` is rejected (`PermissionDenied`) and freeing
//!   with the correct owner succeeds.

mod common;

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::cluster::KvCluster;
use crow_diskdb_client::{DiskdbClient, DiskdbClientError, RetryConfig};
use crow_kv_client::HardwareClient;
use crow_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{
    AllocateBlocksRequest, CompactZoneRequest, DiskGroupValue, DiskType, DiskValue, FreeBlocksRequest,
    Segment,
};

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: u64 = 100;
const INSTANCE_ID: u64 = 999;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;

// Small disks: 4 zones × 128 units × 1 MB = 512 MB per disk.
// 3 disks → 1536 units total — enough for the benchmark (4 × 100 = 400).
const ZONE_SIZE_UNITS: u64 = 128;
const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
const ZONE_COUNT: u32 = 4;
const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * ZONE_COUNT as u64;

fn make_disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

fn make_chunk_id(high: u64, low: u64) -> ChunkId {
    ChunkId { high, low }
}

// ── binary discovery ─────────────────────────────────────────────

fn crow_kv_server_bin() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("CROW_KV_SERVER_BIN") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
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

fn crow_diskdb_bin() -> Option<std::path::PathBuf> {
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

// ── free port helper ─────────────────────────────────────────────

/// Bind a TCP socket to `127.0.0.1:0`, read the assigned port, then
/// close the socket. The port may be reused by the OS before the
/// caller binds to it, but this is sufficient for test isolation.
fn find_free_port() -> i32 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind 0")
        .local_addr()
        .expect("local_addr")
        .port()
        .into()
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

    let disk_ids = vec![make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)];
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

// ── diskdb subprocess ────────────────────────────────────────────

struct DiskdbProcess {
    child: std::process::Child,
    grpc_port: i32,
    http_port: i32,
    _config_file: tempfile::NamedTempFile,
    log_path: std::path::PathBuf,
}

impl DiskdbProcess {
    fn log_content(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Start crow-diskdb with a generated config pointing at the
    /// kv-server management seeds. The diskdb syncs from group 0,
    /// discovers its owned disk-group, loads zones, and registers
    /// with the service registry. When `validate_owner` is true, the
    /// `[storage]` section sets `validate_owner_on_free = true` so the
    /// free path rejects segments whose `owner_chunk` doesn't match
    /// the persisted `BusyBlockValue`.
    fn start(kv_seeds: &[String], validate_owner: bool) -> Self {
        let bin = crow_diskdb_bin().unwrap_or_else(|| {
            panic!("crow-diskdb binary not found; set CROW_DISKDB_BIN or build app/crow-diskdb")
        });

        let grpc_port = find_free_port();
        let http_port = find_free_port();

        // Generate a minimal config file. Only [server] is required;
        // other sections use validated defaults. When a section IS
        // present, all its fields must be specified (no per-field
        // serde defaults). The [storage] section is included only
        // when validate_owner is true (to override the default false).
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
            http_port,
            _config_file: config_file,
            log_path,
        }
    }

    /// Wait for the diskdb HTTP `/ready` endpoint to return 200
    /// (phase = "up"), meaning zone loading is complete and the
    /// instance is ready to serve mutating RPCs.
    async fn wait_for_ready(&self) {
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
/// Reports throughput and verifies zero errors under parallel load.
///
/// The persist-only free model means freed blocks do not reclaim
/// bitmap space — each cycle consumes 1 unit. Total consumption =
/// `BENCH_THREADS × BENCH_CYCLES` = 400 units, well within the 1536
/// unit capacity.
#[allow(clippy::cast_precision_loss)]
async fn run_concurrent_benchmark(client: &Arc<DiskdbClient>) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(BENCH_THREADS);

    for tid in 0..BENCH_THREADS {
        let client = Arc::clone(client);
        handles.push(tokio::spawn(async move {
            let mut ok = 0usize;
            let mut errors = 0usize;
            for i in 0..BENCH_CYCLES {
                let owner = make_chunk_id(u64::try_from(tid).unwrap(), u64::try_from(i).unwrap());

                // Allocate 1 block.
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

                // Free the allocated block.
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

// ── main test ────────────────────────────────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_client_e2e_full_flow() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    if crow_diskdb_bin().is_none() {
        eprintln!("skipping: crow-diskdb binary not found");
        return;
    }

    // 1. Start the single-node kv cluster.
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
    eprintln!("hardware metadata seeded (rack={RACK_ID}, node={NODE_ID}, dg={DG_ID}, 3 disks)");

    // 3. Start crow-diskdb subprocess.
    eprintln!("=== starting crow-diskdb ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;
    eprintln!(
        "crow-diskdb started: grpc=127.0.0.1:{}, http=127.0.0.1:{}",
        diskdb.grpc_port, diskdb.http_port
    );

    // 4. Build the DiskdbClient and refresh endpoints from the
    //    service registry. The diskdb registered itself during the
    //    initial keepalive tick.
    let svc = cluster.make_service_registry_client();
    let client = Arc::new(DiskdbClient::new(svc).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
    }));
    client.refresh_endpoints().await.expect("refresh endpoints");
    eprintln!("diskdb client endpoints refreshed");

    // 5. Smoke test: allocate 3 blocks, verify, free them.
    eprintln!("=== smoke test: allocate / free ===");
    let owner = make_chunk_id(0, 42);
    let alloc_req = AllocateBlocksRequest {
        disk_group_id: DG_ID,
        unit_count: 1,
        count: 3,
        exclude_disk_ids: vec![],
        owner_chunk: Some(owner),
    };
    let alloc_resp = client
        .allocate_blocks(alloc_req)
        .await
        .expect("allocate 3 blocks");
    assert_eq!(alloc_resp.segments.len(), 3, "expected 3 segments from allocate");
    eprintln!("  allocated 3 blocks:");
    for (i, seg) in alloc_resp.segments.iter().enumerate() {
        eprintln!(
            "    seg[{i}]: disk={:?} zone={} offset={} count={}",
            seg.disk_id, seg.zone_index, seg.unit_offset, seg.unit_count
        );
        assert_eq!(seg.unit_count, 1, "segment {i} should have unit_count=1");
    }

    // Query disk-group info to verify the diskdb is serving reads.
    let dg_info = client
        .get_disk_group_info(DG_ID)
        .await
        .expect("get_disk_group_info");
    let group = dg_info.group.expect("disk-group info should have group");
    assert_eq!(group.disk_group_id, DG_ID, "disk-group id mismatch");
    assert!(!group.disk_ids.is_empty(), "disk-group should have disks");
    eprintln!(
        "  get_disk_group_info: dg={} disks={}",
        group.disk_group_id,
        group.disk_ids.len()
    );

    // Query capacity stats.
    let cap = client.query_disk_group(DG_ID).await.expect("query_disk_group");
    let dg_cap = &cap.disk_groups[0];
    eprintln!(
        "  capacity: busy={} free={} cap={}",
        dg_cap.busy_bytes, dg_cap.free_bytes, dg_cap.capacity_bytes
    );
    assert!(
        dg_cap.capacity_bytes > 0,
        "capacity should be non-zero after sync"
    );

    // Free the 3 blocks.
    let free_req = FreeBlocksRequest {
        segments: alloc_resp.segments.clone(),
    };
    let free_resp = client.free_blocks(free_req).await.expect("free 3 blocks");
    assert_eq!(free_resp.freed_count, 3, "expected 3 freed");
    eprintln!("  freed 3 blocks (freed_count={})", free_resp.freed_count);

    eprintln!("smoke test: ALL CHECKS PASSED");

    // 6. Query drill-down: get_disk_info, query_disk, query_zone.
    eprintln!("=== query drill-down ===");
    let test_disk = make_disk_id(0, 1);

    // get_disk_info — verify disk key + value fields.
    let disk_info = client
        .get_disk_info(DG_ID, test_disk)
        .await
        .expect("get_disk_info");
    let di = disk_info.disk.expect("disk info should have disk");
    assert_eq!(di.disk_group_id, DG_ID, "disk_group_id mismatch");
    assert_eq!(di.disk_id, Some(test_disk), "disk_id mismatch");
    assert_eq!(di.zone_count, ZONE_COUNT, "zone_count mismatch");
    assert_eq!(
        di.capacity_bytes,
        CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES),
        "capacity_bytes mismatch"
    );
    eprintln!(
        "  get_disk_info: dg={} disk={:?} zones={} cap={} busy={} free={}",
        di.disk_group_id, di.disk_id, di.zone_count, di.capacity_bytes, di.busy_bytes, di.free_bytes
    );

    // query_disk — verify per-zone brief entries are present.
    let disk_cap = client.query_disk(DG_ID, test_disk).await.expect("query_disk");
    let dg_from_disk = &disk_cap.disk_groups[0];
    let disk_from_query = dg_from_disk
        .disks
        .iter()
        .find(|d| d.disk_id == Some(test_disk))
        .expect("query_disk should return the queried disk");
    assert!(
        !disk_from_query.zone_usages.is_empty(),
        "query_disk should return per-zone entries"
    );
    eprintln!(
        "  query_disk: {} zone entries, busy={} free={}",
        disk_from_query.zone_usages.len(),
        disk_from_query.busy_bytes,
        disk_from_query.free_bytes
    );

    // query_zone — verify usage_bitmap is populated for a specific zone.
    let zone_cap = client.query_zone(DG_ID, test_disk, 0).await.expect("query_zone");
    let zone_dg = &zone_cap.disk_groups[0];
    let zone_disk = zone_dg
        .disks
        .iter()
        .find(|d| d.disk_id == Some(test_disk))
        .expect("query_zone should return the queried disk");
    let zone_usage = &zone_disk.zone_usages[0];
    assert_eq!(zone_usage.zone_index, 0, "zone_index mismatch");
    let bitmap_len = zone_usage.usage_bitmap.as_ref().map_or(0, Vec::len);
    assert!(bitmap_len > 0, "query_zone should populate usage_bitmap");
    eprintln!(
        "  query_zone: zone 0, bitmap {bitmap_len} bytes, busy={} free={}",
        zone_usage.busy_bytes, zone_usage.free_bytes
    );

    eprintln!("query drill-down: ALL CHECKS PASSED");

    // 7. Concurrent benchmark: 4 threads × 100 allocate+free cycles.
    eprintln!("=== concurrent benchmark ===");
    run_concurrent_benchmark(&client).await;

    // 8. Compact + reclaim: verify the persist-only free model.
    //    After the smoke test (3 freed) + benchmark (400 freed), 403
    //    blocks are freed but their bitmap bits are still set
    //    (persist-only). Compaction is the sole bit-clearer. We
    //    compact all zones on all 3 disks, then verify space is
    //    reclaimable by allocating again.
    eprintln!("=== compact + reclaim ===");
    let total_cap_bytes = 3 * CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES);
    let unit_bytes = u64::from(UNIT_SIZE_BYTES);

    // Before compaction: 403 bits set (persist-only free keeps bitmap).
    let cap_before = client
        .query_disk_group(DG_ID)
        .await
        .expect("query before compact");
    let busy_before = cap_before.disk_groups[0].busy_bytes;
    let free_before = cap_before.disk_groups[0].free_bytes;
    eprintln!(
        "  before compact: busy={} ({} units) free={} ({} units) cap={}",
        busy_before,
        busy_before / unit_bytes,
        free_before,
        free_before / unit_bytes,
        total_cap_bytes
    );
    assert!(
        busy_before > 0,
        "persist-only free should keep bitmap bits set (busy > 0)"
    );
    assert_eq!(
        busy_before + free_before,
        total_cap_bytes,
        "busy + free should equal capacity"
    );

    // Compact all zones on all 3 disks.
    let disk_ids = [make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)];
    let mut total_compacted_zones = 0u32;
    let mut total_free_records_deleted = 0u32;
    for did in &disk_ids {
        let resp = client
            .compact_zone(CompactZoneRequest {
                disk_id: Some(*did),
                zone_indices: vec![], // empty = all zones
            })
            .await
            .expect("compact_zone");
        total_compacted_zones += resp.compacted_zone_count;
        total_free_records_deleted += resp.total_free_records_deleted;
        assert!(
            resp.zones.iter().all(|z| z.success),
            "all zone compaction results should be success for disk {did:?}"
        );
    }
    eprintln!("  compacted {total_compacted_zones} zones, deleted {total_free_records_deleted} free records");

    // After compaction: bitmap cleared for freed blocks → busy = 0.
    let cap_after = client.query_disk_group(DG_ID).await.expect("query after compact");
    let busy_after = cap_after.disk_groups[0].busy_bytes;
    let free_after = cap_after.disk_groups[0].free_bytes;
    eprintln!(
        "  after compact: busy={} ({} units) free={} ({} units) cap={}",
        busy_after,
        busy_after / unit_bytes,
        free_after,
        free_after / unit_bytes,
        total_cap_bytes
    );
    assert_eq!(
        busy_after, 0,
        "after compaction all freed bits should be cleared (busy = 0)"
    );
    assert_eq!(
        free_after, total_cap_bytes,
        "after compaction all capacity should be free"
    );

    // Verify space is reclaimable: allocate 3 blocks (should succeed).
    let reclaim_owner = make_chunk_id(0, 77);
    let reclaim_req = AllocateBlocksRequest {
        disk_group_id: DG_ID,
        unit_count: 1,
        count: 3,
        exclude_disk_ids: vec![],
        owner_chunk: Some(reclaim_owner),
    };
    let reclaim_resp = client
        .allocate_blocks(reclaim_req)
        .await
        .expect("allocate after compaction should succeed");
    assert_eq!(
        reclaim_resp.segments.len(),
        3,
        "should allocate 3 blocks after compaction"
    );
    eprintln!("  allocated 3 blocks after compaction (space reclaimed)");

    // Clean up: free the 3 blocks.
    let cleanup_req = FreeBlocksRequest {
        segments: reclaim_resp.segments,
    };
    let cleanup_resp = client.free_blocks(cleanup_req).await.expect("cleanup free");
    assert_eq!(cleanup_resp.freed_count, 3, "cleanup free should free 3");

    eprintln!("compact + reclaim: ALL CHECKS PASSED");

    eprintln!();
    eprintln!("diskdb_client_e2e_full_flow: ALL CHECKS PASSED");
}

/// Validate-owner-on-free: starts diskdb with
/// `validate_owner_on_free = true`, allocates a block, then verifies
/// that freeing with a wrong `owner_chunk` is rejected
/// (`PermissionDenied`) and freeing with the correct owner succeeds.
/// Also verifies that freeing a non-busy block returns `NotFound`.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_client_e2e_validate_owner() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    if crow_diskdb_bin().is_none() {
        eprintln!("skipping: crow-diskdb binary not found");
        return;
    }

    // 1. Start kv cluster + seed hardware.
    eprintln!("=== validate-owner: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 2. Start diskdb with validate_owner_on_free = true.
    eprintln!("=== validate-owner: starting crow-diskdb (validate_owner=true) ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, true);
    diskdb.wait_for_ready().await;

    // 3. Build client + refresh endpoints.
    let svc = cluster.make_service_registry_client();
    let client = Arc::new(DiskdbClient::new(svc).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
    }));
    client.refresh_endpoints().await.expect("refresh endpoints");

    // 4. Allocate 1 block with owner = chunk A.
    let owner_a = make_chunk_id(0, 42);
    let alloc_resp = client
        .allocate_blocks(AllocateBlocksRequest {
            disk_group_id: DG_ID,
            unit_count: 1,
            count: 1,
            exclude_disk_ids: vec![],
            owner_chunk: Some(owner_a),
        })
        .await
        .expect("allocate 1 block");
    assert_eq!(alloc_resp.segments.len(), 1, "expected 1 segment");
    let seg = &alloc_resp.segments[0];
    eprintln!(
        "  allocated: disk={:?} zone={} offset={}",
        seg.disk_id, seg.zone_index, seg.unit_offset
    );

    // 5. Free with wrong owner → PermissionDenied.
    let owner_b = make_chunk_id(0, 999);
    let wrong_seg = Segment {
        disk_id: seg.disk_id,
        zone_index: seg.zone_index,
        unit_offset: seg.unit_offset,
        unit_count: seg.unit_count,
        owner_chunk: Some(owner_b),
    };
    let result = client
        .free_blocks(FreeBlocksRequest {
            segments: vec![wrong_seg],
        })
        .await;
    assert!(
        matches!(&result, Err(DiskdbClientError::Rpc(msg)) if msg.contains("permission denied")),
        "expected permission denied error for wrong owner, got {result:?}"
    );
    eprintln!("  free with wrong owner: rejected (PermissionDenied)");

    // 6. Free with correct owner → success.
    let free_resp = client
        .free_blocks(FreeBlocksRequest {
            segments: alloc_resp.segments.clone(),
        })
        .await
        .expect("free with correct owner should succeed");
    assert_eq!(free_resp.freed_count, 1, "expected 1 freed");
    eprintln!("  free with correct owner: succeeded (freed_count=1)");

    // 7. Free a non-busy block → NotFound.
    let fake_seg = Segment {
        disk_id: seg.disk_id,
        zone_index: seg.zone_index,
        unit_offset: 999_999, // non-existent offset
        unit_count: 1,
        owner_chunk: Some(owner_a),
    };
    let result = client
        .free_blocks(FreeBlocksRequest {
            segments: vec![fake_seg],
        })
        .await;
    assert!(
        matches!(&result, Err(DiskdbClientError::Rpc(msg)) if msg.contains("not found")),
        "expected not found error for non-busy block, got {result:?}"
    );
    eprintln!("  free non-busy block: rejected (NotFound)");

    eprintln!();
    eprintln!("diskdb_client_e2e_validate_owner: ALL CHECKS PASSED");
}
