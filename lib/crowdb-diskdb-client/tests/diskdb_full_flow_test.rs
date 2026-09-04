// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E full-flow test: smoke test (allocate / free / query drill-down)
//! + concurrent benchmark + compact-and-reclaim.
//!
//! Persist-only free model: free keeps bitmap set, compaction reclaims.

use std::sync::Arc;

use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, CommitBlocksRequest, CompactZoneRequest, FreeBlocksRequest,
};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::*;
use crowdb_test_harness::hardware::{
    make_disk_id, seed_hardware, standard_disk_ids_3, CAPACITY_UNITS, DG_ID, UNIT_SIZE_BYTES, ZONE_COUNT,
};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_client_e2e_full_flow() {
    if !check_binaries() {
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
    seed_hardware(&hw, &standard_disk_ids_3()).await;
    eprintln!("hardware metadata seeded (rack=1, node=10, dg=100, 3 disks)");

    // 3. Start crowdb-diskdb subprocess.
    eprintln!("=== starting crowdb-diskdb ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;
    eprintln!(
        "crowdb-diskdb started: listen=127.0.0.1:{}, http=127.0.0.1:{}",
        diskdb.listen_port, diskdb.http_port
    );

    // 4. Build the DiskdbClient and refresh endpoints.
    let svc = cluster.make_service_registry_client();
    let transport = Arc::new(DiskdbRpcTransport::new());
    let client = Arc::new(DiskdbClient::new(svc, transport).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: std::time::Duration::from_millis(100),
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

    let commit_resp = client
        .commit_blocks(CommitBlocksRequest {
            segments: alloc_resp.segments.clone(),
        })
        .await
        .expect("commit 3 blocks");
    assert_eq!(commit_resp.committed_count, 3, "expected 3 committed");

    // Query disk-group info.
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

    // Free the 3 committed blocks.
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

    // 7. Concurrent benchmark.
    eprintln!("=== concurrent benchmark ===");
    run_concurrent_benchmark(&client).await;

    // 8. Compact + reclaim: verify the persist-only free model.
    eprintln!("=== compact + reclaim ===");
    let total_cap_bytes = 3 * CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES);
    let unit_bytes = u64::from(UNIT_SIZE_BYTES);

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
                zone_indices: vec![],
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
