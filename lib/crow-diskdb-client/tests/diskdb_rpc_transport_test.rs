// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! E2E test for the crow-rpc transport (R115 migration).
//!
//! Runs the same allocate / free / commit / query / compact / scan flows
//! as `diskdb_full_flow_test`, but with `with_rpc_transport` enabled so
//! all RPCs go through the crow-rpc flatbuffer transport instead of
//! tonic gRPC.

use std::sync::Arc;

use crow_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crow_protocol::diskdb::rpc::{
    AllocateBlocksRequest, CompactZoneRequest, FreeBlocksRequest, RecalcDiskUsageRequest,
};
use crow_test_harness::cluster::KvCluster;
use crow_test_harness::diskdb::*;
use crow_test_harness::hardware::{
    make_disk_id, seed_hardware, standard_disk_ids_3, CAPACITY_UNITS, DG_ID, UNIT_SIZE_BYTES, ZONE_COUNT,
};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_rpc_transport_e2e() {
    if !check_binaries() {
        return;
    }

    // 1. Start the single-node kv cluster.
    eprintln!("=== [rpc] starting kv cluster ===");
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0={}, group1={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware metadata into group 0.
    eprintln!("=== [rpc] seeding hardware metadata ===");
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_3()).await;
    eprintln!("hardware metadata seeded (rack=1, node=10, dg=100, 3 disks)");

    // 3. Start crow-diskdb subprocess (now with rpc_listen_addr).
    eprintln!("=== [rpc] starting crow-diskdb ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;
    eprintln!(
        "crow-diskdb started: grpc=127.0.0.1:{}, rpc=127.0.0.1:{}, http=127.0.0.1:{}",
        diskdb.grpc_port, diskdb.rpc_port, diskdb.http_port
    );

    // 4. Build the DiskdbClient with crow-rpc transport enabled.
    let svc = cluster.make_service_registry_client();
    let transport = Arc::new(DiskdbRpcTransport::new());
    let client = Arc::new(DiskdbClient::new(svc, transport).with_retry_config(RetryConfig {
        max_retries: 5,
        initial_backoff: std::time::Duration::from_millis(100),
    }));
    client.refresh_endpoints().await.expect("refresh endpoints");
    eprintln!("diskdb client endpoints refreshed (rpc transport enabled)");

    // 5. Allocate 3 blocks via crow-rpc.
    eprintln!("=== [rpc] allocate / free / commit ===");
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
        .expect("[rpc] allocate 3 blocks");
    assert_eq!(
        alloc_resp.segments.len(),
        3,
        "[rpc] expected 3 segments from allocate"
    );
    eprintln!("  [rpc] allocated 3 blocks:");
    for (i, seg) in alloc_resp.segments.iter().enumerate() {
        eprintln!(
            "    seg[{i}]: disk={:?} zone={} offset={} count={}",
            seg.disk_id, seg.zone_index, seg.unit_offset, seg.unit_count
        );
        assert_eq!(seg.unit_count, 1, "[rpc] segment {i} should have unit_count=1");
    }

    // Query disk-group info via crow-rpc.
    let dg_info = client
        .get_disk_group_info(DG_ID)
        .await
        .expect("[rpc] get_disk_group_info");
    let group = dg_info.group.expect("[rpc] disk-group info should have group");
    assert_eq!(group.disk_group_id, DG_ID, "[rpc] disk-group id mismatch");
    assert!(!group.disk_ids.is_empty(), "[rpc] disk-group should have disks");
    eprintln!(
        "  [rpc] get_disk_group_info: dg={} disks={}",
        group.disk_group_id,
        group.disk_ids.len()
    );

    // Query capacity stats via crow-rpc.
    let cap = client
        .query_disk_group(DG_ID)
        .await
        .expect("[rpc] query_disk_group");
    let dg_cap = &cap.disk_groups[0];
    eprintln!(
        "  [rpc] capacity: busy={} free={} cap={}",
        dg_cap.busy_bytes, dg_cap.free_bytes, dg_cap.capacity_bytes
    );
    assert!(
        dg_cap.capacity_bytes > 0,
        "[rpc] capacity should be non-zero after sync"
    );

    // Free the 3 blocks via crow-rpc.
    let free_req = FreeBlocksRequest {
        segments: alloc_resp.segments.clone(),
    };
    let free_resp = client.free_blocks(free_req).await.expect("[rpc] free 3 blocks");
    assert_eq!(free_resp.freed_count, 3, "[rpc] expected 3 freed");
    eprintln!("  [rpc] freed 3 blocks (freed_count={})", free_resp.freed_count);

    eprintln!("[rpc] allocate / free: ALL CHECKS PASSED");

    // 6. Query drill-down: get_disk_info, query_disk, query_zone.
    eprintln!("=== [rpc] query drill-down ===");
    let test_disk = make_disk_id(0, 1);

    let disk_info = client
        .get_disk_info(DG_ID, test_disk)
        .await
        .expect("[rpc] get_disk_info");
    let di = disk_info.disk.expect("[rpc] disk info should have disk");
    assert_eq!(di.disk_group_id, DG_ID, "[rpc] disk_group_id mismatch");
    assert_eq!(di.disk_id, Some(test_disk), "[rpc] disk_id mismatch");
    assert_eq!(di.zone_count, ZONE_COUNT, "[rpc] zone_count mismatch");
    assert_eq!(
        di.capacity_bytes,
        CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES),
        "[rpc] capacity_bytes mismatch"
    );
    eprintln!(
        "  [rpc] get_disk_info: dg={} disk={:?} zones={} cap={} busy={} free={}",
        di.disk_group_id, di.disk_id, di.zone_count, di.capacity_bytes, di.busy_bytes, di.free_bytes
    );

    let disk_cap = client
        .query_disk(DG_ID, test_disk)
        .await
        .expect("[rpc] query_disk");
    let dg_from_disk = &disk_cap.disk_groups[0];
    let disk_from_query = dg_from_disk
        .disks
        .iter()
        .find(|d| d.disk_id == Some(test_disk))
        .expect("[rpc] query_disk should return the queried disk");
    assert!(
        !disk_from_query.zone_usages.is_empty(),
        "[rpc] query_disk should return per-zone entries"
    );
    eprintln!(
        "  [rpc] query_disk: {} zone entries, busy={} free={}",
        disk_from_query.zone_usages.len(),
        disk_from_query.busy_bytes,
        disk_from_query.free_bytes
    );

    let zone_cap = client
        .query_zone(DG_ID, test_disk, 0)
        .await
        .expect("[rpc] query_zone");
    let zone_dg = &zone_cap.disk_groups[0];
    let zone_disk = zone_dg
        .disks
        .iter()
        .find(|d| d.disk_id == Some(test_disk))
        .expect("[rpc] query_zone should return the queried disk");
    let zone_usage = &zone_disk.zone_usages[0];
    assert_eq!(zone_usage.zone_index, 0, "[rpc] zone_index mismatch");
    let bitmap_len = zone_usage.usage_bitmap.as_ref().map_or(0, Vec::len);
    assert!(bitmap_len > 0, "[rpc] query_zone should populate usage_bitmap");
    eprintln!(
        "  [rpc] query_zone: zone 0, bitmap {bitmap_len} bytes, busy={} free={}",
        zone_usage.busy_bytes, zone_usage.free_bytes
    );

    eprintln!("[rpc] query drill-down: ALL CHECKS PASSED");

    // 7. Recalc disk usage via crow-rpc.
    eprintln!("=== [rpc] recalc disk usage ===");
    let recalc_resp = client
        .recalc_disk_usage(RecalcDiskUsageRequest { disk_group_id: None })
        .await
        .expect("[rpc] recalc_disk_usage");
    eprintln!("  [rpc] recalc: {} disk-group results", recalc_resp.results.len());
    assert!(
        !recalc_resp.results.is_empty(),
        "[rpc] recalc should return at least one disk-group result"
    );
    eprintln!("[rpc] recalc: ALL CHECKS PASSED");

    // 8. Compact + reclaim via crow-rpc.
    eprintln!("=== [rpc] compact + reclaim ===");
    let total_cap_bytes = 3 * CAPACITY_UNITS * u64::from(UNIT_SIZE_BYTES);
    let unit_bytes = u64::from(UNIT_SIZE_BYTES);

    let cap_before = client
        .query_disk_group(DG_ID)
        .await
        .expect("[rpc] query before compact");
    let busy_before = cap_before.disk_groups[0].busy_bytes;
    let free_before = cap_before.disk_groups[0].free_bytes;
    eprintln!(
        "  [rpc] before compact: busy={} ({} units) free={} ({} units) cap={}",
        busy_before,
        busy_before / unit_bytes,
        free_before,
        free_before / unit_bytes,
        total_cap_bytes
    );
    assert!(
        busy_before > 0,
        "[rpc] persist-only free should keep bitmap bits set (busy > 0)"
    );

    // Compact all zones on all 3 disks.
    let disk_ids = [make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)];
    let mut total_compacted_zones = 0u32;
    for did in &disk_ids {
        let resp = client
            .compact_zone(CompactZoneRequest {
                disk_id: Some(*did),
                zone_indices: vec![],
            })
            .await
            .expect("[rpc] compact_zone");
        total_compacted_zones += resp.compacted_zone_count;
        assert!(
            resp.zones.iter().all(|z| z.success),
            "[rpc] all zone compaction results should be success for disk {did:?}"
        );
    }
    eprintln!("  [rpc] compacted {total_compacted_zones} zones");

    let cap_after = client
        .query_disk_group(DG_ID)
        .await
        .expect("[rpc] query after compact");
    let busy_after = cap_after.disk_groups[0].busy_bytes;
    let free_after = cap_after.disk_groups[0].free_bytes;
    eprintln!(
        "  [rpc] after compact: busy={} ({} units) free={} ({} units) cap={}",
        busy_after,
        busy_after / unit_bytes,
        free_after,
        free_after / unit_bytes,
        total_cap_bytes
    );
    assert_eq!(busy_after, 0, "[rpc] after compaction busy should be 0");
    assert_eq!(
        free_after, total_cap_bytes,
        "[rpc] after compaction all capacity should be free"
    );

    // Verify space is reclaimable: allocate 3 blocks.
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
        .expect("[rpc] allocate after compaction should succeed");
    assert_eq!(
        reclaim_resp.segments.len(),
        3,
        "[rpc] should allocate 3 blocks after compaction"
    );
    eprintln!("  [rpc] allocated 3 blocks after compaction (space reclaimed)");

    // Clean up.
    let cleanup_req = FreeBlocksRequest {
        segments: reclaim_resp.segments,
    };
    let cleanup_resp = client.free_blocks(cleanup_req).await.expect("[rpc] cleanup free");
    assert_eq!(cleanup_resp.freed_count, 3, "[rpc] cleanup free should free 3");

    eprintln!("[rpc] compact + reclaim: ALL CHECKS PASSED");

    // 9. Trigger scan + get scan status via crow-rpc.
    eprintln!("=== [rpc] trigger scan + get scan status ===");
    let trigger_resp = client
        .trigger_scan(Some(DG_ID))
        .await
        .expect("[rpc] trigger_scan");
    eprintln!(
        "  [rpc] trigger_scan: scan_in_progress={}",
        trigger_resp.scan_in_progress
    );

    let status_resp = client
        .get_scan_status(Some(DG_ID))
        .await
        .expect("[rpc] get_scan_status");
    eprintln!("  [rpc] get_scan_status: has_run={}", status_resp.has_run);
    if let Some(summary) = &status_resp.summary {
        eprintln!(
            "  [rpc] scan summary: zones_scanned={} duration_ms={}",
            summary.zones_scanned, summary.duration_ms
        );
    }
    eprintln!("[rpc] trigger scan + get scan status: ALL CHECKS PASSED");

    // 10. Rebuild zone bitmap via crow-rpc.
    eprintln!("=== [rpc] rebuild zone bitmap ===");
    let rebuild_resp = client
        .rebuild_zone_bitmap(test_disk, 0)
        .await
        .expect("[rpc] rebuild_zone_bitmap");
    eprintln!(
        "  [rpc] rebuild_zone_bitmap: rebuilt={} busy={} free={}",
        rebuild_resp.rebuilt_zone_count, rebuild_resp.total_busy_units, rebuild_resp.total_free_units
    );
    assert_eq!(rebuild_resp.rebuilt_zone_count, 1, "[rpc] should rebuild 1 zone");
    eprintln!("[rpc] rebuild zone bitmap: ALL CHECKS PASSED");

    eprintln!();
    eprintln!("diskdb_rpc_transport_e2e: ALL CHECKS PASSED");
}
