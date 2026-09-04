// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client E2E coverage for routing and compaction-time free validation.

use std::sync::Arc;
use std::time::Duration;

use crowdb_diskdb_client::{DiskdbClient, DiskdbRpcTransport, RetryConfig};
use crowdb_protocol::diskdb::rpc::{AllocateBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::*;
use crowdb_test_harness::hardware::{seed_hardware, standard_disk_ids_3, DG_ID, UNIT_SIZE_BYTES};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn compaction_rejects_mismatched_free_facts() {
    require_binaries();

    // 1. Start kv cluster + seed hardware.
    eprintln!("=== validate-owner: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_3()).await;

    // 2. Start diskdb.
    eprintln!("=== validate-owner: starting crowdb-diskdb (validate_owner=true) ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, true);
    diskdb.wait_for_ready().await;

    // 3. Build client + refresh endpoints.
    let svc = cluster.make_service_registry_client();
    let transport = Arc::new(DiskdbRpcTransport::new());
    let client = Arc::new(
        DiskdbClient::new(svc.clone(), transport).with_retry_config(RetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
        }),
    );
    client.refresh_endpoints().await.expect("refresh endpoints");

    svc.register_diskdb(998, "127.0.0.1:1", &[101], &[])
        .await
        .expect("register temporary route");
    client.refresh_endpoints().await.expect("add temporary route");
    assert!(client.disk_group_ids().contains(&101));
    svc.unregister("diskdb", 998)
        .await
        .expect("remove temporary route");
    client.refresh_endpoints().await.expect("remove stale route");
    assert!(!client.disk_group_ids().contains(&101));

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

    // 5. Blind free persists the wrong-owner fact, but compaction must not
    // clear the current busy incarnation.
    let owner_b = make_chunk_id(0, 999);
    let wrong_seg = Segment {
        disk_id: seg.disk_id,
        zone_index: seg.zone_index,
        unit_offset: seg.unit_offset,
        unit_count: seg.unit_count,
        owner_chunk: Some(owner_b),
        allocation_ts: seg.allocation_ts,
    };
    let wrong_free = client
        .free_blocks(FreeBlocksRequest {
            segments: vec![wrong_seg],
        })
        .await
        .expect("blind wrong-owner free is persisted");
    assert_eq!(wrong_free.freed_count, 1);
    let disk_id = seg.disk_id.expect("allocated segment has disk id");
    let wrong_compaction = client
        .compact_zone(CompactZoneRequest {
            disk_id: Some(disk_id),
            zone_indices: vec![seg.zone_index],
        })
        .await
        .expect("compact wrong-owner fact");
    assert!(wrong_compaction.zones.iter().all(|zone| zone.success));
    let after_wrong = client
        .query_disk_group(DG_ID)
        .await
        .expect("query after mismatch");
    assert_eq!(after_wrong.disk_groups[0].busy_bytes, u64::from(UNIT_SIZE_BYTES));

    eprintln!();
    eprintln!("diskdb_client_e2e_validate_owner: ALL CHECKS PASSED");
}
