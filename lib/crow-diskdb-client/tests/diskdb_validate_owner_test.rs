// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! E2E validate-owner test: starts diskdb with
//! `validate_owner_on_free = true`, allocates a block, then verifies
//! that freeing with a wrong `owner_chunk` is rejected
//! (`PermissionDenied`) and freeing with the correct owner succeeds.
//! Also verifies that freeing a non-busy block returns `NotFound`.

use std::sync::Arc;
use std::time::Duration;

use crow_diskdb_client::{DiskdbClient, DiskdbClientError, DiskdbRpcTransport, RetryConfig};
use crow_protocol::diskdb::rpc::{AllocateBlocksRequest, FreeBlocksRequest, Segment};
use crow_test_harness::cluster::KvCluster;
use crow_test_harness::diskdb::*;
use crow_test_harness::hardware::{seed_hardware, standard_disk_ids_3, DG_ID};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_client_e2e_validate_owner() {
    if !check_binaries() {
        return;
    }

    // 1. Start kv cluster + seed hardware.
    eprintln!("=== validate-owner: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_3()).await;

    // 2. Start diskdb with validate_owner_on_free = true.
    eprintln!("=== validate-owner: starting crow-diskdb (validate_owner=true) ===");
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, true);
    diskdb.wait_for_ready().await;

    // 3. Build client + refresh endpoints.
    let svc = cluster.make_service_registry_client();
    let transport = Arc::new(DiskdbRpcTransport::new());
    let client = Arc::new(DiskdbClient::new(svc, transport).with_retry_config(RetryConfig {
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
        unit_offset: 999_999,
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
