// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb client E2E coverage for exhaustion and deterministic space reuse.

use std::collections::HashSet;

use crowdb_diskdb_client::DiskdbClientError;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{AllocateBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::{make_chunk_id, make_client, require_binaries, DiskdbProcess};
use crowdb_test_harness::hardware::{
    seed_hardware, standard_disk_ids_3, CAPACITY_UNITS, DG_ID, UNIT_SIZE_BYTES,
};

const SEGMENT_UNITS: u32 = 2;
const BLOCKS_PER_REQUEST: u32 = 3;

type UnitAddress = (DiskId, u32, u64);

#[tokio::test]
async fn exhaust_free_compact_and_reuse_exact_space() {
    require_binaries();

    let cluster = KvCluster::start().await;
    let disk_ids = standard_disk_ids_3();
    seed_hardware(&cluster.make_hardware_client(), &disk_ids).await;
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;

    let client = make_client(cluster.make_service_registry_client());
    client.refresh_endpoints().await.expect("refresh endpoints");

    let capacity_units = u64::try_from(disk_ids.len()).expect("disk count fits u64") * CAPACITY_UNITS;
    let capacity_bytes = capacity_units * u64::from(UNIT_SIZE_BYTES);
    assert_usage(&client, capacity_bytes, 0).await;

    let allocated = allocate_until_full(&client, capacity_units).await;
    let allocated_units = collect_units(&allocated);
    assert_eq!(allocated_units.len() as u64, capacity_units);
    assert_usage(&client, capacity_bytes, capacity_bytes).await;
    assert_no_space(&client).await;

    let freed: Vec<_> = allocated.iter().step_by(2).copied().collect();
    free_segments(&client, &freed).await;
    assert_usage(&client, capacity_bytes, capacity_bytes).await;

    compact_all(&client, &disk_ids).await;
    let freed_units = collect_units(&freed);
    let freed_bytes =
        u64::try_from(freed_units.len()).expect("free unit count fits u64") * u64::from(UNIT_SIZE_BYTES);
    assert_usage(&client, capacity_bytes, capacity_bytes - freed_bytes).await;

    let replacements = allocate_exact(&client, freed.len()).await;
    let replacement_units = collect_units(&replacements);
    assert_eq!(replacement_units, freed_units, "only freed units may be reused");
    assert_usage(&client, capacity_bytes, capacity_bytes).await;
    assert_no_space(&client).await;

    eprintln!(
        "space reuse verified: capacity_units={capacity_units} segments={} freed_segments={} reused_units={}",
        allocated.len(),
        freed.len(),
        replacement_units.len()
    );
}

async fn allocate_until_full(
    client: &crowdb_diskdb_client::DiskdbClient,
    capacity_units: u64,
) -> Vec<Segment> {
    let expected_segments =
        usize::try_from(capacity_units / u64::from(SEGMENT_UNITS)).expect("segment count fits usize");
    let mut segments = Vec::with_capacity(expected_segments);
    while segments.len() < expected_segments {
        let remaining = expected_segments - segments.len();
        let count =
            u32::try_from(remaining.min(BLOCKS_PER_REQUEST as usize)).expect("request count fits u32");
        let response = client
            .allocate_blocks(request(count, segments.len() as u64))
            .await
            .expect("allocate through client before exhaustion");
        assert_eq!(response.segments.len(), count as usize);
        assert!(response
            .segments
            .iter()
            .all(|segment| segment.unit_count == SEGMENT_UNITS));
        segments.extend(response.segments);
    }
    segments
}

async fn allocate_exact(client: &crowdb_diskdb_client::DiskdbClient, count: usize) -> Vec<Segment> {
    let mut segments = Vec::with_capacity(count);
    while segments.len() < count {
        let remaining = count - segments.len();
        let request_count =
            u32::try_from(remaining.min(BLOCKS_PER_REQUEST as usize)).expect("request count fits u32");
        let response = client
            .allocate_blocks(request(request_count, 10_000 + segments.len() as u64))
            .await
            .expect("reallocate freed space");
        assert_eq!(response.segments.len(), request_count as usize);
        segments.extend(response.segments);
    }
    segments
}

fn request(count: u32, sequence: u64) -> AllocateBlocksRequest {
    AllocateBlocksRequest {
        disk_group_id: DG_ID,
        unit_count: SEGMENT_UNITS,
        count,
        exclude_disk_ids: Vec::new(),
        owner_chunk: Some(make_chunk_id(1, sequence)),
    }
}

async fn free_segments(client: &crowdb_diskdb_client::DiskdbClient, segments: &[Segment]) {
    for chunk in segments.chunks(100) {
        let response = client
            .free_blocks(FreeBlocksRequest {
                segments: chunk.to_vec(),
            })
            .await
            .expect("free deterministic subset");
        assert_eq!(response.freed_count as usize, chunk.len());
    }
}

async fn compact_all(client: &crowdb_diskdb_client::DiskdbClient, disk_ids: &[DiskId]) {
    for disk_id in disk_ids {
        let response = client
            .compact_zone(CompactZoneRequest {
                disk_id: Some(*disk_id),
                zone_indices: Vec::new(),
            })
            .await
            .expect("compact all zones");
        assert!(response.zones.iter().all(|zone| zone.success));
    }
}

async fn assert_no_space(client: &crowdb_diskdb_client::DiskdbClient) {
    let result = client.allocate_blocks(request(1, u64::MAX)).await;
    assert!(matches!(result, Err(DiskdbClientError::NoSpace(_))));
}

async fn assert_usage(client: &crowdb_diskdb_client::DiskdbClient, capacity: u64, busy: u64) {
    let response = client
        .query_disk_group(DG_ID)
        .await
        .expect("query disk-group usage");
    assert_eq!(response.disk_groups.len(), 1);
    let group = &response.disk_groups[0];
    assert_eq!(group.capacity_bytes, capacity);
    assert_eq!(group.busy_bytes, busy);
    assert_eq!(group.free_bytes, capacity - busy);
    assert_eq!(
        group.disks.iter().map(|disk| disk.capacity_bytes).sum::<u64>(),
        capacity
    );
    assert_eq!(group.disks.iter().map(|disk| disk.busy_bytes).sum::<u64>(), busy);
    assert_eq!(
        group.disks.iter().map(|disk| disk.free_bytes).sum::<u64>(),
        capacity - busy
    );
    for disk in &group.disks {
        assert_eq!(disk.busy_bytes + disk.free_bytes, disk.capacity_bytes);
    }
}

fn collect_units(segments: &[Segment]) -> HashSet<UnitAddress> {
    let mut units = HashSet::new();
    for segment in segments {
        let disk_id = segment.disk_id.expect("allocated segment has disk id");
        for offset in segment.unit_offset..segment.unit_offset + u64::from(segment.unit_count) {
            assert!(
                units.insert((disk_id, segment.zone_index, offset)),
                "allocated unit intervals must not overlap"
            );
        }
    }
    units
}
