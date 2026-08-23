// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

//! UTs for `StripPlacement` methods.

use crow_chunk_client::StripPlacement;
use crow_protocol::common::{ChunkId, DiskId as ProtoDiskId};
use crow_protocol::diskdb::rpc::Segment;

fn make_placement(unit_kb: u32, num_segments: usize) -> StripPlacement {
    let segments: Vec<Segment> = (0..num_segments)
        .map(|i| Segment {
            disk_id: Some(ProtoDiskId {
                high: 1000 + i as u64,
                low: i as u64,
            }),
            zone_index: i as u32,
            unit_offset: i as u64 * 10,
            unit_count: 1,
            owner_chunk: Some(ChunkId { high: 1, low: 1 }),
        })
        .collect();
    StripPlacement {
        chunk_id: ChunkId { high: 1, low: 1 },
        strip_index_in_chunk: 0,
        segments,
        unit_kb,
    }
}

#[test]
fn strip_placement_unit_bytes() {
    let p = make_placement(4, 5);
    assert_eq!(p.unit_bytes(), 4096);

    let p = make_placement(1, 5);
    assert_eq!(p.unit_bytes(), 1024);
}

#[test]
fn strip_placement_segment_bounds() {
    let p = make_placement(4, 5);
    assert!(p.segment(0).is_ok());
    assert!(p.segment(4).is_ok());
    assert!(p.segment(5).is_err());
}

#[test]
fn strip_placement_disk_id() {
    let p = make_placement(4, 5);
    let did = p.disk_id(0).unwrap();
    assert_eq!(did.high, 1000);
    assert_eq!(did.low, 0);

    let did = p.disk_id(3).unwrap();
    assert_eq!(did.high, 1003);
    assert_eq!(did.low, 3);
}

#[test]
fn strip_placement_zone_offset() {
    let p = make_placement(4, 5);
    // unit_offset 0 * 4096 = 0
    assert_eq!(p.zone_offset(0).unwrap(), 0);
    // unit_offset 10 * 4096 = 40960
    assert_eq!(p.zone_offset(1).unwrap(), 40960);
    // unit_offset 20 * 4096 = 81920
    assert_eq!(p.zone_offset(2).unwrap(), 81920);
}

#[test]
fn strip_placement_zone_index() {
    let p = make_placement(4, 5);
    assert_eq!(p.zone_index(0).unwrap(), 0);
    assert_eq!(p.zone_index(1).unwrap(), 1);
    assert_eq!(p.zone_index(4).unwrap(), 4);
}
