// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Accessor tests for `EcStripWriter` with `Arc<Chunk>` + strip index.

#![allow(clippy::similar_names, clippy::cast_possible_truncation)]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, EcStripWriter, Result};
use crowdb_common::ec::EcScheme;
use crowdb_diskio_client::DiskId;
use crowdb_protocol::chunkdb::rpc::Strip as StripOneof;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, ChunkType, EcStrip, StripType};
use crowdb_protocol::common::{ChunkId, DiskId as ProtoDiskId};
use crowdb_protocol::diskdb::rpc::Segment;

/// No-op `DiskWriter` for accessor tests (no IO performed).
struct NoopDiskWriter;
#[async_trait]
impl DiskWriter for NoopDiskWriter {
    async fn write(&self, _seg: &Segment, _unit_bytes: u64, _data: Bytes) -> Result<()> {
        Ok(())
    }
    async fn fsync(&self, _id: DiskId) -> Result<()> {
        Ok(())
    }
}

/// Build a `Chunk` with one EC strip of `num_segments` segments.
fn make_chunk(unit_kb: u32, num_segments: usize) -> Arc<Chunk> {
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
    let strip = ChunkStrip {
        chunk_offset: 0,
        strip_sequence: 0,
        unit_kb,
        capacity: num_segments as u32,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        sealed_length: 0,
        strip_type: StripType::Ec as i32,
        strip: Some(StripOneof::EcStrip(EcStrip {
            data_num: 4,
            code_num: 1,
            ec_state: 0,
            segments,
        })),
        usage_bitmap: Vec::new(),
    };
    Arc::new(Chunk {
        id: Some(ChunkId { high: 1, low: 1 }),
        state: 1,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        capacity: num_segments as u32,
        sealed_length: 0,
        strips: vec![strip],
        chunk_type: ChunkType::Repo as i32,
    })
}

fn make_writer(unit_kb: u32, num_segments: usize) -> EcStripWriter {
    EcStripWriter::new(
        make_chunk(unit_kb, num_segments),
        0,
        Arc::new(NoopDiskWriter),
        EcScheme::new(4, 1),
    )
}

#[test]
fn accessor_unit_bytes() {
    let w = make_writer(4, 5);
    assert_eq!(w.unit_bytes_for_tests(), 4096);
    let w = make_writer(1, 5);
    assert_eq!(w.unit_bytes_for_tests(), 1024);
}

#[test]
fn accessor_segment_bounds() {
    let w = make_writer(4, 5);
    assert!(w.segment_for_tests(0).is_ok());
    assert!(w.segment_for_tests(4).is_ok());
    assert!(w.segment_for_tests(5).is_err());
}

#[test]
fn accessor_disk_id() {
    let w = make_writer(4, 5);
    let did = w.disk_id_for_tests(0).unwrap();
    assert_eq!(did.high, 1000);
    assert_eq!(did.low, 0);
    let did = w.disk_id_for_tests(3).unwrap();
    assert_eq!(did.high, 1003);
    assert_eq!(did.low, 3);
}

#[test]
fn accessor_zone_offset() {
    let w = make_writer(4, 5);
    assert_eq!(w.zone_offset_for_tests(0).unwrap(), 0);
    assert_eq!(w.zone_offset_for_tests(1).unwrap(), 40960);
    assert_eq!(w.zone_offset_for_tests(2).unwrap(), 81920);
}
