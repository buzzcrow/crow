// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for the chunk data path: `ProtoLocation`, `ChunkIoWriter`,
//! `WriterConfig`, EC `encode_parity_from_shards` integration.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::needless_range_loop
)]

use bytes::Bytes;
use crowdb_chunk_client::{
    BackpressurePolicy, ChunkClientConfig, ChunkIoWriter, FeedStatus, IoError, ProtoLocation,
};
use crowdb_common::ec::{decode, encode_parity_from_shards, EcScheme};
use crowdb_protocol::common::ChunkId;

// ── ProtoLocation tests ──────────────────────────────────────────

#[test]
fn location_proto_round_trip_single() {
    let loc = ProtoLocation {
        chunk_id: Some(ChunkId {
            high: 0x1234,
            low: 0x5678,
        }),
        offset: 1024,
        length: 50 * 1024 * 1024,
        logical_offset: 0,
        logical_length: 50 * 1024 * 1024,
    };
    let bytes = bincode::serialize(&loc).unwrap();
    let back: ProtoLocation = bincode::deserialize(&bytes).unwrap();
    assert_eq!(loc, back);
}

#[test]
fn location_proto_bytes_round_trip_3_entries() {
    let locs: Vec<ProtoLocation> = (0..3)
        .map(|i| ProtoLocation {
            chunk_id: Some(ChunkId {
                high: 100 + i,
                low: 200 + i,
            }),
            offset: 0,
            length: 8 * 1024 * 1024,
            logical_offset: i * 8 * 1024 * 1024,
            logical_length: 8 * 1024 * 1024,
        })
        .collect();

    let encoded: Vec<Vec<u8>> = locs.iter().map(|l| bincode::serialize(l).unwrap()).collect();
    let decoded: Vec<ProtoLocation> = encoded.iter().map(|b| bincode::deserialize(b).unwrap()).collect();
    assert_eq!(locs, decoded);
}

// ── ChunkClientConfig defaults ───────────────────────────────────

#[test]
fn chunk_client_config_defaults() {
    let cfg = ChunkClientConfig::default();
    assert_eq!(cfg.max_chunk_size, 1024 * 1024 * 1024);
    assert_eq!(cfg.prealloc_depth, 2);
    assert_eq!(cfg.parity_depth, 2);
    assert_eq!(cfg.chunk_prefetch_depth, 1);
    assert_eq!(cfg.read_buffer_size, 1024 * 1024);
    assert_eq!(cfg.max_cached_buffer, 4 * 1024 * 1024);
    assert_eq!(cfg.prefetch_chunk_count, 1);
}

// ── ChunkIoWriter mock contract ──────────────────────────────────

struct MockWriter {
    finished: bool,
    data_count: usize,
}

impl MockWriter {
    fn new() -> Self {
        Self {
            finished: false,
            data_count: 0,
        }
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for MockWriter {
    async fn on_data(&mut self, _buffer: Bytes) -> Result<FeedStatus, IoError> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.data_count += 1;
        Ok(FeedStatus::Continue)
    }

    async fn on_finish(&mut self) -> Result<Vec<ProtoLocation>, IoError> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;
        Ok(Vec::new())
    }

    async fn on_error(&mut self) -> Result<Vec<ProtoLocation>, IoError> {
        Ok(Vec::new())
    }

    fn require_data(&self) -> bool {
        !self.finished
    }
}

#[tokio::test]
async fn chunk_io_writer_mock_basic() {
    let mut w = MockWriter::new();
    assert!(w.require_data());
    let status = w.on_data(Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(status, FeedStatus::Continue);
    let locs = w.on_finish().await.unwrap();
    assert!(locs.is_empty());
    assert!(!w.require_data());
}

#[tokio::test]
async fn chunk_io_writer_on_data_after_finish() {
    let mut w = MockWriter::new();
    w.on_finish().await.unwrap();
    let result = w.on_data(Bytes::from_static(b"late")).await;
    assert!(matches!(result, Err(IoError::Finished)));
}

#[tokio::test]
async fn chunk_io_writer_on_finish_twice() {
    let mut w = MockWriter::new();
    w.on_finish().await.unwrap();
    let result = w.on_finish().await;
    assert!(matches!(result, Err(IoError::Finished)));
}

#[tokio::test]
async fn chunk_io_writer_on_error_no_sealed() {
    let mut w = MockWriter::new();
    let locs = w.on_error().await.unwrap();
    assert!(locs.is_empty());
}

// ── EC encode_parity_from_shards (gate already passed in crowdb-common) ──

#[test]
fn ec_parity_from_shards_full_strip_4_1() {
    let scheme = EcScheme::new(4, 1);
    let shard_size = 4096;
    let data: Vec<Vec<u8>> = (0..4)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * shard_size + j) % 251) as u8)
                .collect()
        })
        .collect();
    let shards: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();

    let parity = encode_parity_from_shards(scheme, &shards).unwrap();
    assert_eq!(parity.len(), 1);
    assert_eq!(parity[0].len(), shard_size);

    // Decode with all 5 present.
    let mut blocks: Vec<Option<Vec<u8>>> = data.into_iter().map(Some).collect();
    blocks.extend(parity.into_iter().map(Some));
    let recovered = decode(scheme, blocks).unwrap();
    for i in 0..4 {
        let expected: Vec<u8> = (0..shard_size)
            .map(|j| ((i * shard_size + j) % 251) as u8)
            .collect();
        assert_eq!(recovered[i], expected);
    }
}

// ── BackpressurePolicy is constructible ──────────────────────────

#[test]
fn backpressure_policy_variants() {
    let _ = (BackpressurePolicy::Blocking, BackpressurePolicy::NonBlocking);
}
