// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for the chunk data path: `Location`, `ChunkIoWriter`,
//! `WriterConfig`, EC `encode_parity_from_shards` integration.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::needless_range_loop
)]

use bytes::Bytes;
use crow_chunk_client::{BackpressurePolicy, ChunkIoWriter, FeedStatus, IoError, Location, WriterConfig};
use crow_common::ec::{decode, encode_parity_from_shards, EcScheme};
use crow_protocol::common::ChunkId;

// ── Location tests ───────────────────────────────────────────────

#[test]
fn location_proto_round_trip_single() {
    let loc = Location {
        chunk_id: ChunkId {
            high: 0x1234,
            low: 0x5678,
        },
        offset: 1024,
        length: 50 * 1024 * 1024,
        logical_offset: 0,
        logical_length: 50 * 1024 * 1024,
    };
    let proto = loc.to_proto();
    let back = Location::from_proto(&proto);
    assert_eq!(loc, back);
}

#[test]
fn location_proto_bytes_round_trip_3_entries() {
    let locs: Vec<Location> = (0..3)
        .map(|i| Location {
            chunk_id: ChunkId {
                high: 100 + i,
                low: 200 + i,
            },
            offset: 0,
            length: 8 * 1024 * 1024,
            logical_offset: i * 8 * 1024 * 1024,
            logical_length: 8 * 1024 * 1024,
        })
        .collect();

    let encoded: Vec<Vec<u8>> = locs.iter().map(Location::to_proto_bytes).collect();
    let decoded: Vec<Location> = encoded
        .iter()
        .map(|b| Location::from_proto_bytes(b).unwrap())
        .collect();
    assert_eq!(locs, decoded);
}

#[test]
fn location_binary_size_under_64() {
    let loc = Location {
        chunk_id: ChunkId {
            high: u64::MAX,
            low: u64::MAX,
        },
        offset: u64::MAX,
        length: u64::MAX,
        logical_offset: u64::MAX,
        logical_length: u64::MAX,
    };
    let bytes = loc.to_bytes();
    assert_eq!(bytes.len(), 48);
    assert!(bytes.len() < 64);
}

#[test]
fn location_binary_round_trip() {
    let loc = Location {
        chunk_id: ChunkId {
            high: 0xDEAD,
            low: 0xBEEF,
        },
        offset: 4096,
        length: 100_000,
        logical_offset: 200_000,
        logical_length: 100_000,
    };
    let bytes = loc.to_bytes();
    let back = Location::from_bytes(&bytes).unwrap();
    assert_eq!(loc, back);
}

#[test]
fn location_binary_bad_length() {
    let result = Location::from_bytes(&[0u8; 40]);
    assert!(result.is_err());
}

// ── WriterConfig defaults ────────────────────────────────────────

#[test]
fn writer_config_defaults() {
    let cfg = WriterConfig::default();
    assert_eq!(cfg.max_chunk_size, 1024 * 1024 * 1024);
    assert_eq!(cfg.prealloc_depth, 2);
    assert_eq!(cfg.parity_depth, 2);
    assert_eq!(cfg.chunk_prefetch_depth, 1);
    assert_eq!(cfg.read_buffer_size, 1024 * 1024);
    assert_eq!(cfg.max_cached_buffer, 4 * 1024 * 1024);
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

    async fn on_finish(&mut self) -> Result<Vec<Location>, IoError> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;
        Ok(Vec::new())
    }

    async fn on_error(&mut self) -> Result<Vec<Location>, IoError> {
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

// ── EC encode_parity_from_shards (gate already passed in crow-common) ──

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
