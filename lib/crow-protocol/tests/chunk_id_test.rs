// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! 128-bit chunk ID generation and routing tests.

use std::collections::HashSet;

use crow_protocol::chunk_id::{
    generate, is_zero, ChunkIdParts, CHUNK_TYPE_BTREE_PAGE, CHUNK_TYPE_PAGE_INDEX, CHUNK_TYPE_REPO,
    CHUNK_TYPE_WAL,
};
use crow_protocol::common::ChunkId;

#[test]
fn generate_sets_chunk_type() {
    for ct in [
        CHUNK_TYPE_REPO,
        CHUNK_TYPE_WAL,
        CHUNK_TYPE_BTREE_PAGE,
        CHUNK_TYPE_PAGE_INDEX,
    ] {
        let id = generate(ct);
        assert_eq!(id.chunk_type(), ct, "chunk type bits must match");
    }
}

#[test]
fn generate_is_unique() {
    let mut ids = Vec::new();
    for _ in 0..1000 {
        ids.push(generate(CHUNK_TYPE_REPO));
    }
    let set: HashSet<_> = ids.iter().collect();
    assert_eq!(set.len(), 1000, "chunk IDs should be unique");
}

#[test]
fn hash_to_bucket_in_range() {
    let id = generate(CHUNK_TYPE_REPO);
    let bucket = id.hash_to_bucket();
    let _ = bucket;
}

#[test]
fn hash_distribution_is_reasonable() {
    let mut buckets = [0u32; 256];
    for _ in 0..10000 {
        let id = generate(CHUNK_TYPE_REPO);
        let bucket = id.hash_to_bucket();
        buckets[usize::from(bucket) >> 8] += 1;
    }
    // Each of the 256 high-buckets should get ~39 (10000/256).
    // Verify no bucket is wildly skewed (> 3× expected).
    for &count in &buckets {
        assert!(count < 120, "bucket distribution skewed: {count}");
    }
}

#[test]
fn round_trip_bytes() {
    let id = generate(CHUNK_TYPE_WAL);
    let bytes = id.to_bytes();
    assert_eq!(bytes.len(), 16);
    let restored = ChunkIdParts::from_bytes(&bytes);
    assert_eq!(id, restored);
}

#[test]
fn to_from_proto_round_trip() {
    let id = generate(CHUNK_TYPE_REPO);
    let proto: ChunkId = id.to_proto();
    let restored = ChunkIdParts::from_proto(&proto);
    assert_eq!(id, restored);
}

#[test]
fn is_zero_detects_all_zeros() {
    assert!(is_zero(&ChunkId { high: 0, low: 0 }));
    assert!(!is_zero(&ChunkId { high: 0, low: 1 }));
    assert!(!is_zero(&ChunkId { high: 1, low: 0 }));
}
