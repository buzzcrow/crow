// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk ID generation and routing tests.

use std::collections::HashSet;

use crow_common::chunk_id::{
    generate, ChunkIdParts, CHUNK_TYPE_BTREE_PAGE, CHUNK_TYPE_PAGE_INDEX, CHUNK_TYPE_REPO, CHUNK_TYPE_WAL,
};

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
    let _bucket = id.hash_to_bucket();
    // u16 is always 0-65535 by definition; just verify it doesn't panic.
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
    let restored = ChunkIdParts::from_bytes(&bytes);
    assert_eq!(id, restored);
}
