// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Routing unit tests — hash bucket, binding cache, route.

#![allow(clippy::cast_possible_truncation)]

use crow_chunkdb::routing::{self, BindingCache, BindingTable, BucketBinding, MigrationState, RouteError};
use crow_protocol::common::ChunkId;

fn make_chunk_id(n: u64) -> ChunkId {
    ChunkId {
        high: n,
        mid: n.wrapping_mul(31),
        low: n.wrapping_mul(37),
    }
}

fn binding(start: u16, end: u16, store: u64, group: u64) -> BucketBinding {
    BucketBinding {
        start,
        end,
        kv_store_id: store,
        kv_group_id: group,
        old_kv_store_id: None,
        old_kv_group_id: None,
        migration_state: MigrationState::NotMigrating,
    }
}

#[test]
fn hash_to_bucket_is_in_range() {
    for i in 0..1000_u64 {
        let id = make_chunk_id(i);
        let bucket = routing::hash_to_bucket(&id);
        let _ = bucket;
    }
}

#[test]
fn hash_to_bucket_is_uniform() {
    // 10000 chunk IDs → no single bucket has > 1% (100) of IDs.
    let mut counts = std::collections::HashMap::<u16, u32>::new();
    for i in 0..10000_u64 {
        let id = make_chunk_id(i);
        let bucket = routing::hash_to_bucket(&id);
        *counts.entry(bucket).or_insert(0) += 1;
    }

    let max = counts.values().copied().max().unwrap_or(0);
    assert!(
        max <= 100,
        "uniform distribution violated: max bucket count = {max} (expected <= 100)"
    );
}

#[test]
fn route_returns_binding_for_bucket() {
    let cache = BindingCache::new();
    cache.replace(BindingTable::new(vec![binding(0, 16384, 1, 10)]));

    // Find a chunk ID that hashes to bucket < 16384.
    for i in 0..10000_u64 {
        let id = make_chunk_id(i);
        let bucket = routing::hash_to_bucket(&id);
        if bucket < 16384 {
            let r = routing::route(&cache, &id).unwrap();
            assert_eq!(r.kv_store_id, 1);
            assert_eq!(r.kv_group_id, 10);
            assert_eq!(r.migration_state, MigrationState::NotMigrating);
            return;
        }
    }
    panic!("no chunk ID hashed to bucket < 16384 in 10000 tries");
}

#[test]
fn route_empty_cache_returns_error() {
    let cache = BindingCache::new();
    let id = make_chunk_id(1);
    let result = routing::route(&cache, &id);
    assert!(matches!(result, Err(RouteError::NoBinding)));
}

#[test]
fn route_unbound_bucket_returns_error() {
    let cache = BindingCache::new();
    cache.replace(BindingTable::new(vec![binding(0, 100, 1, 10)]));

    // Find a chunk ID that hashes to bucket >= 100.
    for i in 0..10000_u64 {
        let id = make_chunk_id(i);
        let bucket = routing::hash_to_bucket(&id);
        if bucket >= 100 {
            let result = routing::route(&cache, &id);
            assert!(matches!(result, Err(RouteError::BucketUnbound { .. })));
            return;
        }
    }
    panic!("no chunk ID hashed to bucket >= 100 in 10000 tries");
}

#[test]
fn route_with_migration_returns_old_group() {
    let cache = BindingCache::new();
    cache.replace(BindingTable::new(vec![BucketBinding {
        start: 0,
        end: 16384,
        kv_store_id: 2,
        kv_group_id: 20,
        old_kv_store_id: Some(1),
        old_kv_group_id: Some(10),
        migration_state: MigrationState::Copying,
    }]));

    for i in 0..10000_u64 {
        let id = make_chunk_id(i);
        let bucket = routing::hash_to_bucket(&id);
        if bucket < 16384 {
            let r = routing::route(&cache, &id).unwrap();
            assert_eq!(r.kv_store_id, 2);
            assert_eq!(r.kv_group_id, 20);
            assert_eq!(r.migration_state, MigrationState::Copying);
            assert_eq!(r.old_kv_store_id, Some(1));
            assert_eq!(r.old_kv_group_id, Some(10));
            return;
        }
    }
    panic!("no chunk ID hashed to bucket < 16384 in 10000 tries");
}

#[test]
fn default_binding_table_covers_all_buckets() {
    let table = routing::default_binding_table(1, 1);
    assert_eq!(table.len(), 1);
    let b = &table.bindings()[0];
    assert_eq!(b.start, 0);
    assert_eq!(b.end, 65535);
    assert_eq!(b.kv_store_id, 1);
    assert_eq!(b.kv_group_id, 1);
}

#[test]
fn binding_cache_route_bucket_directly() {
    let cache = BindingCache::new();
    cache.replace(BindingTable::new(vec![
        binding(0, 100, 1, 10),
        binding(100, 65535, 2, 20),
    ]));

    let r1 = cache.route_bucket(50).unwrap();
    assert_eq!(r1.kv_group_id, 10);

    let r2 = cache.route_bucket(200).unwrap();
    assert_eq!(r2.kv_group_id, 20);

    assert!(cache.route_bucket(65535).is_none());
}
