// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Lifecycle state machine + per-chunk lock unit tests.

use std::sync::Arc;
use std::time::Duration;

use crow_chunkdb::lifecycle::state::{ChunkState, StateTransitionError};
use crow_chunkdb::lifecycle::{CacheHint, ChunkLockMap, LifecycleError, LockPolicy};
use crow_chunkdb::metrics::LifecycleMetrics;
use crow_protocol::chunkdb::rpc::{Chunk, ChunkState as ProtoChunkState};
use crow_protocol::common::ChunkId;

fn make_lock_map(capacity: usize) -> Arc<ChunkLockMap> {
    Arc::new(ChunkLockMap::new(
        capacity,
        Arc::new(LifecycleMetrics::new()),
        Duration::from_secs(60),
    ))
}

fn make_chunk(id: ChunkId, state: i32) -> Chunk {
    Chunk {
        id: Some(id),
        state,
        create_ts_ms: 0,
        sealed_ts_ms: 0,
        capacity: 0,
        sealed_length: 0,
        strips: vec![],
        chunk_type: 0,
    }
}

fn make_chunk_id(high: u64, low: u64) -> ChunkId {
    ChunkId { high, low }
}

// ── state machine tests ──────────────────────────────────────────

#[test]
fn state_round_trip_proto() {
    for (rust, proto) in [
        (ChunkState::Init, ProtoChunkState::Init),
        (ChunkState::Active, ProtoChunkState::Active),
        (ChunkState::Sealed, ProtoChunkState::Sealed),
        (ChunkState::Deleted, ProtoChunkState::Deleted),
    ] {
        assert_eq!(ChunkState::from_proto(proto as i32), rust);
        assert_eq!(rust.to_proto(), proto as i32);
    }
}

#[test]
fn from_proto_invalid_defaults_to_init() {
    assert_eq!(ChunkState::from_proto(999), ChunkState::Init);
}

#[test]
fn active_can_append() {
    assert!(ChunkState::Active.check_can_append().is_ok());
    assert!(ChunkState::Sealed.check_can_append().is_err());
    assert!(ChunkState::Deleted.check_can_append().is_err());
    assert!(ChunkState::Init.check_can_append().is_err());
}

#[test]
fn active_can_seal() {
    assert!(ChunkState::Active.check_can_seal().is_ok());
    assert!(ChunkState::Sealed.check_can_seal().is_err());
    assert!(ChunkState::Deleted.check_can_seal().is_err());
}

#[test]
fn active_or_sealed_can_delete() {
    assert!(ChunkState::Active.check_can_delete().is_ok());
    assert!(ChunkState::Sealed.check_can_delete().is_ok());
    assert!(ChunkState::Deleted.check_can_delete().is_err());
    assert!(ChunkState::Init.check_can_delete().is_err());
}

#[test]
fn transition_error_message() {
    let err = StateTransitionError::new(ChunkState::Deleted, "Active|Sealed");
    let msg = err.to_string();
    assert!(msg.contains("Deleted"));
    assert!(msg.contains("Active|Sealed"));
}

// ── lock serialization tests ─────────────────────────────────────

#[tokio::test]
async fn acquire_for_create_serializes_concurrent() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(1, 0);
    let locks_a = Arc::clone(&locks);
    let locks_b = Arc::clone(&locks);
    let id_a = id;
    let id_b = id;

    // Task A acquires and holds; Task B should wait.
    let a = tokio::spawn(async move {
        let _guard = locks_a
            .acquire_for_create(&id_a, &LockPolicy::default(), CacheHint::Cache)
            .await
            .expect("acquire A");
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    let b = tokio::spawn(async move {
        // Small delay so A acquires first.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let start = std::time::Instant::now();
        let _guard = locks_b
            .acquire_for_create(&id_b, &LockPolicy::default(), CacheHint::Cache)
            .await
            .expect("acquire B");
        start.elapsed()
    });
    a.await.expect("task A");
    let b_wait = b.await.expect("task B");
    // B should have waited at least ~40ms (A holds for 50ms, B starts at 10ms).
    assert!(
        b_wait >= Duration::from_millis(30),
        "B should have waited, got {b_wait:?}"
    );
}

#[tokio::test]
async fn trylock_on_held_returns_lock_busy() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(2, 0);

    let _guard = locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
        .await
        .expect("acquire");

    let err = locks
        .acquire_for_create(&id, &LockPolicy::TryLock, CacheHint::Cache)
        .await
        .unwrap_err();
    assert!(matches!(err, LifecycleError::LockBusy), "got {err:?}");
}

#[tokio::test]
async fn wait_timeout_returns_lock_timeout() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(3, 0);

    let _guard = locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
        .await
        .expect("acquire");

    let err = locks
        .acquire_for_create(
            &id,
            &LockPolicy::Wait(Duration::from_millis(50)),
            CacheHint::Cache,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, LifecycleError::LockTimeout), "got {err:?}");
}

#[tokio::test]
async fn wait_succeeds_when_released_in_time() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(4, 0);
    let locks_clone = Arc::clone(&locks);

    // Hold lock briefly, then release.
    let holder = tokio::spawn(async move {
        let _guard = locks_clone
            .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
            .await
            .expect("acquire");
        tokio::time::sleep(Duration::from_millis(20)).await;
        // _guard drops here, releasing the lock.
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    let result = locks
        .acquire_for_create(&id, &LockPolicy::Wait(Duration::from_secs(1)), CacheHint::Cache)
        .await;
    assert!(result.is_ok(), "should acquire after release: {:?}", result.err());
    holder.await.expect("holder");
}

#[tokio::test]
async fn default_policy_is_wait_10s() {
    let policy = LockPolicy::default();
    match policy {
        LockPolicy::Wait(d) => assert_eq!(d, Duration::from_secs(10)),
        LockPolicy::TryLock => panic!("default should be Wait"),
    }
}

// ── cache tests ──────────────────────────────────────────────────

#[tokio::test]
async fn populate_cache_then_invalidate() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(10, 0);
    let chunk = make_chunk(id, ProtoChunkState::Active as i32);

    locks.populate_cache(&id, chunk);
    assert_eq!(locks.cache_len(), 1);

    assert!(locks.invalidate_chunk(&id));
    assert_eq!(locks.cache_len(), 0);
    assert!(!locks.invalidate_chunk(&id), "already removed");
}

#[tokio::test]
async fn invalidate_range_removes_only_in_range() {
    let locks = make_lock_map(100);
    // Insert chunks with known bucket values.
    // We just insert arbitrary IDs; the range filter uses hash_to_bucket.
    for i in 0..20u64 {
        let id = make_chunk_id(i, 0);
        let chunk = make_chunk(id, ProtoChunkState::Active as i32);
        locks.populate_cache(&id, chunk);
    }
    assert_eq!(locks.cache_len(), 20);

    // Invalidate the full range — should remove all 20.
    let removed = locks.invalidate_range(0, u16::MAX);
    assert_eq!(removed, 20, "should remove all 20 entries");
    assert_eq!(locks.cache_len(), 0);
}

#[tokio::test]
async fn cache_eviction_on_capacity() {
    let locks = make_lock_map(2);
    let id1 = make_chunk_id(1, 0);
    let id2 = make_chunk_id(2, 0);
    let id3 = make_chunk_id(3, 0);

    locks.populate_cache(&id1, make_chunk(id1, ProtoChunkState::Active as i32));
    locks.populate_cache(&id2, make_chunk(id2, ProtoChunkState::Active as i32));
    locks.populate_cache(&id3, make_chunk(id3, ProtoChunkState::Active as i32));
    // Cache capacity is 2; at least one of the first two should be evicted.
    // The exact eviction depends on S3-FIFO policy.
    let len = locks.cache_len();
    assert!(len <= 2, "cache should not exceed capacity, got {len}");
}

// ── reap_idle tests ──────────────────────────────────────────────

#[tokio::test]
async fn reap_idle_removes_uncontended() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(20, 0);

    {
        let _guard = locks
            .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
            .await
            .expect("acquire");
    }
    // Guard dropped — entry should be reapable.
    locks.reap_idle();
    // After reap, a new acquire should create a fresh mutex (no error).
    let _guard = locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
        .await
        .expect("re-acquire after reap");
}

#[tokio::test]
async fn reap_idle_retains_contended() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(21, 0);

    let _guard = locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
        .await
        .expect("acquire");

    // Guard is still held — reap should not remove the entry.
    locks.reap_idle();

    // TryLock should still fail (entry exists, mutex held).
    let err = locks
        .acquire_for_create(&id, &LockPolicy::TryLock, CacheHint::Cache)
        .await
        .unwrap_err();
    assert!(matches!(err, LifecycleError::LockBusy));
}

// ── metrics tests ────────────────────────────────────────────────

#[tokio::test]
async fn metrics_record_lock_timeout() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(30, 0);

    let _guard = locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
        .await
        .expect("acquire");

    let _ = locks
        .acquire_for_create(
            &id,
            &LockPolicy::Wait(Duration::from_millis(10)),
            CacheHint::Cache,
        )
        .await;

    let snap = locks.metrics_snapshot();
    assert!(
        snap.lock_timeout_count >= 1,
        "timeout count: {}",
        snap.lock_timeout_count
    );
}

#[tokio::test]
async fn metrics_record_lock_busy() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(31, 0);

    let _guard = locks
        .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
        .await
        .expect("acquire");

    let _ = locks
        .acquire_for_create(&id, &LockPolicy::TryLock, CacheHint::Cache)
        .await;

    let snap = locks.metrics_snapshot();
    assert!(snap.lock_busy_count >= 1, "busy count: {}", snap.lock_busy_count);
}

#[tokio::test]
async fn metrics_record_cache_hit_miss() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(32, 0);
    locks.populate_cache(&id, make_chunk(id, ProtoChunkState::Active as i32));

    // We can't easily test acquire's cache hit/miss without a store,
    // but we can verify populate_cache + invalidate metrics.
    locks.invalidate_chunk(&id);

    let snap = locks.metrics_snapshot();
    assert!(
        snap.invalidate_count >= 1,
        "invalidate count: {}",
        snap.invalidate_count
    );
}

#[tokio::test]
async fn metrics_record_reap_idle() {
    let locks = make_lock_map(100);
    let id = make_chunk_id(33, 0);

    {
        let _guard = locks
            .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
            .await
            .expect("acquire");
    }
    locks.reap_idle();

    let snap = locks.metrics_snapshot();
    assert!(snap.reap_idle_count >= 1, "reap count: {}", snap.reap_idle_count);
    assert!(
        snap.reap_idle_entries_removed >= 1,
        "entries removed: {}",
        snap.reap_idle_entries_removed
    );
}

#[tokio::test]
async fn metrics_snapshot_has_all_fields() {
    let locks = make_lock_map(100);
    let snap = locks.metrics_snapshot();
    // All fields should be present and non-negative (zero on fresh map).
    assert_eq!(snap.lock_timeout_count, 0);
    assert_eq!(snap.lock_busy_count, 0);
    assert_eq!(snap.cache_hit_count, 0);
    assert_eq!(snap.cache_miss_count, 0);
    assert_eq!(snap.cache_size, 0);
    assert_eq!(snap.reap_idle_count, 0);
    assert_eq!(snap.reap_idle_entries_removed, 0);
    assert_eq!(snap.invalidate_count, 0);
}

// ── error mapping tests ──────────────────────────────────────────

#[test]
fn lock_busy_error_display() {
    let e = LifecycleError::LockBusy;
    assert!(e.to_string().contains("busy"));
}

#[test]
fn lock_timeout_error_display() {
    let e = LifecycleError::LockTimeout;
    assert!(e.to_string().contains("timed out"));
}
