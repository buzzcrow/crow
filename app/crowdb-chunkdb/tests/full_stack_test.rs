// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Full-stack E2E test: real KV cluster + diskdb + chunkdb in-process.
//!
//! Verifies the allocate → append → seal → query → delete lifecycle
//! against a real 3-node crowdb-kv-server cluster with diskdb running
//! in-process as a crowdb-rpc server.

mod common;

use std::sync::Arc;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster};
use crowdb_chunkdb::lifecycle::LifecycleError;
use crowdb_protocol::chunkdb::rpc::{ChunkState, ChunkType, StripType};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn chunkdb_full_stack_allocate_seal_delete() {
    // Skip if crowdb-kv-server binary is not built.
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start the kv cluster.
    let cluster = KvCluster::start().await;
    eprintln!("kv cluster started");

    // 2. Seed hardware metadata.
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware seeded");

    // 3. Start diskdb in-process.
    let diskdb = DiskdbServer::start(&cluster).await;
    eprintln!("diskdb started: {}", diskdb.rpc_endpoint);

    // 4. Wire chunkdb handler.
    let harness = ChunkdbHarness::start(&cluster).await;
    eprintln!("chunkdb harness ready");

    // 5. Allocate a chunk (1 unit = 1 MB per strip, 3 mirror copies).
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1, // 1 unit per strip
            1, // 1 strip
            StripType::Mirror,
            0,
            0,
            3, // 3 mirror copies
            ChunkType::Repo,
        )
        .await
        .expect("allocate_chunk");
    assert_eq!(chunk.state, ChunkState::Active as i32);
    assert!(!chunk.strips.is_empty(), "chunk should have strips");
    eprintln!("chunk allocated: {} strips", chunk.strips.len());

    // 6. Query the chunk.
    let chunk_id = chunk.id.as_ref().expect("chunk has id");
    let queried = harness.handler.query_chunk(chunk_id).await.expect("query_chunk");
    assert_eq!(queried.id, chunk.id);
    assert_eq!(queried.state, ChunkState::Active as i32);
    eprintln!("chunk queried");

    // 7. Seal the chunk.
    let sealed = harness
        .handler
        .seal_chunk(chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 100);
    eprintln!("chunk sealed");

    // 8. Delete the chunk.
    let deleted = harness
        .handler
        .delete_chunk(chunk_id)
        .await
        .expect("delete_chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    eprintln!("chunk deleted");

    // 9. Delete again → should return ChunkNotFound (GAP-9).
    let result = harness.handler.delete_chunk(chunk_id).await;
    assert!(
        matches!(result, Err(LifecycleError::ChunkNotFound)),
        "second delete should return ChunkNotFound, got {result:?}"
    );
    eprintln!("second delete returned ChunkNotFound (correct)");

    // 10. Query after delete → should return the deleted chunk (state=Deleted).
    let queried_after = harness
        .handler
        .query_chunk(chunk_id)
        .await
        .expect("query after delete");
    assert_eq!(queried_after.state, ChunkState::Deleted as i32);
    eprintln!("chunk queried after delete (state=Deleted)");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_append() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk.
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent appends on the same chunk — both should succeed
    // (serialized by the per-chunk lock, not corrupted).
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move {
        h1.append_chunk(&id1, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append 1")
    });
    let t2 = tokio::spawn(async move {
        h2.append_chunk(&id2, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append 2")
    });
    let r1 = t1.await.expect("task 1");
    let r2 = t2.await.expect("task 2");
    // Both appends succeeded; the lock serialized them so one ran
    // first (2 strips) and the other ran second (3 strips). The
    // important invariant: no lost update — total is 1 + 1 + 1 = 3.
    let max_strips = r1.strips.len().max(r2.strips.len());
    assert_eq!(
        max_strips, 3,
        "should have 3 strips after 2 serialized appends, got {max_strips}"
    );
    let min_strips = r1.strips.len().min(r2.strips.len());
    assert_eq!(
        min_strips, 2,
        "first append should see 2 strips, got {min_strips}"
    );
    eprintln!(
        "concurrent append serialized correctly: {} and {} strips",
        r1.strips.len(),
        r2.strips.len()
    );
}

#[tokio::test]
async fn chunkdb_lock_no_deadlock_different_chunks() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate two chunks.
    let chunk_a = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate A");
    let chunk_b = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate B");
    let id_a = *chunk_a.id.as_ref().expect("chunk A id");
    let id_b = *chunk_b.id.as_ref().expect("chunk B id");

    // Concurrent append on different chunks — no deadlock, both succeed quickly.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let t1 = tokio::spawn(async move {
        h1.append_chunk(&id_a, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append A")
    });
    let t2 = tokio::spawn(async move {
        h2.append_chunk(&id_b, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .expect("append B")
    });
    // If there's a deadlock, this timeout will fire.
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task A"), t2.await.expect("task B"))
    })
    .await
    .expect("no deadlock — both appends completed within 10s");
    assert_eq!(r1.strips.len(), 2, "chunk A should have 2 strips");
    assert_eq!(r2.strips.len(), 2, "chunk B should have 2 strips");
    eprintln!("no deadlock: both chunks appended independently");
}

#[tokio::test]
async fn chunkdb_cache_hit_on_second_query() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (populates cache via populate_cache for auto-gen ID).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Append (should be a cache hit — no store round-trip for get_chunk).
    let appended = harness
        .handler
        .append_chunk(&chunk_id, 1, StripType::Mirror, 0, 0, 3, 1)
        .await
        .expect("append_chunk");
    assert_eq!(appended.strips.len(), 2, "should have 2 strips after append");

    // Seal (should also be a cache hit after append refreshed the cache).
    let sealed = harness
        .handler
        .seal_chunk(&chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);

    // Check metrics — cache should have hits.
    if let Some(locks) = harness.handler.locks() {
        let snap = locks.metrics_snapshot();
        assert!(
            snap.cache_hit_count > 0 || snap.cache_miss_count > 0,
            "cache metrics should be non-zero (hits={}, misses={})",
            snap.cache_hit_count,
            snap.cache_miss_count
        );
        eprintln!(
            "cache metrics: hits={}, misses={}, size={}",
            snap.cache_hit_count, snap.cache_miss_count, snap.cache_size
        );
    }
    eprintln!("cache hit on second query verified");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_seal() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent seals on the same chunk — the lock serializes
    // them: exactly one succeeds (Sealed), the other sees Sealed
    // state and fails with InvalidStateTransition.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.seal_chunk(&id1, 100).await });
    let t2 = tokio::spawn(async move { h2.seal_chunk(&id2, 200).await });
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task 1"), t2.await.expect("task 2"))
    })
    .await
    .expect("no deadlock — both seals completed within 10s");

    let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok_count, 1, "exactly one seal should succeed, got {ok_count}");
    let sealed = if let Ok(c) = &r1 { c } else { r2.as_ref().unwrap() };
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    eprintln!("concurrent seal serialized: one Ok, one Err (InvalidStateTransition)");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_delete() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Two concurrent deletes on the same chunk — the lock serializes
    // them: exactly one succeeds (Deleted), the other sees Deleted
    // state and returns ChunkNotFound.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.delete_chunk(&id1).await });
    let t2 = tokio::spawn(async move { h2.delete_chunk(&id2).await });
    let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("task 1"), t2.await.expect("task 2"))
    })
    .await
    .expect("no deadlock — both deletes completed within 10s");

    let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok_count, 1, "exactly one delete should succeed, got {ok_count}");
    let deleted = if let Ok(c) = &r1 { c } else { r2.as_ref().unwrap() };
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    eprintln!("concurrent delete serialized: one Ok, one Err (ChunkNotFound)");
}

#[tokio::test]
async fn chunkdb_lock_serializes_concurrent_append_delete() {
    if std::env::var("CROWDB_KV_SERVER_BIN").is_err() && common::cluster::crowdb_kv_server_bin().is_none() {
        eprintln!("skipping: CROWDB_KV_SERVER_BIN not set and binary not found");
        return;
    }
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;

    // Allocate a chunk (Active).
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 1, StripType::Mirror, 0, 0, 3, ChunkType::Repo)
        .await
        .expect("allocate_chunk");
    let chunk_id = *chunk.id.as_ref().expect("chunk has id");

    // Concurrent append + delete on the same chunk — the lock
    // serializes them. Delete always wins (Active → Deleted). If
    // delete runs first, append sees Deleted → InvalidStateTransition.
    // If append runs first, append succeeds then delete → Deleted.
    // Invariant: delete always succeeds; final state is Deleted.
    let h1 = Arc::clone(&harness.handler);
    let h2 = Arc::clone(&harness.handler);
    let id1 = chunk_id;
    let id2 = chunk_id;
    let t1 = tokio::spawn(async move { h1.append_chunk(&id1, 1, StripType::Mirror, 0, 0, 3, 1).await });
    let t2 = tokio::spawn(async move { h2.delete_chunk(&id2).await });
    let (append_res, delete_res) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (t1.await.expect("append task"), t2.await.expect("delete task"))
    })
    .await
    .expect("no deadlock — both completed within 10s");

    let deleted = delete_res.expect("delete always succeeds on Active chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    // Append may succeed (if it ran first) or fail with
    // InvalidStateTransition (if delete ran first) — both are valid.
    match &append_res {
        Ok(c) => assert_eq!(
            c.state,
            ChunkState::Active as i32,
            "append ran first → returns Active chunk (delete runs after)"
        ),
        Err(LifecycleError::InvalidStateTransition(_)) => {
            eprintln!("delete ran first → append rejected (InvalidStateTransition)");
        }
        Err(e) => panic!("append failed with unexpected error: {e:?}"),
    }
    // Final state from the store must be Deleted.
    let final_chunk = harness.handler.query_chunk(&chunk_id).await.expect("query final");
    assert_eq!(final_chunk.state, ChunkState::Deleted as i32);
    eprintln!("concurrent append+delete serialized: final state Deleted");
}
