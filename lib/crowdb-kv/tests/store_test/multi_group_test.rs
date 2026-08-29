// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Multi-group KV store routing and missing-group error tests.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};

fn leader_group(group_id: u64, node_id: u64) -> PxGroup {
    let local = PxLocalReplica::new(node_id, PxLocalReplicaRole::Leader);
    PxGroup::new(group_id, local)
}

#[tokio::test]
async fn multi_group_routes_to_correct_group() {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(leader_group(1, 10));
    store.add_group(leader_group(2, 20));

    // Write to group 1
    let r1 = store.kv_put(1, b"g1-key", b"g1-val", 1, 1, 100, 1000).await;
    assert!(r1.ok, "group 1 put should succeed");

    // Write to group 2
    let r2 = store.kv_put(2, b"g2-key", b"g2-val", 2, 1, 200, 2000).await;
    assert!(r2.ok, "group 2 put should succeed");

    // Verify isolation: group 1 has only its key
    let g1 = store.get_group(1).unwrap();
    assert!(g1
        .local_replica()
        .learner
        .engine_get(b"g1-key".as_slice())
        .await
        .is_some());
    assert!(g1
        .local_replica()
        .learner
        .engine_get(b"g2-key".as_slice())
        .await
        .is_none());

    // Verify isolation: group 2 has only its key
    let g2 = store.get_group(2).unwrap();
    assert!(g2
        .local_replica()
        .learner
        .engine_get(b"g2-key".as_slice())
        .await
        .is_some());
    assert!(g2
        .local_replica()
        .learner
        .engine_get(b"g1-key".as_slice())
        .await
        .is_none());
}

#[tokio::test]
async fn missing_group_returns_error() {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(leader_group(1, 10));

    // Put to non-existent group
    let resp = store.kv_put(99, b"k", b"v", 1, 1, 100, 1000).await;
    assert!(!resp.ok);
    assert!(
        resp.error.contains("no kv group"),
        "error should mention missing group, got: {}",
        resp.error
    );

    // Delete to non-existent group
    let resp = store.kv_delete(99, b"k", 1, 2, 101, 1001).await;
    assert!(!resp.ok);
    assert!(resp.error.contains("no kv group"));

    // Batch write to non-existent group
    let resp = store.kv_batch_write(99, vec![], 1, 3, 102, 1002).await;
    assert!(!resp.ok);
    assert!(resp.error.contains("no kv group"));

    // Get from non-existent group
    let resp = store.kv_get(99, b"k", 0, 0, 103, 1003).await;
    assert!(!resp.ok);
    assert!(resp.error.contains("no kv group"));
}

#[tokio::test]
async fn add_and_remove_group_dynamic() {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(leader_group(1, 10));

    let r = store.kv_put(1, b"k", b"v", 1, 1, 100, 1000).await;
    assert!(r.ok);

    // Remove the group
    assert!(store.remove_group(1));
    assert!(!store.remove_group(1), "second remove returns false");

    // Requests to removed group fail
    let r = store.kv_put(1, b"k", b"v", 1, 2, 101, 1001).await;
    assert!(!r.ok);
    assert!(r.error.contains("no kv group"));
}
