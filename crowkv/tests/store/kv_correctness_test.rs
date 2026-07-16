// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Store-layer KV operation correctness through `PxKvStore` public API.
//!
//! Covers all op types and orderings: Put, overwrite, Delete,
//! delete non-existent, batch with multiple puts, intra-batch
//! last-wins, put-then-delete, delete-then-put, empty batch,
//! mixed ops across slots. Also covers edge-case keys.

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowkv::rpc::KvBatchItem;
use std::net::SocketAddr;

fn leader_store() -> PxKvStore {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    let local = PxLocalReplica::new(10, PxLocalReplicaRole::Leader);
    let group = PxGroup::new(1, local);
    store.add_group(group);
    store
}

async fn assert_key(store: &PxKvStore, key: &[u8], expected: Option<&[u8]>) {
    let g = store.get_group(1).unwrap();
    let val = g.local_replica().learner.engine_get(key).await;
    match expected {
        Some(bytes) => {
            assert_eq!(val.expect("value missing").1.as_slice(), bytes);
        }
        None => {
            assert!(val.is_none(), "key {key:?} should be absent");
        }
    }
}

#[tokio::test]
async fn put_overwrite_keeps_latest() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k", b"v1", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_put(1, b"k", b"v2", 2, 1, 101, 1001).await.ok);
    assert_key(&store, b"k", Some(b"v2")).await;
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let store = leader_store();
    let r = store.kv_delete(1, b"missing", 1, 1, 100, 1000).await;
    assert!(r.ok);
    assert_key(&store, b"missing", None).await;
}

#[tokio::test]
async fn batch_multiple_puts_all_visible() {
    let store = leader_store();
    let items = vec![
        KvBatchItem {
            key: b"a".to_vec(),
            value: b"1".to_vec(),
            is_delete: false,
        },
        KvBatchItem {
            key: b"b".to_vec(),
            value: b"2".to_vec(),
            is_delete: false,
        },
        KvBatchItem {
            key: b"c".to_vec(),
            value: b"3".to_vec(),
            is_delete: false,
        },
    ];
    assert!(store.kv_batch_write(1, items, 1, 1, 100, 1000).await.ok);
    assert_key(&store, b"a", Some(b"1")).await;
    assert_key(&store, b"b", Some(b"2")).await;
    assert_key(&store, b"c", Some(b"3")).await;
}

#[tokio::test]
async fn intra_batch_last_occurrence_wins() {
    let store = leader_store();
    let items = vec![
        KvBatchItem {
            key: b"k".to_vec(),
            value: b"a".to_vec(),
            is_delete: false,
        },
        KvBatchItem {
            key: b"k".to_vec(),
            value: vec![],
            is_delete: true,
        },
        KvBatchItem {
            key: b"k".to_vec(),
            value: b"final".to_vec(),
            is_delete: false,
        },
    ];
    assert!(store.kv_batch_write(1, items, 1, 1, 100, 1000).await.ok);
    assert_key(&store, b"k", Some(b"final")).await;
}

#[tokio::test]
async fn put_then_delete_key_absent() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k", b"v", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_delete(1, b"k", 2, 1, 101, 1001).await.ok);
    assert_key(&store, b"k", None).await;
}

#[tokio::test]
async fn delete_then_put_key_has_new_value() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k", b"initial", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_delete(1, b"k", 2, 1, 101, 1001).await.ok);
    assert!(store.kv_put(1, b"k", b"reborn", 3, 1, 102, 1002).await.ok);
    assert_key(&store, b"k", Some(b"reborn")).await;
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k", b"v", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_batch_write(1, vec![], 2, 1, 101, 1001).await.ok);
    assert_key(&store, b"k", Some(b"v")).await;
}

#[tokio::test]
async fn mixed_ops_across_slots() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k1", b"v1", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_put(1, b"k2", b"v2", 2, 1, 101, 1001).await.ok);
    assert!(store.kv_delete(1, b"k1", 3, 1, 102, 1002).await.ok);
    assert!(store.kv_put(1, b"k3", b"v3", 4, 1, 103, 1003).await.ok);
    assert!(store.kv_put(1, b"k2", b"v2b", 5, 1, 104, 1004).await.ok);
    assert_key(&store, b"k1", None).await;
    assert_key(&store, b"k2", Some(b"v2b")).await;
    assert_key(&store, b"k3", Some(b"v3")).await;
}

// ---------- edge-case keys ----------

#[tokio::test]
async fn empty_value_roundtrips() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k", b"", 1, 1, 100, 1000).await.ok);
    assert_key(&store, b"k", Some(b"")).await;
}

#[tokio::test]
async fn single_byte_value_roundtrips() {
    let store = leader_store();
    assert!(store.kv_put(1, b"k", b"x", 1, 1, 100, 1000).await.ok);
    assert_key(&store, b"k", Some(b"x")).await;
}

#[tokio::test]
async fn large_key_1kb_roundtrips() {
    let store = leader_store();
    let key = vec![0x42u8; 1024];
    assert!(store.kv_put(1, &key, b"big_key", 1, 1, 100, 1000).await.ok);
    assert_key(&store, &key, Some(b"big_key")).await;
}

#[tokio::test]
async fn large_value_100kb_roundtrips() {
    let store = leader_store();
    let val = vec![0xABu8; 102_400];
    assert!(store.kv_put(1, b"k", &val, 1, 1, 100, 1000).await.ok);
    assert_key(&store, b"k", Some(&val)).await;
}

#[tokio::test]
async fn special_bytes_key_roundtrips() {
    let store = leader_store();
    let key = b"\0\xFF\xC0 \t\nkey";
    assert!(store.kv_put(1, key, b"special", 1, 1, 100, 1000).await.ok);
    assert_key(&store, key, Some(b"special")).await;
}
