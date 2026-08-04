// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! KV edge-case keys through group gRPC KV API.
//!
//! Covers: empty key, large key (1KB), special-bytes key (null,
//! high-UTF8, whitespace), large value (100KB), small value (1 byte),
//! empty value. All verified via `engine_get` on all replicas.

use crate::testkit::cluster::{start_cluster, TestCluster};
use bytes::Bytes;
use crow_kv::rpc::KvSetRequest;

async fn put_raw(
    client: &mut crow_kv::rpc::kv_service_client::KvServiceClient<tonic::transport::Channel>,
    key: &[u8],
    val: &[u8],
    req_id: u64,
) {
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(val),
            ttl_ms: 0,
            request_id: req_id,
            request_create_ms: 1000 + req_id,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok, "put failed for key {key:?}");
}

async fn assert_cluster_value(cluster: &TestCluster, key: &[u8], expected: &[u8]) {
    // Yield + brief sleep so the async learner-stream task drains before
    // checking follower engines. The learner-stream chosen notification
    // is async; a single yield_now is not always enough.
    tokio::task::yield_now().await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let value = replica.learner.engine_get(key).await.expect("value missing");
        assert_eq!(
            value.1.as_slice(),
            expected,
            "key {key:?} mismatch on replica {}",
            replica.id
        );
    }
}

#[tokio::test]
async fn empty_value_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put_raw(&mut client, b"k_empty_val", b"", 300).await;
    assert_cluster_value(&cluster, b"k_empty_val", b"").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn single_byte_value_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put_raw(&mut client, b"k_small", b"x", 310).await;
    assert_cluster_value(&cluster, b"k_small", b"x").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn large_key_1kb_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let key = vec![0x42u8; 1024];
    put_raw(&mut client, &key, b"big_key_val", 320).await;
    assert_cluster_value(&cluster, &key, b"big_key_val").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn large_value_100kb_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let val = vec![0xABu8; 102_400];
    put_raw(&mut client, b"k_big_val", &val, 330).await;
    assert_cluster_value(&cluster, b"k_big_val", &val).await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn special_bytes_key_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let key = b"\0\xFF\xC0 \t\nkey";
    put_raw(&mut client, key, b"special", 340).await;
    assert_cluster_value(&cluster, key, b"special").await;

    cluster.shutdown().await;
}

#[tokio::test]
async fn whitespace_key_roundtrips() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    put_raw(&mut client, b"   \t\n  ", b"ws_val", 350).await;
    assert_cluster_value(&cluster, b"   \t\n  ", b"ws_val").await;

    cluster.shutdown().await;
}
