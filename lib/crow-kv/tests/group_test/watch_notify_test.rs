// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version  2.0.

//! End-to-end tests for the watch/notify flow: subscribe → write →
//! notify → client-receive. Uses the real gRPC bidi stream against
//! a live single-node cluster (no mocks).

use std::time::Duration;

use bytes::Bytes;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::watch_notify_request;
use crow_kv::rpc::watch_notify_response;
use crow_kv::rpc::{KvSetRequest, WatchNotifyRequest, WatchNotifyResponse, WatchSubscribe};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tonic::transport::Channel;
use tonic::Streaming;

use crate::common::cluster::start_cluster;

/// Open a `watch_notify` bidi stream to `node`, send a
/// `WatchSubscribe` for `prefix` on `group_id`, and return the
/// response stream + the inbound sender (for later unsubscribe).
async fn open_watch(
    client: &mut KvServiceClient<Channel>,
    group_id: u64,
    prefix: &[u8],
) -> (mpsc::Sender<WatchNotifyRequest>, Streaming<WatchNotifyResponse>) {
    let (tx, rx) = mpsc::channel::<WatchNotifyRequest>(16);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let response = client
        .watch_notify(stream)
        .await
        .expect("watch_notify stream open");
    let resp_stream = response.into_inner();
    tx.send(WatchNotifyRequest {
        frame: Some(watch_notify_request::Frame::Subscribe(WatchSubscribe {
            version: 1,
            group_id,
            prefix: prefix.to_vec(),
        })),
    })
    .await
    .expect("send subscribe");
    (tx, resp_stream)
}

/// Subscribe to a prefix, write a matching key, assert the notify
/// arrives with the correct key and value.
#[tokio::test]
async fn watch_notify_put_receives_key_and_value() {
    let cluster = start_cluster(&[0], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let (_inbound_tx, mut resp_stream) = open_watch(&mut client, 1, b"test/").await;

    // Give the server time to process the subscribe frame before
    // writing, so the watcher is registered in the registry.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Write a key matching the watched prefix.
    client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"test/k1"),
            value: Bytes::from_static(b"v1"),
            ttl_ms: 0,
            request_id: 201,
            request_create_ms: 1200,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();

    // Wait for the notify frame.
    let resp = tokio::time::timeout(Duration::from_secs(5), resp_stream.next())
        .await
        .expect("notify timed out")
        .expect("stream closed")
        .expect("stream error");

    let notify = match resp.frame {
        Some(watch_notify_response::Frame::Notify(n)) => n,
        other => panic!("expected Notify, got {other:?}"),
    };

    assert_eq!(notify.prefix, b"test/");
    assert!(
        notify.keys.contains(&b"test/k1".to_vec()),
        "keys: {:?}",
        notify.keys
    );
    let idx = notify
        .keys
        .iter()
        .position(|k| k == b"test/k1")
        .expect("key in notify");
    assert_eq!(
        notify.values.get(idx).map(std::vec::Vec::as_slice),
        Some(b"v1".as_slice()),
        "value mismatch"
    );

    cluster.shutdown().await;
}

/// Subscribe to a prefix, delete a matching key, assert the notify
/// arrives with the correct key and an empty value (tombstone).
#[tokio::test]
async fn watch_notify_delete_receives_key_with_empty_value() {
    use crow_kv::rpc::KvDeleteRequest;

    let cluster = start_cluster(&[0], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    // Seed the key first.
    client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"test/del"),
            value: Bytes::from_static(b"seed"),
            ttl_ms: 0,
            request_id: 301,
            request_create_ms: 1300,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put seed");

    let (_inbound_tx, mut resp_stream) = open_watch(&mut client, 1, b"test/").await;

    // Give the server time to process the subscribe frame.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Delete the key.
    client
        .delete(KvDeleteRequest {
            version: 1,
            key: Bytes::from_static(b"test/del"),
            request_id: 302,
            request_create_ms: 1301,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv delete");

    let resp = tokio::time::timeout(Duration::from_secs(5), resp_stream.next())
        .await
        .expect("notify timed out")
        .expect("stream closed")
        .expect("stream error");

    let notify = match resp.frame {
        Some(watch_notify_response::Frame::Notify(n)) => n,
        other => panic!("expected Notify, got {other:?}"),
    };

    assert_eq!(notify.prefix, b"test/");
    let idx = notify
        .keys
        .iter()
        .position(|k| k == b"test/del")
        .expect("deleted key in notify");
    assert_eq!(
        notify.values.get(idx).map(std::vec::Vec::as_slice),
        Some(&[] as &[u8]),
        "delete value should be empty"
    );

    cluster.shutdown().await;
}

/// A write to a non-matching prefix must NOT produce a notify.
#[tokio::test]
async fn watch_notify_non_matching_key_no_notify() {
    let cluster = start_cluster(&[0], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let (_inbound_tx, mut resp_stream) = open_watch(&mut client, 1, b"watched/").await;

    // Give the server time to process the subscribe frame.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Write a key that does NOT match the watched prefix.
    client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"other/k1"),
            value: Bytes::from_static(b"v1"),
            ttl_ms: 0,
            request_id: 401,
            request_create_ms: 1400,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put");

    // No notify should arrive within a short window.
    let result = tokio::time::timeout(Duration::from_millis(500), resp_stream.next()).await;
    assert!(result.is_err(), "received unexpected notify for non-matching key");

    cluster.shutdown().await;
}

/// Batch write touching multiple keys under the watched prefix — all
/// keys should appear in the notify.
#[tokio::test]
async fn watch_notify_batch_write_multiple_keys() {
    use crow_kv::rpc::{KvBatchItem, KvBatchWriteRequest};

    let cluster = start_cluster(&[0], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    let (_inbound_tx, mut resp_stream) = open_watch(&mut client, 1, b"batch/").await;

    // Give the server time to process the subscribe frame.
    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .batch_write(KvBatchWriteRequest {
            version: 1,
            items: vec![
                KvBatchItem {
                    key: Bytes::from_static(b"batch/k1"),
                    value: Bytes::from_static(b"v1"),
                    is_delete: false,
                },
                KvBatchItem {
                    key: Bytes::from_static(b"batch/k2"),
                    value: Bytes::from_static(b"v2"),
                    is_delete: false,
                },
            ],
            request_id: 501,
            request_create_ms: 1500,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv batch");

    let resp = tokio::time::timeout(Duration::from_secs(5), resp_stream.next())
        .await
        .expect("notify timed out")
        .expect("stream closed")
        .expect("stream error");

    let notify = match resp.frame {
        Some(watch_notify_response::Frame::Notify(n)) => n,
        other => panic!("expected Notify, got {other:?}"),
    };

    assert_eq!(notify.prefix, b"batch/");
    assert!(
        notify.keys.contains(&b"batch/k1".to_vec()),
        "keys: {:?}",
        notify.keys
    );
    assert!(
        notify.keys.contains(&b"batch/k2".to_vec()),
        "keys: {:?}",
        notify.keys
    );

    // Verify values match their keys.
    for (key, expected) in [(b"batch/k1", b"v1"), (b"batch/k2", b"v2")] {
        let idx = notify.keys.iter().position(|k| k == key).expect("key in notify");
        assert_eq!(
            notify.values.get(idx).map(std::vec::Vec::as_slice),
            Some(expected.as_slice()),
            "value mismatch for {key:?}"
        );
    }

    cluster.shutdown().await;
}

/// Subscribe from a follower — the follower should redirect with
/// `not_leader_hint`.
#[tokio::test]
async fn watch_notify_follower_redirects_to_leader() {
    let cluster = start_cluster(&[0, 1], 0).await;
    let follower = cluster
        .followers()
        .into_iter()
        .next()
        .expect("at least one follower");
    let mut client = cluster.kv_client(follower).await;

    let (_inbound_tx, mut resp_stream) = open_watch(&mut client, 1, b"test/").await;

    // The follower is not the leader, so it should send an error with
    // not_leader_hint.
    let resp = tokio::time::timeout(Duration::from_secs(5), resp_stream.next())
        .await
        .expect("response timed out")
        .expect("stream closed")
        .expect("stream error");

    match resp.frame {
        Some(watch_notify_response::Frame::Error(err)) => {
            assert!(
                !err.not_leader_hint.is_empty(),
                "follower should return non-empty not_leader_hint"
            );
        }
        other => panic!("expected Error with not_leader_hint, got {other:?}"),
    }

    cluster.shutdown().await;
}
