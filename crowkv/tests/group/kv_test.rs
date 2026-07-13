//! KV gRPC integration tests covering Put/Delete/BatchWrite flows.

use crate::testkit::cluster::{start_cluster, TestCluster};
use crowkv::rpc::{KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvSetRequest};

#[tokio::test]
async fn kv_mutations_apply_to_all_learners() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    // Put k1=v1
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            request_id: 101,
            request_create_ms: 1001,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok);

    assert_cluster_value(&cluster, b"k1", Some(b"v1")).await;

    // Batch write: update k1, insert k2
    let resp = client
        .batch_write(KvBatchWriteRequest {
            version: 1,
            items: vec![
                KvBatchItem {
                    key: b"k1".to_vec(),
                    value: b"v2".to_vec(),
                    is_delete: false,
                },
                KvBatchItem {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                    is_delete: false,
                },
            ],
            request_id: 102,
            request_create_ms: 1002,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv batch")
        .into_inner();
    assert!(resp.ok);

    assert_cluster_value(&cluster, b"k1", Some(b"v2")).await;
    assert_cluster_value(&cluster, b"k2", Some(b"v2")).await;

    // Delete k1
    let resp = client
        .delete(KvDeleteRequest {
            version: 1,
            key: b"k1".to_vec(),
            request_id: 103,
            request_create_ms: 1003,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv delete")
        .into_inner();
    assert!(resp.ok);

    assert_cluster_value(&cluster, b"k1", None).await;
    assert_cluster_value(&cluster, b"k2", Some(b"v2")).await;

    drop(client);
    cluster.shutdown().await;
}

async fn assert_cluster_value(cluster: &TestCluster, key: &[u8], expected: Option<&[u8]>) {
    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let value = replica.learner.engine_get(key).await.map(|(_, v)| v);
        match expected {
            Some(bytes) => {
                let stored = value.expect("value missing");
                assert_eq!(stored.as_slice(), bytes);
            }
            None => {
                assert!(value.is_none(), "value for {key:?} should be absent");
            }
        }
    }
}
