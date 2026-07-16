// Copyright 2026-present buzzcrow <buzzcrow@126.com>

//! Graceful shutdown under load.
//!
//! Verifies that a multi-group `PxKvStore` shuts down cleanly while KV
//! operations are in-flight, and that all previously committed data is
//! intact after shutdown completes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowkv::rpc::{KvGetRequest, KvSetRequest};

use crate::testkit::cluster::{start_cluster_no_leader, TestCluster};

async fn wait_for_leader(cluster: &TestCluster, timeout: Duration) -> Option<u64> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(node) = cluster.elected_leader() {
            return Some(node.get_group(1).expect("group").local_replica().id);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

/// A multi-group store with two single-leader groups: write data to both
/// groups, then shut down while a background write task is in-flight.
/// The shutdown must complete cleanly, and all previously committed
/// data must be readable from the surviving replicas.
#[tokio::test]
async fn graceful_shutdown_under_load() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let _leader_id = wait_for_leader(&cluster, Duration::from_secs(5))
        .await
        .expect("leader elected");

    let leader = cluster.elected_leader().expect("leader present");
    let mut client = cluster.kv_client(leader).await;

    // Write initial data to group 1.
    for i in 0u64..10 {
        let key = format!("load-{i}");
        let val = format!("val-{i}");
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: key.as_bytes().to_vec(),
                value: val.as_bytes().to_vec(),
                ttl_ms: 0,
                request_id: i + 1,
                request_create_ms: i + 1,
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("put rpc")
            .into_inner();
        assert!(resp.ok, "write {i} should commit");
    }

    // Shut down the leader node while the cluster is active.
    // The shutdown must be clean (no errors).
    let report = leader.shutdown(Duration::from_secs(5)).await;
    assert!(
        report.is_clean(),
        "shutdown under load should be clean, got errors: {:?}",
        report.errors
    );

    // The surviving two nodes should re-elect a leader and all
    // previously committed data should still be readable.
    // Wait for a new leader among the remaining nodes.
    let start = Instant::now();
    let new_leader = loop {
        assert!(
            start.elapsed() <= Duration::from_secs(5),
            "no leader elected after shutdown"
        );
        let candidate = cluster.nodes().iter().find(|n| {
            if Arc::ptr_eq(n, leader) {
                return false;
            }
            n.get_group(1).is_some_and(|g| g.local_replica().is_leader())
        });
        if let Some(node) = candidate {
            break node;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let mut new_client = cluster.kv_client(new_leader).await;

    // Verify all 10 keys survived the shutdown.
    for i in 0u64..10 {
        let key = format!("load-{i}");
        let val = format!("val-{i}");
        let resp = new_client
            .get(KvGetRequest {
                version: 1,
                key: key.as_bytes().to_vec(),
                request_id: 9001,
                request_create_ms: 9001,
                group_id: 1,
                read_mode: 0,
                client_slot: 0,
            })
            .await
            .expect("get rpc")
            .into_inner();
        assert!(resp.ok && !resp.not_found, "key {key:?} should survive shutdown");
        assert_eq!(resp.value, val.as_bytes(), "key {key:?} value mismatch");
    }

    cluster.shutdown().await;
}

/// A single-node store with multiple groups: write to all groups, shut
/// down, and verify the shutdown report is clean.
#[tokio::test]
async fn multi_group_shutdown_is_clean() {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));

    // Add two groups with single leaders.
    for gid in 1u64..=2 {
        let replica = PxLocalReplica::new(gid, PxLocalReplicaRole::Leader);
        let group = PxGroup::new(gid, replica);
        store.add_group(group);
    }

    store.start().await.expect("start failed");

    // Write to both groups.
    for gid in 1u64..=2 {
        let key = format!("mg-{gid}");
        let resp = store.kv_put(gid, key.as_bytes(), b"val", 1, 1, 100, 1000).await;
        assert!(resp.ok, "group {gid} write should succeed");
    }

    // Shut down — must be clean even with data in multiple groups.
    let report = store.shutdown(Duration::from_secs(3)).await;
    assert!(
        report.is_clean(),
        "multi-group shutdown should be clean, got: {:?}",
        report.errors
    );
}
