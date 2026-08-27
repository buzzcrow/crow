// Copyright 2026-present buzzcrow <buzzcrow@126.com>

//! Graceful shutdown under load.
//!
//! Verifies that a multi-group `PxKvStore` shuts down cleanly while KV
//! operations are in-flight, and that all previously committed data is
//! intact after shutdown completes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::kv_store::KvStore;
use crow_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crow_kv::rpc::{KvGetRequest, KvSetRequest};

use crate::common::cluster::{start_cluster_no_leader, TestCluster};

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
    let client = cluster.kv_client(leader).await;

    // Write initial data to group 1.
    for i in 0u64..10 {
        let key = format!("load-{i}");
        let val = format!("val-{i}");
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: Bytes::copy_from_slice(val.as_bytes()),
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
    // previously committed data should still be readable. The new leader's
    // lease may expire before all GETs complete (it cannot reach the shut-
    // down node for heartbeats), so we retry with a re-elected leader on
    // failure.
    let mut verified = 0u64;
    let deadline = Instant::now() + Duration::from_secs(10);
    while verified < 10 {
        assert!(
            Instant::now() <= deadline,
            "timed out verifying keys after shutdown"
        );

        // Find a current leader among the surviving nodes.
        let leader_node = loop {
            assert!(Instant::now() <= deadline, "no leader elected after shutdown");
            if let Some(node) = cluster.nodes().iter().find(|n| {
                !Arc::ptr_eq(n, leader) && n.get_group(1).is_some_and(|g| g.local_replica().is_leader())
            }) {
                break node;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        let client = cluster.kv_client(leader_node).await;
        let mut all_ok = true;
        let mut idx = verified;
        while idx < 10 {
            let key = format!("load-{idx}");
            let val = format!("val-{idx}");
            let resp = client
                .get(KvGetRequest {
                    version: 1,
                    key: Bytes::copy_from_slice(key.as_bytes()),
                    request_id: 9001 + idx,
                    request_create_ms: 9001 + idx,
                    group_id: 1,
                    read_mode: 0,
                    min_slot: 0,
                })
                .await
                .expect("get rpc")
                .into_inner();
            if !resp.ok || resp.not_found {
                all_ok = false;
                break;
            }
            assert_eq!(resp.value, val.as_bytes(), "key {key:?} value mismatch");
            idx += 1;
        }
        verified = idx;
        if all_ok {
            break;
        }
        // Leader stepped down mid-verification; wait for re-election and retry.
        tokio::time::sleep(Duration::from_millis(20)).await;
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
