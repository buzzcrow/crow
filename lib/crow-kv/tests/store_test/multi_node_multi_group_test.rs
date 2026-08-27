// Copyright 2026-present buzzcrow <buzzcrow@126.com>

//! Multi-node, multi-group store tests.
//!
//! Verifies that a single `PxKvStore` hosting multiple groups correctly
//! routes KV operations to the appropriate group across a multi-node
//! cluster, and that groups are isolated from each other.

use std::time::{Duration, Instant};

use bytes::Bytes;
use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::group_election::LeaderElection;
use crow_kv::cluster::{KvServer, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crow_kv::common::config::PxElectionConfig;
use crow_kv::rpc::{KvGetRequest, KvSetRequest};

use crate::common::cluster::{start_cluster_no_leader, TestCluster};
use crate::common::test_client::TestKvClient;

async fn wait_for_leader_in_group(cluster: &TestCluster, group_id: u64, timeout: Duration) -> Option<u64> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        for node in cluster.nodes() {
            if let Some(group) = node.get_group(group_id) {
                if group.local_replica().is_leader() {
                    return Some(group.local_replica().id);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    None
}

async fn put_to_group(client: &mut TestKvClient, group_id: u64, key: &[u8], val: &[u8], req_id: u64) -> bool {
    let resp = client
        .put(KvSetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(val),
            ttl_ms: 0,
            request_id: req_id,
            request_create_ms: req_id,
            client_id: 0,
            seq: 0,
            group_id,
        })
        .await
        .expect("put rpc")
        .into_inner();
    resp.ok
}

async fn get_from_group(client: &mut TestKvClient, group_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    let resp = client
        .get(KvGetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            request_id: 9001,
            request_create_ms: 9001,
            group_id,
            read_mode: 0,
            min_slot: 0,
        })
        .await
        .ok()?
        .into_inner();
    if resp.ok && !resp.not_found {
        Some(resp.value.to_vec())
    } else {
        None
    }
}

/// Add a second group (group 2) to every node in the cluster, wiring
/// remotes the same way as group 1 but with a different `group_id`.
fn add_second_group_to_cluster(cluster: &TestCluster) {
    let cfg = PxElectionConfig {
        election_min_ms: 500,
        election_max_ms: 1000,
        lease_duration_ms: 1100,
        ..PxElectionConfig::for_e2e()
    };

    // First pass: create group 2 on each node with placeholder remotes.
    for node in cluster.nodes() {
        let g1 = node.get_group(1).expect("group 1 exists");
        let my_id = g1.local_replica().id;
        let replica = PxLocalReplica::new(my_id, PxLocalReplicaRole::Follower);

        let mut group = PxGroup::new(2, replica);
        group.set_election_config(cfg);
        node.add_group(group);
    }

    // Second pass: rebuild group 2 with bound endpoints.
    let bound: Vec<_> = cluster
        .nodes()
        .iter()
        .map(|n| {
            let g1 = n.get_group(1).expect("group 1");
            (
                g1.local_replica().id,
                n.listen_addr().expect("started").to_string(),
            )
        })
        .collect();

    for node in cluster.nodes() {
        let g1 = node.get_group(1).expect("group 1");
        let my_id = g1.local_replica().id;
        let replica = PxLocalReplica::new(my_id, PxLocalReplicaRole::Follower);

        let remotes: Vec<PxRemoteReplica> = bound
            .iter()
            .filter(|(id, _)| *id != my_id)
            .map(|(id, ep)| PxRemoteReplica::new(*id, ep.clone()))
            .collect();

        let mut group = PxGroup::new(2, replica);
        group.set_remote_replicas(remotes);
        group.set_election_config(cfg);
        node.add_group(group);
    }
}

/// A multi-node cluster with two groups: writes to group 1 and group 2
/// are routed independently, and keys in group 1 are not visible in
/// group 2 (and vice versa).
#[tokio::test]
async fn multi_node_multi_group_routing_and_isolation() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    // Wait for group 1 leader.
    let _g1_leader = wait_for_leader_in_group(&cluster, 1, Duration::from_secs(5))
        .await
        .expect("group 1 leader elected");

    // Add group 2 to all nodes.
    add_second_group_to_cluster(&cluster);

    // Wait for group 2 leader.
    let _g2_leader = wait_for_leader_in_group(&cluster, 2, Duration::from_secs(5))
        .await
        .expect("group 2 leader elected");

    // Find the leader node for each group.
    let g1_leader_node = cluster
        .nodes()
        .iter()
        .find(|n| n.get_group(1).expect("g1").local_replica().is_leader())
        .expect("g1 leader");

    let g2_leader_node = cluster
        .nodes()
        .iter()
        .find(|n| n.get_group(2).expect("g2").local_replica().is_leader())
        .expect("g2 leader");

    let mut g1_client = cluster.kv_client(g1_leader_node).await;
    let mut g2_client = cluster.kv_client(g2_leader_node).await;

    // Write to group 1.
    assert!(
        put_to_group(&mut g1_client, 1, b"g1-key", b"g1-val", 1).await,
        "group 1 write should commit"
    );

    // Write to group 2.
    assert!(
        put_to_group(&mut g2_client, 2, b"g2-key", b"g2-val", 2).await,
        "group 2 write should commit"
    );

    // Verify isolation: group 1 has g1-key but not g2-key.
    assert_eq!(
        get_from_group(&mut g1_client, 1, b"g1-key").await.as_deref(),
        Some(b"g1-val".as_slice()),
    );
    assert!(
        get_from_group(&mut g1_client, 1, b"g2-key").await.is_none(),
        "g2-key must not be visible in group 1"
    );

    // Verify isolation: group 2 has g2-key but not g1-key.
    assert_eq!(
        get_from_group(&mut g2_client, 2, b"g2-key").await.as_deref(),
        Some(b"g2-val".as_slice()),
    );
    assert!(
        get_from_group(&mut g2_client, 2, b"g1-key").await.is_none(),
        "g1-key must not be visible in group 2"
    );

    // Cross-group write: write a different key to each group and verify
    // they don't interfere.
    assert!(put_to_group(&mut g1_client, 1, b"shared", b"from-g1", 3).await);
    assert!(put_to_group(&mut g2_client, 2, b"shared", b"from-g2", 4).await);

    assert_eq!(
        get_from_group(&mut g1_client, 1, b"shared").await.as_deref(),
        Some(b"from-g1".as_slice()),
    );
    assert_eq!(
        get_from_group(&mut g2_client, 2, b"shared").await.as_deref(),
        Some(b"from-g2".as_slice()),
    );

    cluster.shutdown().await;
}
