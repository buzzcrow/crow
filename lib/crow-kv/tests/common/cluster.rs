// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crate::testkit::logging::init_test_subscriber;
use crate::testkit::net_lock::{lock, unique_port};
use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::group_election::LeaderElection;
use crow_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crow_kv::common::config::PxElectionConfig;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::px_service_client::PxServiceClient;
use tonic::transport::Channel;

pub struct TestCluster {
    nodes: Vec<Arc<PxKvStore>>,
    leader_id: u64,
    _net: tokio::sync::MutexGuard<'static, ()>,
}

impl TestCluster {
    fn new(nodes: Vec<Arc<PxKvStore>>, leader_id: u64, net: tokio::sync::MutexGuard<'static, ()>) -> Self {
        Self {
            nodes,
            leader_id,
            _net: net,
        }
    }

    pub fn nodes(&self) -> &[Arc<PxKvStore>] {
        &self.nodes
    }

    #[allow(dead_code)]
    pub fn leader(&self) -> &Arc<PxKvStore> {
        self.nodes
            .iter()
            .find(|n| n.get_group(1).expect("group exists").local_replica().id == self.leader_id)
            .expect("leader present")
    }

    /// Discover which node currently holds the `Leader` role. Used by
    /// `start_cluster_no_leader` tests where the leader is elected
    /// rather than pinned at construction time.
    #[allow(dead_code)]
    pub fn elected_leader(&self) -> Option<&Arc<PxKvStore>> {
        self.nodes.iter().find(|n| {
            let group = n.get_group(1).expect("group exists");
            group.local_replica().is_leader()
        })
    }

    #[allow(dead_code)]
    pub fn followers(&self) -> Vec<&Arc<PxKvStore>> {
        self.nodes
            .iter()
            .filter(|n| n.get_group(1).expect("group exists").local_replica().id != self.leader_id)
            .collect()
    }

    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.stop();
        }
        for node in self.nodes {
            node.join().await;
        }
    }

    #[allow(dead_code)]
    pub async fn px_client(&self, node: &Arc<PxKvStore>) -> PxServiceClient<Channel> {
        PxServiceClient::connect(format!(
            "http://{}",
            node.listen_addr().expect("server not started")
        ))
        .await
        .expect("connect PxService")
    }

    pub async fn kv_client(&self, node: &Arc<PxKvStore>) -> KvServiceClient<Channel> {
        KvServiceClient::connect(format!(
            "http://{}",
            node.listen_addr().expect("server not started")
        ))
        .await
        .expect("connect KvService")
    }
}

#[allow(dead_code)]
pub async fn start_cluster(ids: &[u64], leader_id: u64) -> TestCluster {
    start_cluster_inner(ids, leader_id, false).await
}

#[allow(dead_code)]
pub async fn start_cluster_classic(ids: &[u64], leader_id: u64) -> TestCluster {
    start_cluster_inner(ids, leader_id, true).await
}

/// Step 12c: start a cluster with no pre-set leader. All replicas come
/// up as `Follower` and the election driver picks a leader using the
/// `PxElectionConfig::for_tests()` profile (5 ms heartbeat / 30–60 ms
/// election timer / 25 ms lease) so tests resolve within a few hundred
/// milliseconds without `tokio::time::advance`.
///
/// Note on small clusters:
/// - 1-node: the lone replica auto-promotes on the first election tick
///   (quorum = 1, self-vote wins).
/// - 2-node: quorum = 2; both replicas must be up at startup for any
///   election to make progress. Neither is leader at boot.
///
/// Use [`TestCluster::elected_leader`] to discover the winner once the
/// election has completed.
#[allow(dead_code)]
pub async fn start_cluster_no_leader(ids: &[u64]) -> TestCluster {
    start_cluster_no_leader_inner(ids).await
}

async fn start_cluster_no_leader_inner(ids: &[u64]) -> TestCluster {
    let net = lock().await;
    init_test_subscriber();

    let cfg = PxElectionConfig::for_tests();
    let mut running = Vec::with_capacity(ids.len());
    for &id in ids {
        let replica = PxLocalReplica::new(id, PxLocalReplicaRole::Follower);

        let store = PxKvStore::new(id, "127.0.0.1:0".parse().unwrap());
        let server = Arc::new(store);

        let remote_replicas: Vec<PxRemoteReplica> = ids
            .iter()
            .filter(|&&other_id| other_id != id)
            .map(|&other_id| PxRemoteReplica::new(other_id, format!("127.0.0.1:{}", unique_port())))
            .collect();

        let mut group = PxGroup::new(1, replica);
        group.set_remote_replicas(remote_replicas);
        group.set_election_config(cfg);
        // Deliberately no `set_leader_id` — let the driver elect.

        server.add_group(group);
        server.start().await.expect("failed to start KvStore");
        running.push(server);
    }

    // Rebuild groups with bound endpoints (same shape as start_cluster_inner).
    let bound_endpoints: Vec<_> = running
        .iter()
        .map(|node| {
            let group = node.get_group(1).expect("group exists");
            (
                group.local_replica().id,
                node.listen_addr().expect("server not started").to_string(),
            )
        })
        .collect();
    for node in &running {
        let group = node.get_group(1).expect("group should exist");
        let group_id = group.group_id;
        let lr = group.local_replica();
        let local_replica = PxLocalReplica::new(lr.id, lr.role());
        let my_id = lr.id;
        let remote_replicas: Vec<PxRemoteReplica> = bound_endpoints
            .iter()
            .filter(|(node_id, _)| *node_id != my_id)
            .map(|(node_id, endpoint)| PxRemoteReplica::new(*node_id, endpoint.clone()))
            .collect();

        let mut new_group = PxGroup::new(group_id, local_replica);
        new_group.set_remote_replicas(remote_replicas);
        new_group.set_election_config(cfg);
        node.add_group(new_group);
    }

    // `leader_id` is unknown until an election completes; seed with the
    // first id as a placeholder so the legacy `leader()` accessor still
    // compiles. Tests should prefer `elected_leader()`.
    TestCluster::new(running, ids.first().copied().unwrap_or(0), net)
}

async fn start_cluster_inner(ids: &[u64], leader_id: u64, force_classic: bool) -> TestCluster {
    let net = lock().await;
    init_test_subscriber();

    let mut running = Vec::with_capacity(ids.len());
    for &id in ids {
        let role = if id == leader_id {
            PxLocalReplicaRole::Leader
        } else {
            PxLocalReplicaRole::Follower
        };
        let replica = PxLocalReplica::new(id, role);
        if id != leader_id {
            replica.set_believed_leader(leader_id);
        }

        let store = PxKvStore::new(id, "127.0.0.1:0".parse().unwrap());
        let server = Arc::new(store);

        let remote_replicas: Vec<PxRemoteReplica> = ids
            .iter()
            .filter(|&&other_id| other_id != id)
            .map(|&other_id| PxRemoteReplica::new(other_id, format!("127.0.0.1:{}", unique_port())))
            .collect();

        let mut group = PxGroup::new(1, replica);
        group.set_remote_replicas(remote_replicas);

        if force_classic {
            group.set_force_classic(true);
        }

        server.add_group(group);

        server.start().await.expect("failed to start KvStore");

        running.push(server);
    }

    // Update endpoints to actual bound addresses
    let bound_endpoints: Vec<_> = running
        .iter()
        .map(|node| {
            let group = node.get_group(1).expect("group exists");
            (
                group.local_replica().id,
                node.listen_addr().expect("server not started").to_string(),
            )
        })
        .collect();
    for node in &running {
        let group = node.get_group(1).expect("group should exist");
        let group_id = group.group_id;
        let force_classic = group.force_classic();
        let lr = group.local_replica();
        let local_replica = PxLocalReplica::new(lr.id, lr.role());
        if let Some(believed) = lr.believed_leader_id() {
            local_replica.set_believed_leader(believed);
        }

        // Reconstruct remote replicas with updated endpoints
        let my_id = group.local_replica().id;
        let remote_replicas: Vec<PxRemoteReplica> = bound_endpoints
            .iter()
            .filter(|(node_id, _)| *node_id != my_id)
            .map(|(node_id, endpoint)| PxRemoteReplica::new(*node_id, endpoint.clone()))
            .collect();

        let mut new_group = PxGroup::new(group_id, local_replica);
        new_group.set_remote_replicas(remote_replicas);

        if force_classic {
            new_group.set_force_classic(true);
        }

        node.add_group(new_group);
    }

    TestCluster::new(running, leader_id, net)
}

#[allow(dead_code)]
pub async fn assert_all_accepted(cluster: &TestCluster, slot: u64, expected_payload: &[u8]) {
    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let accepted = replica
            .accepted_at(slot)
            .await
            .unwrap_or_else(|| panic!("replica {} missing slot {}", replica.id, slot));
        assert_eq!(
            accepted.payload.as_ref(),
            expected_payload,
            "replica {} has wrong payload at slot {}",
            replica.id,
            slot
        );
    }
}
