use std::sync::Arc;

use crate::testkit::logging::init_test_subscriber;
use crowkv::cluster::group::PxGroup;
use crowkv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::px_service_client::PxServiceClient;
use tonic::transport::Channel;

pub struct TestCluster {
    nodes: Vec<Arc<PxKvStore>>,
    leader_id: u64,
}

impl TestCluster {
    fn new(nodes: Vec<Arc<PxKvStore>>, leader_id: u64) -> Self {
        Self { nodes, leader_id }
    }

    pub fn nodes(&self) -> &[Arc<PxKvStore>] {
        &self.nodes
    }

    pub fn leader(&self) -> &Arc<PxKvStore> {
        self.nodes
            .iter()
            .find(|n| n.get_group(1).expect("group exists").local_replica().id == self.leader_id)
            .expect("leader present")
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
        PxServiceClient::connect(format!("http://{}", node.listen_addr().expect("server not started")))
            .await
            .expect("connect PxService")
    }

    pub async fn kv_client(&self, node: &Arc<PxKvStore>) -> KvServiceClient<Channel> {
        KvServiceClient::connect(format!("http://{}", node.listen_addr().expect("server not started")))
            .await
            .expect("connect KvService")
    }
}

pub async fn start_cluster(ids: &[u64], leader_id: u64) -> TestCluster {
    start_cluster_inner(ids, leader_id, false).await
}

#[allow(dead_code)]
pub async fn start_cluster_classic(ids: &[u64], leader_id: u64) -> TestCluster {
    start_cluster_inner(ids, leader_id, true).await
}

async fn start_cluster_inner(ids: &[u64], leader_id: u64, force_classic: bool) -> TestCluster {
    init_test_subscriber();

    let mut running = Vec::with_capacity(ids.len());
    for &id in ids {
        let role = if id == leader_id { PxLocalReplicaRole::Leader } else { PxLocalReplicaRole::Follower };
        let replica = PxLocalReplica::new(id, role);

        let store = PxKvStore::new(id, "127.0.0.1:0".parse().unwrap());
        let server = Arc::new(store);

        let remote_replicas: Vec<PxRemoteReplica> = ids
            .iter()
            .filter(|&&other_id| other_id != id)
            .map(|&other_id| PxRemoteReplica::new(other_id, format!("127.0.0.1:{}", other_id + 10000)))
            .collect();

        let mut group = PxGroup::new(1, replica);
        group.set_remote_replicas(remote_replicas);
        group.set_leader_id(leader_id);

        if force_classic {
            group.set_force_classic(true);
        }

        server.add_group(group);

        assert!(server.start().await, "failed to start KvStore");

        running.push(server);
    }

    // Update endpoints to actual bound addresses
    let bound_endpoints: Vec<_> = running
        .iter()
        .map(|node| {
            let group = node.get_group(1).expect("group exists");
            (group.local_replica().id, node.listen_addr().expect("server not started").to_string())
        })
        .collect();
    for node in &running {
        let group = node.get_group(1).expect("group should exist");
        let group_id = group.group_id;
        let leader_id = group.leader_id;
        let force_classic = group.force_classic();
        let lr = group.local_replica();
        let local_replica = PxLocalReplica::new(lr.id, lr.role);

        // Reconstruct remote replicas with updated endpoints
        let my_id = group.local_replica().id;
        let remote_replicas: Vec<PxRemoteReplica> = bound_endpoints
            .iter()
            .filter(|(node_id, _)| *node_id != my_id)
            .map(|(node_id, endpoint)| PxRemoteReplica::new(*node_id, endpoint.clone()))
            .collect();

        let mut new_group = PxGroup::new(group_id, local_replica);
        new_group.set_remote_replicas(remote_replicas);
        new_group.set_leader_id(leader_id);

        if force_classic {
            new_group.set_force_classic(true);
        }

        node.add_group(new_group);
    }

    TestCluster::new(running, leader_id)
}

#[allow(dead_code)]
pub async fn assert_all_accepted(cluster: &TestCluster, slot: u64, expected_payload: &[u8]) {
    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let accepted = replica.accepted_at(slot).await.unwrap_or_else(|| panic!("replica {} missing slot {}", replica.id, slot));
        assert_eq!(*accepted.payload, expected_payload, "replica {} has wrong payload at slot {}", replica.id, slot);
    }
}
