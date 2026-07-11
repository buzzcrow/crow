use std::net::SocketAddr;
use std::sync::Arc;

use crate::testkit::logging::init_test_subscriber;
use crowkv::cluster::group::PxGroup;
use crowkv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::paxos::roles::PxBallot;
use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::px_service_client::PxServiceClient;
use crowkv::rpc::{AcceptRequest, AcceptedValue, PrepareRequest};
use tonic::transport::Channel;

/// Running in-process node server together with its bound gRPC endpoint.
pub struct RunningNode {
    pub server: Arc<PxKvStore>,
    pub addr: SocketAddr,
}

impl RunningNode {
    pub async fn px_client(&self) -> PxServiceClient<Channel> {
        PxServiceClient::connect(format!("http://{}", self.addr))
            .await
            .expect("connect PxService")
    }

    pub async fn kv_client(&self) -> KvServiceClient<Channel> {
        KvServiceClient::connect(format!("http://{}", self.addr))
            .await
            .expect("connect KvService")
    }

    pub fn group(&self) -> Arc<PxGroup> {
        self.server
            .get_group(1)
            .expect("group exists")
    }
}

pub struct TestCluster {
    nodes: Vec<RunningNode>,
    leader_id: u64,
}

impl TestCluster {
    fn new(nodes: Vec<RunningNode>, leader_id: u64) -> Self {
        Self { nodes, leader_id }
    }

    pub fn nodes(&self) -> &[RunningNode] {
        &self.nodes
    }

    pub fn leader(&self) -> &RunningNode {
        self.nodes
            .iter()
            .find(|n| n.group().local_replica().id == self.leader_id)
            .expect("leader present")
    }

    pub fn follower(&self) -> Option<&RunningNode> {
        self.nodes.iter().find(|n| n.group().local_replica().id != self.leader_id)
    }

    pub fn quorum(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.server.stop();
        }
        for node in self.nodes {
            node.server.join().await;
        }
    }
}

pub async fn start_cluster(ids: &[u64], leader_id: u64) -> TestCluster {
    start_cluster_inner(ids, leader_id, false).await
}

pub async fn start_cluster_classic(ids: &[u64], leader_id: u64) -> TestCluster {
    start_cluster_inner(ids, leader_id, true).await
}

async fn start_cluster_inner(ids: &[u64], leader_id: u64, force_classic: bool) -> TestCluster {
    init_test_subscriber();

    let mut running = Vec::with_capacity(ids.len());
    for &id in ids {
        let role = if id == leader_id {
            PxLocalReplicaRole::Leader
        } else {
            PxLocalReplicaRole::Follower
        };
        let replica = PxLocalReplica::new(id, role);

        let store = PxKvStore::new("127.0.0.1:0".parse().unwrap());
        let server = Arc::new(store);

        let remote_replicas: Vec<PxRemoteReplica> = ids
            .iter()
            .filter(|&&other_id| other_id != id)
            .map(|&other_id| {
                PxRemoteReplica::new(other_id, format!("127.0.0.1:{}", other_id + 10000))
            })
            .collect();

        let mut group = PxGroup::new(1, replica);
        group.set_remote_replicas(remote_replicas);
        group.set_leader_id(leader_id);

        if force_classic {
            group.set_force_classic(true);
        }

        server.add_group(group);

        assert!(server.start().await, "failed to start KvStore");
        let addr = server
            .listen_addr()
            .expect("listener address should be available after start");

        running.push(RunningNode { server, addr });
    }

    // Update endpoints to actual bound addresses
    let bound_endpoints: Vec<_> = running
        .iter()
        .map(|node| {
            let group = node.group();
            (group.local_replica().id, node.addr.to_string())
        })
        .collect();
    for node in &running {
        let group = node.server.get_group(1).expect("group should exist");
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

        node.server.add_group(new_group);
    }

    TestCluster::new(running, leader_id)
}

pub async fn assert_all_accepted(cluster: &TestCluster, slot: u64, expected_payload: &[u8]) {
    for node in cluster.nodes() {
        let group = node.group();
        let replica = group.local_replica();
        let accepted = replica
            .accepted_at(slot)
            .await
            .unwrap_or_else(|| panic!("replica {} missing slot {}", replica.id, slot));
        assert_eq!(
            accepted.payload, expected_payload,
            "replica {} has wrong payload at slot {}",
            replica.id, slot
        );
    }
}

pub struct GrpcProposer<'a> {
    cluster: &'a TestCluster,
}

impl<'a> GrpcProposer<'a> {
    pub fn new(cluster: &'a TestCluster) -> Self {
        Self { cluster }
    }

    pub async fn classic_round(&self, slot: u64, ballot: PxBallot, payload: Vec<u8>) -> bool {
        let mut promises = 0usize;
        let mut adopted: Option<AcceptedValue> = None;

        for node in self.cluster.nodes() {
            let mut client = node.px_client().await;
            let resp = client
                .prepare(PrepareRequest {
                    version: 1,
                    slot,
                    round: ballot.round,
                    leader_id: ballot.leader_id,
                    request_id: 0,
                    request_create_ms: 0,
                    group_id: 1,
                })
                .await
                .expect("prepare request")
                .into_inner();

            if resp.rejected {
                continue;
            }

            promises += 1;
            if let Some(prev) = resp.previously_accepted {
                let replace = adopted
                    .as_ref()
                    .map(|current| {
                        (prev.round, prev.leader_id) > (current.round, current.leader_id)
                    })
                    .unwrap_or(true);
                if replace {
                    adopted = Some(prev);
                }
            }
        }

        if promises < self.cluster.quorum() {
            return false;
        }

        let payload = adopted.map(|v| v.payload).unwrap_or(payload);
        self.accept_phase(slot, ballot, payload).await
    }

    pub async fn optimized_round(&self, slot: u64, ballot: PxBallot, payload: Vec<u8>) -> bool {
        self.accept_phase(slot, ballot, payload).await
    }

    async fn accept_phase(&self, slot: u64, ballot: PxBallot, payload: Vec<u8>) -> bool {
        let mut accepted = 0usize;
        for node in self.cluster.nodes() {
            let mut client = node.px_client().await;
            let req = AcceptRequest {
                version: 1,
                slot,
                round: ballot.round,
                leader_id: ballot.leader_id,
                term: 0,
                value: Some(AcceptedValue {
                    slot,
                    round: ballot.round,
                    leader_id: ballot.leader_id,
                    term: 0,
                    payload: payload.clone(),
                }),
                request_id: 0,
                request_create_ms: 0,
                client_id: 0,
                seq: 0,
                group_id: 1,
            };
            let resp = client
                .accept(req)
                .await
                .expect("accept request")
                .into_inner();
            if !resp.rejected {
                accepted += 1;
            }
        }

        accepted >= self.cluster.quorum()
    }
}
