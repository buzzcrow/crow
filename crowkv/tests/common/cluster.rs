use std::net::SocketAddr;

use crate::common::logging::init_test_subscriber;
use crowkv::group::group::{PxGroup, PxGroupConfig, PxGroupMember};
use crowkv::node::server::NodeServer;
use crowkv::node::{PxNode, PxNodeRole, PxPaxosMode};
use crowkv::paxos::roles::Ballot as PxBallot;
use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::px_service_client::PxServiceClient;
use crowkv::rpc::{AcceptRequest, AcceptedValue, PrepareRequest};
use tonic::transport::Channel;

/// Running in-process node together with its bound gRPC endpoint.
pub struct RunningNode {
    pub node: PxNode,
    //TODO use different port
    pub addr: SocketAddr,
}

#[allow(dead_code)]
impl RunningNode {
    pub async fn px_client(&self) -> PxServiceClient<Channel> {
        //TODO use different port
        PxServiceClient::connect(format!("http://{}", self.addr))
            .await
            .expect("connect PxService")
    }

    pub async fn kv_client(&self) -> KvServiceClient<Channel> {
        //TODO use different port
        KvServiceClient::connect(format!("http://{}", self.addr))
            .await
            .expect("connect KvService")
    }
}

pub struct TestCluster {
    nodes: Vec<RunningNode>,
    leader_id: u64,
}

#[allow(dead_code)]
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
            .find(|n| n.node.id == self.leader_id)
            .expect("leader present")
    }

    pub fn follower(&self) -> Option<&RunningNode> {
        self.nodes.iter().find(|n| n.node.id != self.leader_id)
    }

    pub fn quorum(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.node.stop();
        }
        for node in self.nodes {
            node.node.join().await;
        }
    }
}

pub async fn start_cluster(
    ids: &[u64],
    leader_id: u64,
    mode: PxPaxosMode,
    _attach_peers: bool,
) -> TestCluster {
    init_test_subscriber();

    let nodes: Vec<PxNode> = ids
        .iter()
        .map(|&id| {
            let role = if id == leader_id {
                PxNodeRole::Leader
            } else {
                PxNodeRole::Follower
            };
            PxNode::new(id, role, mode)
        })
        .collect();

    let mut running = Vec::with_capacity(nodes.len());
    for node in nodes {
        assert!(node.start().await, "failed to start PxNode");
        let addr = node
            .listen_addr()
            .expect("listener address should be available after start");
        running.push(RunningNode { node, addr });
    }

    let members: Vec<PxGroupMember> = running
        .iter()
        .map(|node| PxGroupMember {
            node_id: node.node.id,
            endpoint: node.addr.to_string(),
            voting: true,
        })
        .collect();
    let quorum_size = (members.len() / 2) + 1;
    for node in running.iter_mut() {
        node.node.with_group(PxGroup::new(
            PxGroupConfig {
                group_id: 1,
                members: members.clone(),
                quorum_size,
                config_version: 1,
            },
            leader_id,
            node.node.id,
        ));
    }

    TestCluster::new(running, leader_id)
}

#[allow(dead_code)]
pub async fn assert_all_accepted(cluster: &TestCluster, slot: u64, expected_payload: &[u8]) {
    for node in cluster.nodes() {
        let accepted = node
            .node
            .accepted_at(slot)
            .await
            .unwrap_or_else(|| panic!("node {} missing slot {}", node.node.id, slot));
        assert_eq!(
            accepted.payload, expected_payload,
            "node {} has wrong payload at slot {}",
            node.node.id, slot
        );
    }
}

pub struct GrpcProposer<'a> {
    cluster: &'a TestCluster,
}

#[allow(dead_code)]
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
