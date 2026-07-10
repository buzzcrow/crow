//! Test harness: spawn CrowKV nodes as in-process tasks with real TCP listeners.
//!
//! `TestNodeHarness` starts a `Node` gRPC server in a background `tokio` task,
//! binds a real `TcpListener` on `127.0.0.1:0`, and returns the resolved
//! address so the caller can wire peer connections.
//!
//! `MinimalProposer` drives classic / optimized / multi-Paxos rounds over
//! these loopback connections.
//!
//! Used by P1 M2 integration tests. See `doc/plan/plan-consensus.md` §1 M2.3.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::node::{Node, NodeRole};
use crate::paxos::slot_list::SlotIndex;
use crate::paxos::slot_node::PxBallot;
use crate::rpc::peer_service_client::PeerServiceClient;
use crate::rpc::{AcceptRequest, AcceptedValue, PrepareRequest};
use tonic::transport::Channel;

/// Handle to a spawned test node.
///
/// Dropping the handle sends the shutdown signal, stopping the gRPC server.
pub struct TestNodeHarness {
    pub node_id: u64,
    pub listen_addr: SocketAddr,
    pub role: NodeRole,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TestNodeHarness {
    /// Spawn a new node and start its gRPC server in a background task.
    ///
    /// # Panics
    ///
    /// Panics if the TCP listener cannot be bound.
    pub async fn spawn(node_id: u64, role: NodeRole) -> Self {
        let node = Node::new(node_id, role);
        let (listen_addr, shutdown) = node.serve().await.expect("bind failed");
        Self {
            node_id,
            listen_addr,
            role,
            shutdown: Some(shutdown),
        }
    }
}

impl Drop for TestNodeHarness {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Minimal proposer for P1 M2: drives classic / optimized / multi-Paxos rounds
/// against a set of peer addresses via real loopback RPC.
pub struct MinimalProposer {
    pub node_id: u64,
    peers: Vec<PeerServiceClient<Channel>>,
    quorum: usize,
}

impl MinimalProposer {
    /// Create a proposer connected to **all** nodes (including itself).
    ///
    /// `all_addrs` maps node_id → SocketAddr. The proposer connects to
    /// every node via loopback RPC so that even the local acceptor receives
    /// requests through the full wire stack.
    pub async fn new(
        node_id: u64,
        all_addrs: &HashMap<u64, SocketAddr>,
    ) -> Result<Self, tonic::transport::Error> {
        let mut peers = Vec::new();
        for (_id, addr) in all_addrs {
            let endpoint = format!("http://{}", addr);
            let client = PeerServiceClient::connect(endpoint).await?;
            peers.push(client);
        }
        let peer_count = peers.len();
        let quorum = (peer_count / 2) + 1;
        Ok(Self {
            node_id,
            peers,
            quorum,
        })
    }

    /// Classic Paxos: Phase 1 (Prepare → wait quorum Promise) +
    /// Phase 2 (Accept → wait quorum Accepted).
    ///
    /// Returns `true` if the value was chosen (quorum of Accepted).
    pub async fn classic_round(
        &mut self,
        slot: SlotIndex,
        ballot: PxBallot,
        payload: Vec<u8>,
    ) -> bool {
        // Phase 1: Prepare
        let prepare_req = PrepareRequest {
            version: 1,
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
        };

        let mut promise_count = 0;
        let mut highest_accepted: Option<AcceptedValue> = None;

        // Count self (local acceptor) — not implemented in this minimal version,
        // we rely on peer responses only for M2 and assume the leader's own
        // acceptor is not in the peer list.
        // TODO: invoke local acceptor directly for accurate quorum counting.

        for peer in &mut self.peers {
            if let Ok(resp) = peer.prepare(prepare_req.clone()).await {
                let p = resp.into_inner();
                if !p.rejected {
                    promise_count += 1;
                    if let Some(prev) = p.previously_accepted {
                        // Classic Paxos: adopt the highest previously accepted value.
                        if highest_accepted.as_ref().map_or(true, |h| {
                            (prev.round, prev.leader_id) > (h.round, h.leader_id)
                        }) {
                            highest_accepted = Some(prev);
                        }
                    }
                }
            }
        }

        if promise_count < self.quorum {
            return false;
        }

        // Phase 2: Accept
        let value = highest_accepted.unwrap_or_else(|| AcceptedValue {
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
            term: 0, // term not used in M2
            payload,
        });

        let accept_req = AcceptRequest {
            version: 1,
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
            term: 0,
            value: Some(value),
        };

        let mut accept_count = 0;
        for peer in &mut self.peers {
            if let Ok(resp) = peer.accept(accept_req.clone()).await {
                let a = resp.into_inner();
                if !a.rejected {
                    accept_count += 1;
                }
            }
        }

        accept_count >= self.quorum
    }

    /// Optimized Paxos: skip Phase 1, drive `Accept` directly.
    /// Assumes the leader already holds the highest ballot.
    pub async fn optimized_round(
        &mut self,
        slot: SlotIndex,
        ballot: PxBallot,
        payload: Vec<u8>,
    ) -> bool {
        let value = AcceptedValue {
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
            term: 0,
            payload,
        };

        let accept_req = AcceptRequest {
            version: 1,
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
            term: 0,
            value: Some(value),
        };

        let mut accept_count = 0;
        for peer in &mut self.peers {
            if let Ok(resp) = peer.accept(accept_req.clone()).await {
                let a = resp.into_inner();
                if !a.rejected {
                    accept_count += 1;
                }
            }
        }

        accept_count >= self.quorum
    }
}
