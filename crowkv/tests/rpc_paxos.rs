//! P1 M2 integration tests: classic / optimized / multi-Paxos over loopback RPC.
//!
//! Scenarios:
//!   S0-A — Classic Paxos (3-node)
//!   S0-B — Optimized Paxos (3-node)
//!   S0-C — Multi-Paxos (5-node, 10 slots)
//!   S0-D — Quorum with rejection (5-node)

use std::collections::HashMap;
use std::net::SocketAddr;

use crowkv::node::NodeRole;
use crowkv::paxos::slot_node::PxBallot;
use crowkv::rpc::peer_service_client::PeerServiceClient;
use crowkv::rpc::PrepareRequest;
use crowkv::testkit::{MinimalProposer, TestNodeHarness};
use tonic::transport::Channel;

async fn spawn_cluster(
    ids: &[u64],
    leader_id: u64,
) -> (Vec<TestNodeHarness>, HashMap<u64, SocketAddr>) {
    let mut nodes = Vec::new();
    let mut addrs = HashMap::new();

    for &id in ids {
        let role = if id == leader_id {
            NodeRole::Leader
        } else {
            NodeRole::Follower
        };
        let node = TestNodeHarness::spawn(id, role).await;
        addrs.insert(id, node.listen_addr);
        nodes.push(node);
    }

    (nodes, addrs)
}

/// Verify that every node in `addrs` has accepted `payload` at `slot`.
async fn assert_all_accepted(addrs: &HashMap<u64, SocketAddr>, slot: u64, expected_payload: &[u8]) {
    for (id, addr) in addrs {
        let endpoint = format!("http://{}", addr);
        let mut client = PeerServiceClient::<Channel>::connect(endpoint)
            .await
            .expect("connect for verification");

        let probe = PrepareRequest {
            version: 1,
            slot,
            round: 999, // fresh high ballot to force promise
            leader_id: 99,
        };
        let resp = client
            .prepare(probe)
            .await
            .expect("probe prepare")
            .into_inner();

        assert!(
            !resp.rejected,
            "node {} rejected probe ballot for slot {}",
            id, slot
        );

        let prev = resp.previously_accepted.as_ref().expect(&format!(
            "node {} has no accepted value at slot {}",
            id, slot
        ));

        assert_eq!(
            prev.payload, expected_payload,
            "node {} has wrong payload at slot {}",
            id, slot
        );
    }
}

// ---------- S0-A: Classic Paxos (3-node) ----------

#[tokio::test]
async fn integration_loopback_classic_paxos() {
    let (nodes, addrs) = spawn_cluster(&[0, 1, 2], 0).await;
    let mut proposer = MinimalProposer::new(0, &addrs).await.unwrap();

    let ballot = PxBallot {
        round: 1,
        leader_id: 0,
    };
    let chosen = proposer
        .classic_round(1, ballot, b"hello-classic".to_vec())
        .await;
    assert!(chosen, "classic round should reach quorum and choose");

    assert_all_accepted(&addrs, 1, b"hello-classic").await;

    drop(nodes);
}

// ---------- S0-B: Optimized Paxos (3-node) ----------

#[tokio::test]
async fn integration_loopback_optimized_paxos() {
    let (nodes, addrs) = spawn_cluster(&[0, 1, 2], 0).await;
    let mut proposer = MinimalProposer::new(0, &addrs).await.unwrap();

    let ballot = PxBallot {
        round: 1,
        leader_id: 0,
    };
    let chosen = proposer
        .optimized_round(1, ballot, b"hello-optimized".to_vec())
        .await;
    assert!(chosen, "optimized round should reach quorum and choose");

    assert_all_accepted(&addrs, 1, b"hello-optimized").await;

    drop(nodes);
}

// ---------- S0-C: Multi-Paxos (5-node, 10 slots) ----------

#[tokio::test]
async fn integration_loopback_multi_paxos() {
    let (nodes, addrs) = spawn_cluster(&[0, 1, 2, 3, 4], 0).await;
    let mut proposer = MinimalProposer::new(0, &addrs).await.unwrap();

    let ballot = PxBallot {
        round: 1,
        leader_id: 0,
    };

    // Multi-Paxos: Phase 1 once for the ballot, then Phase 2 for each slot.
    // We reuse the same ballot across slots 1..=10.
    for slot in 1..=10 {
        let payload = format!("slot-{}-multi", slot).into_bytes();
        let chosen = proposer
            .optimized_round(slot, ballot, payload.clone())
            .await;
        assert!(chosen, "slot {} should be chosen", slot);
    }

    // Verify every slot on every node.
    for slot in 1..=10 {
        let expected = format!("slot-{}-multi", slot).into_bytes();
        assert_all_accepted(&addrs, slot, &expected).await;
    }

    drop(nodes);
}

// ---------- S0-D: Quorum with rejection (5-node) ----------

#[tokio::test]
async fn integration_loopback_quorum_rejection() {
    let (nodes, addrs) = spawn_cluster(&[0, 1, 2, 3, 4], 0).await;

    // Pre-promise followers 1, 2, and 3 at a higher ballot.
    // With 5 nodes quorum = 3; blocking 3 followers leaves only
    // node 0 (leader) + node 4 = 2 < quorum, so the low-ballot round fails.
    let high_ballot = PxBallot {
        round: 10,
        leader_id: 99,
    };

    for &follower_id in &[1, 2, 3] {
        let addr = addrs[&follower_id];
        let endpoint = format!("http://{}", addr);
        let mut client = PeerServiceClient::<Channel>::connect(endpoint)
            .await
            .expect("connect to pre-promise follower");

        let pre_prepare = PrepareRequest {
            version: 1,
            slot: 1,
            round: high_ballot.round,
            leader_id: high_ballot.leader_id,
        };
        let resp = client.prepare(pre_prepare).await.unwrap().into_inner();
        assert!(!resp.rejected, "pre-prepare should succeed");
    }

    // Leader proposes with a lower ballot (round=1).
    let mut proposer = MinimalProposer::new(0, &addrs).await.unwrap();
    let low_ballot = PxBallot {
        round: 1,
        leader_id: 0,
    };

    // Classic round should fail at Phase 1 because followers 1,2 reject Prepare.
    let chosen = proposer
        .classic_round(1, low_ballot, b"should-fail".to_vec())
        .await;
    assert!(
        !chosen,
        "round with pre-promised higher ballot should fail to reach quorum"
    );

    // Now use the high ballot (round=10) — should succeed.
    let chosen = proposer
        .classic_round(1, high_ballot, b"should-succeed".to_vec())
        .await;
    assert!(chosen, "round with the higher ballot should succeed");

    assert_all_accepted(&addrs, 1, b"should-succeed").await;

    drop(nodes);
}
