mod common;

use common::cluster::{start_cluster, GrpcProposer};
use crowkv::group::group::{PxGroup, PxGroupConfig, PxGroupMember};
use crowkv::node::{PxNode, PxNodeRole, PxPaxosMode};
use crowkv::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crowkv::paxos::roles::Ballot as PxBallot;
use crowkv::rpc::{AcceptRequest, PrepareRequest};

#[test]
fn paxos_error_classifier_maps_prepare_rejection_to_same_slot_prepare() {
    let error = PxPaxosError::PrepareRejected {
        promised: PxBallot::new(10, 2),
    };

    assert_eq!(error.keyword(), "prepare_rejected");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: Some(10),
            force_prepare: true,
        }
    );
}

#[test]
fn paxos_error_classifier_maps_accept_rejection_to_classic_repair() {
    let error = PxPaxosError::AcceptRejected {
        promised: PxBallot::new(10, 2),
    };

    assert_eq!(error.keyword(), "accept_rejected");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: Some(11),
            force_prepare: true,
        }
    );
}

#[test]
fn paxos_error_classifier_keeps_transport_on_same_slot_without_ballot_bump() {
    let error = PxPaxosError::TransportFailure {
        phase: PxPaxosPhase::Accept,
        message: "timeout".to_string(),
    };

    assert_eq!(error.keyword(), "transport_failure");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: None,
            force_prepare: false,
        }
    );
}

#[tokio::test]
async fn follower_request_maps_to_not_leader_with_hint() {
    let mut node = PxNode::new(7, PxNodeRole::Follower, PxPaxosMode::Leader);
    node.with_group(PxGroup::new(
        PxGroupConfig {
            group_id: 1,
            members: vec![PxGroupMember {
                node_id: 42,
                endpoint: "127.0.0.1:4444".to_string(),
                voting: true,
            }],
            quorum_size: 1,
            config_version: 1,
        },
        42,
        7,
    ));

    let resp = node
        .kv_put(b"k".to_vec(), b"v".to_vec(), 13, 1, 301, 3001)
        .await;

    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, "127.0.0.1:4444");
}

#[tokio::test]
async fn prepare_rejection_blocks_low_ballot_until_retry_uses_higher_ballot() {
    let cluster = start_cluster(&[0, 1, 2, 3, 4], 0, PxPaxosMode::Classic, false).await;
    let high_ballot = PxBallot::new(10, 99);

    for node in cluster.nodes().iter().filter(|n| n.node.id != 0).take(3) {
        let mut client = node.px_client().await;
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot: 1,
                round: high_ballot.round,
                leader_id: high_ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
            })
            .await
            .expect("prepare request")
            .into_inner();
        assert!(!resp.rejected);
    }

    let proposer = GrpcProposer::new(&cluster);
    assert!(
        !proposer
            .classic_round(1, PxBallot::new(1, 0), b"low".to_vec())
            .await
    );
    assert!(
        proposer
            .classic_round(1, high_ballot, b"high".to_vec())
            .await
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn malformed_accept_request_is_rejected_by_grpc_boundary() {
    let cluster = start_cluster(&[0, 1, 2], 0, PxPaxosMode::Leader, false).await;
    let mut client = cluster.leader().px_client().await;

    let status = client
        .accept(AcceptRequest {
            version: 1,
            slot: 1,
            round: 1,
            leader_id: 0,
            term: 0,
            value: None,
            request_id: 0,
            request_create_ms: 0,
            client_id: 0,
            seq: 0,
        })
        .await
        .expect_err("missing value should be rejected");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cluster.shutdown().await;
}
