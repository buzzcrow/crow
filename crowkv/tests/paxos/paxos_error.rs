use crate::testkit::cluster::{start_cluster, GrpcProposer};
use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crowkv::paxos::roles::PxBallot;
use crowkv::rpc::{AcceptRequest, PrepareRequest};
use std::net::SocketAddr;

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
    let store = PxKvStore::new(SocketAddr::from(([127, 0, 0, 1], 0)));
    let remote_replicas = vec![
        PxRemoteReplica::new(42, "127.0.0.1:4444".to_string()),
        PxRemoteReplica::new(7, "127.0.0.1:7777".to_string()),
    ];
    let local_replica = PxLocalReplica::new(7, PxLocalReplicaRole::Follower);
    let mut group = PxGroup::new(1, local_replica);
    group.set_remote_replicas(remote_replicas);
    group.set_leader_id(42);
    store.add_group(group);

    let resp = store
        .kv_put(1, b"k".to_vec(), b"v".to_vec(), 13, 1, 301, 3001)
        .await;

    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, "127.0.0.1:4444");
}

#[tokio::test]
async fn prepare_rejection_blocks_low_ballot_until_retry_uses_higher_ballot() {
    let cluster = start_cluster(&[0, 1, 2, 3, 4], 0).await;
    let high_ballot = PxBallot::new(10, 99);

    for node in cluster
        .nodes()
        .iter()
        .filter(|n| n.group().local_replica().id != 0)
        .take(3)
    {
        let mut client = node.px_client().await;
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot: 1,
                round: high_ballot.round,
                leader_id: high_ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id: 1,
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
    let cluster = start_cluster(&[0, 1, 2], 0).await;
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
            group_id: 1,
        })
        .await
        .expect_err("missing value should be rejected");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    cluster.shutdown().await;
}
