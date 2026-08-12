// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Group-layer Paxos error propagation: not-leader hint surfacing, preemption
//! retry through `PxGroup::propose`, and gRPC boundary rejection. The pure
//! error-classifier unit tests live at `tests/paxos/error_test.rs`.

use crate::testkit::cluster::start_cluster;
use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::kv_store::KvStore;
use crow_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crow_kv::paxos::roles::PxBallot;
use crow_kv::rpc::{AcceptRequest, PrepareRequest};
use std::net::SocketAddr;

#[tokio::test]
async fn follower_request_maps_to_not_leader_with_hint() {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    let remote_replicas = vec![
        PxRemoteReplica::new(42, "127.0.0.1:4444".to_string()),
        PxRemoteReplica::new(7, "127.0.0.1:7777".to_string()),
    ];
    let local_replica = PxLocalReplica::new(7, PxLocalReplicaRole::Follower);
    let mut group = PxGroup::new(1, local_replica);
    group.set_remote_replicas(remote_replicas);
    group.local_replica().set_believed_leader(42);
    store.add_group(group);

    let resp = store.kv_put(1, b"k", b"v", 13, 1, 301, 3001).await;

    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, "127.0.0.1:4444");
}

#[tokio::test]
async fn prepare_rejection_blocks_low_ballot_until_retry_uses_higher_ballot() {
    let cluster = start_cluster(&[0, 1, 2, 3, 4], 0).await;
    let high_ballot = PxBallot::new(10, 99);

    // Pre-empt some replicas with a high ballot
    for node in cluster
        .nodes()
        .iter()
        .filter(|n| n.get_group(1).expect("group exists").local_replica().id != 0)
        .take(3)
    {
        let mut client = cluster.px_client(node).await;
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot: 1,
                round: high_ballot.round,
                leader_id: high_ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id: 1,
                term: 0,
                membership_epoch: 0,
            })
            .await
            .expect("prepare request")
            .into_inner();
        assert!(!resp.rejected);
    }

    // Use PxGroup::propose() - it should detect preemption, bump ballot, and succeed
    let leader = cluster.leader();
    let group = leader.get_group(1).expect("group exists");
    let result = group.propose(b"test-value".to_vec(), Some(1), Some(1)).await;
    match result {
        crow_kv::cluster::group::ProposeResult::Chosen { slot: _ } => {
            // Success - PxGroup handled the ballot bump internally
        }
        _ => panic!("Expected Chosen result, got {result:?}"),
    }

    cluster.shutdown().await;
}

#[tokio::test]
async fn malformed_accept_request_is_rejected_by_grpc_boundary() {
    let cluster = start_cluster(&[0, 1, 2], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.px_client(leader).await;

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
            membership_epoch: 0,
            dedup_tags: Vec::new(),
        })
        .await
        .expect_err("missing value should be rejected");

    // Step 10.7: unary `Accept` is retired (proposers use `LearnerStream`),
    // so the server now returns `Unimplemented` regardless of payload
    // shape. The boundary input-validation that previously surfaced
    // `InvalidArgument` (missing `value`) now lives behind the bidi
    // stream's `handle_accept_inner` helper, exercised by the M3
    // integration tests rather than this unary-only smoke test.
    assert_eq!(status.code(), tonic::Code::Unimplemented);

    cluster.shutdown().await;
}
