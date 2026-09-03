// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-layer Paxos error propagation: not-leader hint surfacing, preemption
//! retry through `PxGroup::propose`, and crowdb-rpc boundary rejection. The pure
//! error-classifier unit tests live at `tests/paxos/error_test.rs`.

use crate::common::cluster::start_cluster;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_kv::paxos::roles::PxBallot;
use crowdb_kv::rpc::PrepareRequest;

#[tokio::test]
async fn follower_request_maps_to_not_leader_with_hint() {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
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
        let client = cluster.px_client(node).await;
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
        crowdb_kv::cluster::group::ProposeResult::Chosen { slot: _ } => {
            // Success - PxGroup handled the ballot bump internally
        }
        _ => panic!("Expected Chosen result, got {result:?}"),
    }

    cluster.shutdown().await;
}

// R120: The server's `PxRpcService::handle_accept` rejects malformed
// `EAcceptRequest` frames with `FBKvRetCode::InvalidArgument` via
// `submit_error`. Two rejection paths exist: (1) the flatbuffer root
// parse fails (`flatbuffers::root::<FBAcceptRequest>` returns Err), and
// (2) the `value` field is missing (`fb_req.value()` returns None). This
// test exercises both paths and asserts the server returns an error
// response without panicking.
#[tokio::test]
async fn malformed_accept_request_is_rejected_by_rpc_boundary() {
    use crowdb_protocol::fb_wrappers::kv_consensus::FBAcceptedResponseRef;
    use crowdb_protocol::kv_consensus_fb::{
        FBAcceptRequest, FBAcceptRequestArgs, FBKvRetCode, FBPrepareRequest, FBPrepareRequestArgs,
    };
    use flatbuffers::FlatBufferBuilder;

    let cluster = start_cluster(&[0, 1], 0).await;
    let leader = cluster.leader();
    let client = cluster.px_client(leader).await;

    // 1. Wrong table type: build a valid `FBPrepareRequest` flatbuffer
    //    and send it with `msg_type = EAcceptRequest`. The handler's
    //    `flatbuffers::root::<FBAcceptRequest>` verification fails because
    //    the vtable layout doesn't match.
    let mut builder = FlatBufferBuilder::new();
    let args = FBPrepareRequestArgs {
        id: 1,
        rpc_create_nano: 0,
        version: 1,
        slot: 1,
        round: 1,
        leader_id: 0,
        term: 0,
        group_id: 1,
        membership_epoch: 0,
    };
    let req = FBPrepareRequest::create(&mut builder, &args);
    builder.finish(req, None);
    let wrong_type_bytes = builder.finished_data().to_vec();

    let resp = client
        .send_raw_accept(wrong_type_bytes)
        .await
        .expect("raw accept RPC should complete (server must not panic)");

    let r = FBAcceptedResponseRef::new(&resp);
    assert!(
        !r.valid() || r.ret_code() != FBKvRetCode::Success,
        "server must reject wrong-type Accept frame, got valid={} ret_code={:?}",
        r.valid(),
        r.ret_code()
    );

    // 2. Missing required field: build a valid `FBAcceptRequest` with
    //    `value = None`. The handler parses it successfully but rejects
    //    it at the `fb_req.value()` guard with `InvalidArgument`.
    let mut builder = FlatBufferBuilder::new();
    let args = FBAcceptRequestArgs {
        id: 2,
        rpc_create_nano: 0,
        version: 1,
        slot: 1,
        round: 1,
        leader_id: 0,
        term: 0,
        value: None,
        client_id: 0,
        seq: 0,
        group_id: 1,
        membership_epoch: 0,
        dedup_tags: None,
    };
    let req = FBAcceptRequest::create(&mut builder, &args);
    builder.finish(req, None);
    let missing_value_bytes = builder.finished_data().to_vec();

    let resp = client
        .send_raw_accept(missing_value_bytes)
        .await
        .expect("raw accept RPC should complete (server must not panic)");

    let r = FBAcceptedResponseRef::new(&resp);
    assert!(
        !r.valid() || r.ret_code() != FBKvRetCode::Success,
        "server must reject missing-value Accept frame, got valid={} ret_code={:?}",
        r.valid(),
        r.ret_code()
    );

    cluster.shutdown().await;
}
