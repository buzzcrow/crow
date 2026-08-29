// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R32 integration tests: exercise the KV consensus hot path over
//! crowdb-rpc (flatbuffer transport) instead of the legacy transport.

mod common;

use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::replica::ReplicaClient;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_kv::paxos::roles::{PxBallot, PxPrepareReply};

use common::logging::init_test_subscriber;
use common::net_lock::lock;

/// Cluster handle — holds the net lock until dropped.
struct CrowdbRpcCluster {
    leader: Arc<PxKvStore>,
    follower: Arc<PxKvStore>,
    _net: tokio::sync::MutexGuard<'static, ()>,
}

/// Start a 2-node cluster where consensus RPCs flow over crowdb-rpc.
async fn start_crow_rpc_cluster() -> CrowdbRpcCluster {
    init_test_subscriber();
    let net = lock().await;

    let leader_id: u64 = 1;
    let follower_id: u64 = 2;

    let leader_store = Arc::new(PxKvStore::new(leader_id, "127.0.0.1:0".parse().unwrap()));
    let follower_store = Arc::new(PxKvStore::new(follower_id, "127.0.0.1:0".parse().unwrap()));

    leader_store.start().await.expect("leader start");
    follower_store.start().await.expect("follower start");

    let leader_transport = leader_store.rpc_transport().expect("leader transport");
    let follower_transport = follower_store.rpc_transport().expect("follower transport");

    let leader_endpoint = leader_store
        .listen_addr()
        .expect("leader listen_addr")
        .to_string();
    let follower_endpoint = follower_store
        .listen_addr()
        .expect("follower listen_addr")
        .to_string();

    // Leader group: local replica is Leader, remote is follower.
    let leader_replica = PxLocalReplica::new(leader_id, PxLocalReplicaRole::Leader);
    let follower_remote =
        PxRemoteReplica::new(follower_id, follower_endpoint).with_rpc_transport(leader_transport);
    let mut leader_group = PxGroup::new(1, leader_replica);
    leader_group.set_remote_replicas(vec![follower_remote]);
    leader_store.add_group(leader_group);

    // Follower group: local replica is Follower, remote is leader.
    let follower_replica = PxLocalReplica::new(follower_id, PxLocalReplicaRole::Follower);
    follower_replica.set_believed_leader(leader_id);
    let leader_remote =
        PxRemoteReplica::new(leader_id, leader_endpoint).with_rpc_transport(follower_transport);
    let mut follower_group = PxGroup::new(1, follower_replica);
    follower_group.set_remote_replicas(vec![leader_remote]);
    follower_store.add_group(follower_group);

    CrowdbRpcCluster {
        leader: leader_store,
        follower: follower_store,
        _net: net,
    }
}

impl CrowdbRpcCluster {
    async fn shutdown(self) {
        self.leader.stop();
        self.follower.stop();
        self.leader.join().await;
        self.follower.join().await;
    }
}

#[tokio::test]
async fn crowdb_rpc_prepare_accept_roundtrip() {
    let cluster = start_crow_rpc_cluster().await;

    // Get the leader's remote replica (points at follower).
    let leader_group = cluster.leader.get_group(1).expect("leader group");
    let follower_remote = leader_group.get_remote_replica(2).expect("follower remote");

    // Send a Prepare for slot 1, ballot (1, leader_id=1).
    let ballot = PxBallot::new(1, 1);
    let prepare_result = follower_remote.send_prepare(1, ballot, 0, 1, 0).await;
    assert!(
        prepare_result.is_ok(),
        "prepare should succeed over crowdb-rpc: {:?}",
        prepare_result.err()
    );
    match prepare_result.unwrap() {
        PxPrepareReply::Promised { slot, .. } => {
            assert_eq!(slot, 1, "promised slot should be 1");
        }
        other => panic!("expected Promised, got {other:?}"),
    }

    // Send an Accept for slot 1.
    let entry = crowdb_kv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot,
        term: 0,
        payload: bytes::Bytes::from_static(b"hello-crowdb-rpc"),
    };
    let accept_result = follower_remote.send_accept(&entry, &[], 1, 0).await;
    assert!(
        accept_result.is_ok(),
        "accept should succeed over crowdb-rpc: {:?}",
        accept_result.err()
    );
    match accept_result.unwrap() {
        crowdb_kv::paxos::roles::PxAcceptReply::Accepted { slot, .. } => {
            assert_eq!(slot, 1, "accepted slot should be 1");
        }
        other => panic!("expected Accepted, got {other:?}"),
    }

    // Verify the follower actually stored the value.
    let follower_group = cluster.follower.get_group(1).expect("follower group");
    let follower_replica = follower_group.local_replica();
    let accepted = follower_replica.accepted_at(1).await;
    assert!(accepted.is_some(), "follower should have accepted slot 1");
    let accepted = accepted.unwrap();
    assert_eq!(
        accepted.payload.as_ref(),
        b"hello-crowdb-rpc",
        "follower should have the correct payload"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn crowdb_rpc_chosen_notification_fire_and_forget() {
    let cluster = start_crow_rpc_cluster().await;

    let leader_group = cluster.leader.get_group(1).expect("leader group");
    let follower_remote = leader_group.get_remote_replica(2).expect("follower remote");

    // First, Prepare + Accept slot 1 so the follower has a value.
    let ballot = PxBallot::new(1, 1);
    let entry = crowdb_kv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot,
        term: 0,
        payload: bytes::Bytes::from_static(b"chosen-test"),
    };
    follower_remote
        .send_prepare(1, ballot, 0, 1, 0)
        .await
        .expect("prepare");
    follower_remote
        .send_accept(&entry, &[], 1, 0)
        .await
        .expect("accept");

    // Send a fire-and-forget ChosenNotification.
    let result = follower_remote.send_chosen_notice(1, 0, 1, 1, ballot.round);
    assert!(
        result.is_ok(),
        "chosen notification should succeed: {:?}",
        result.err()
    );

    // Poll until the follower has processed the chosen notification
    // and recorded slot 1 as accepted.
    let follower_group = cluster.follower.get_group(1).expect("follower group");
    let follower_replica = follower_group.local_replica();
    let poll_start = std::time::Instant::now();
    let accepted = loop {
        if let Some(a) = follower_replica.accepted_at(1).await {
            break Some(a);
        }
        if poll_start.elapsed() >= std::time::Duration::from_secs(5) {
            break None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert!(
        accepted.is_some(),
        "follower should have accepted slot 1 after chosen notification"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn crowdb_rpc_fetch_gap() {
    let cluster = start_crow_rpc_cluster().await;

    // Prepare + Accept slot 1 on both leader (locally) and follower (via RPC).
    let ballot = PxBallot::new(1, 1);
    let entry = crowdb_kv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot,
        term: 0,
        payload: bytes::Bytes::from_static(b"gap-test"),
    };

    // Leader accepts locally.
    let leader_group = cluster.leader.get_group(1).expect("leader group");
    let leader_replica = leader_group.local_replica();
    let prepare_reply = leader_replica.on_prepare(1, ballot, 0).await;
    assert!(
        matches!(prepare_reply, PxPrepareReply::Promised { .. }),
        "leader prepare should succeed: {prepare_reply:?}"
    );
    let accept_reply = leader_replica.on_accept(&entry).await;
    assert!(
        matches!(
            accept_reply,
            crowdb_kv::paxos::roles::PxAcceptReply::Accepted { .. }
        ),
        "leader accept should succeed: {accept_reply:?}"
    );

    // Follower accepts via crowdb-rpc.
    let follower_remote = leader_group.get_remote_replica(2).expect("follower remote");
    follower_remote
        .send_prepare(1, ballot, 0, 1, 0)
        .await
        .expect("follower prepare");
    follower_remote
        .send_accept(&entry, &[], 1, 0)
        .await
        .expect("follower accept");

    // Use the follower's remote (points at leader) to fetch gap.
    let follower_group = cluster.follower.get_group(1).expect("follower group");
    let leader_remote = follower_group.get_remote_replica(1).expect("leader remote");

    // FetchGap from the leader for slot 1.
    let result = leader_remote.send_fetch_gap(1, 0, 1, 1).await;
    assert!(result.is_ok(), "fetch_gap should succeed: {:?}", result.err());
    let reply = result.unwrap();
    assert_eq!(reply.slot, 1, "fetch_gap reply slot");
    assert_eq!(reply.payload.as_ref(), b"gap-test", "fetch_gap reply payload");

    cluster.shutdown().await;
}
