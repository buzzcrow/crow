use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::rpc::KvBatchItem;
use std::net::SocketAddr;
use std::sync::Arc;

fn sample_group(my_id: u64, leader_id: u64, leader_endpoint: &str) -> PxGroup {
    let remote_replicas = if my_id == leader_id {
        vec![]
    } else {
        vec![PxRemoteReplica::new(leader_id, leader_endpoint.to_string())]
    };
    let role = if my_id == leader_id {
        PxLocalReplicaRole::Leader
    } else {
        PxLocalReplicaRole::Follower
    };
    let local_replica = PxLocalReplica::new(my_id, role);

    let mut group = PxGroup::new(1, local_replica);
    group.set_remote_replicas(remote_replicas);
    group
}

#[tokio::test]
async fn kv_ops_apply_locally_for_single_leader() {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    let group = sample_group(1, 1, "127.0.0.1:0");
    store.add_group(group);

    let resp = store.kv_put(1, b"k1", b"v1", 11, 1, 101, 1001).await;
    assert!(resp.ok, "leader should accept put");

    let group = store.get_group(1).unwrap();
    let replica = group.local_replica();
    assert_eq!(
        replica.learner.engine_get("k1".as_bytes()).map(|(_, v)| v),
        Some(b"v1".to_vec())
    );

    let batch_resp = store
        .kv_batch_write(
            1,
            vec![
                KvBatchItem {
                    key: b"k1".to_vec(),
                    value: b"v2".to_vec(),
                    is_delete: false,
                },
                KvBatchItem {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                    is_delete: false,
                },
            ],
            11,
            2,
            102,
            1002,
        )
        .await;
    assert!(batch_resp.ok);

    let group = store.get_group(1).unwrap();
    let replica = group.local_replica();
    assert_eq!(
        replica.learner.engine_get("k1".as_bytes()).map(|(_, v)| v),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        replica.learner.engine_get("k2".as_bytes()).map(|(_, v)| v),
        Some(b"v2".to_vec())
    );

    let delete_resp = store.kv_delete(1, b"k1", 11, 3, 103, 1003).await;
    assert!(delete_resp.ok);

    let group = store.get_group(1).unwrap();
    let replica = group.local_replica();
    assert!(replica.learner.engine_get("k1".as_bytes()).is_none());
}

#[tokio::test]
async fn follower_redirects_with_leader_hint() {
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

    let resp = store.kv_put(1, b"k", b"v", 12, 1, 201, 2001).await;
    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, "127.0.0.1:4444");
}

#[tokio::test]
async fn classic_prepare_and_accept_track_state() {
    let node = PxLocalReplica::new(9, PxLocalReplicaRole::Leader);
    let ballot = PxBallot::new(1, node.id);

    let prepare_reply = node.on_prepare(5, ballot, 0).await;
    assert!(matches!(
        prepare_reply,
        crowkv::paxos::roles::PxPrepareReply::Promised { .. }
    ));

    let entry = PxLogEntry {
        slot: 5,
        ballot,
        term: 0,
        payload: bytes::Bytes::from_static(b"payload"),
    };

    let accept_reply = node.on_accept(entry.clone()).await;
    assert!(matches!(
        accept_reply,
        crowkv::paxos::roles::PxAcceptReply::Accepted { .. }
    ));

    let accepted = node.accepted_at(5).await.expect("accepted entry present");
    assert_eq!(accepted.payload, entry.payload);
    let promised = node.promised_at(5).await.expect("promised ballot present");
    assert_eq!(promised.round, ballot.round);
    assert!(node.is_leader());
}

#[tokio::test]
async fn role_can_be_changed_for_tests() {
    let node = PxLocalReplica::new(21, PxLocalReplicaRole::Follower);
    assert!(!node.is_leader());

    node.become_leader();
    assert!(node.is_leader());
}

#[tokio::test]
async fn read_modes_serve_value_with_slots_on_single_leader() {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    store.add_group(sample_group(1, 1, "127.0.0.1:0"));

    // Commit one write so the leader has an applied frontier at slot 1.
    let put = store.kv_put(1, b"rk", b"rv", 11, 1, 1, 1).await;
    assert!(put.ok, "leader put should commit");
    let revision = put.revision;
    assert!(revision >= 1, "commit should land at slot >= 1");

    // Linearizable (mode 0): single-voter leader runs the ReadIndex barrier
    // (lease starts expired), confirms quorum trivially, and serves locally.
    let lin = store.kv_get(1, b"rk", 0, 0, 2, 2).await;
    assert!(
        lin.ok && lin.value == b"rv",
        "linearizable read should hit: {lin:?}"
    );
    assert!(
        lin.read_slot >= revision,
        "read_slot should be at the committed frontier"
    );

    // ReadYourWrites (mode 1) with the client's own write slot: the applied
    // frontier has caught up, so it is served locally.
    let ryw = store.kv_get(1, b"rk", 1, revision, 3, 3).await;
    assert!(
        ryw.ok && ryw.value == b"rv",
        "read-your-writes should hit: {ryw:?}"
    );

    // ReadYourWrites demanding a future slot the replica has not applied yet
    // is redirected rather than served stale.
    let ryw_future = store.kv_get(1, b"rk", 1, revision + 100, 4, 4).await;
    assert!(
        !ryw_future.ok,
        "RYW past the applied frontier must not serve locally"
    );

    // BoundedStale (mode 2) and BestEffort (mode 3) always serve locally.
    let bounded = store.kv_get(1, b"rk", 2, 0, 5, 5).await;
    assert!(
        bounded.ok && bounded.value == b"rv",
        "bounded-stale read should hit"
    );
    let best = store.kv_get(1, b"rk", 3, 0, 6, 6).await;
    assert!(best.ok && best.value == b"rv", "best-effort read should hit");
}

#[tokio::test]
async fn dedup_suppresses_retried_client_seq() {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    store.add_group(sample_group(1, 1, "127.0.0.1:0"));

    // First write commits at some slot.
    let r1 = store.kv_put(1, b"dk", b"v1", 5, 1, 1, 1).await;
    assert!(r1.ok, "first write should commit");
    let slot1 = r1.revision;

    // Retry the SAME (client_id=5, seq=1) with a different value: dedup must
    // suppress re-execution, returning the original commit slot and leaving
    // the stored value untouched (exactly-once).
    let dup = store.kv_put(1, b"dk", b"v2", 5, 1, 2, 2).await;
    assert!(dup.ok, "duplicate should report ok");
    assert_eq!(dup.revision, slot1, "duplicate returns the original commit slot");
    let group = store.get_group(1).unwrap();
    assert_eq!(
        group.local_replica().learner.engine_get(b"dk").map(|(_, v)| v),
        Some(b"v1".to_vec()),
        "duplicate (client,seq) must not overwrite the committed value"
    );

    // A higher seq is a new request: it advances and applies.
    let r2 = store.kv_put(1, b"dk", b"v3", 5, 2, 3, 3).await;
    assert!(r2.ok);
    assert!(r2.revision > slot1, "higher seq advances to a new slot");
    assert_eq!(
        group.local_replica().learner.engine_get(b"dk").map(|(_, v)| v),
        Some(b"v3".to_vec()),
        "higher seq applies the new value"
    );
}

#[tokio::test]
async fn node_server_lifecycle_sets_listen_addr() {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    let server = Arc::new(store);

    let replica = PxLocalReplica::new(11, PxLocalReplicaRole::Leader);
    let remote_replicas = vec![];
    let mut group = PxGroup::new(1, replica);
    group.set_remote_replicas(remote_replicas);

    server.add_group(group);

    assert!(server.listen_addr().is_none(), "not started yet");

    server.start().await.expect("server should start");
    let addr = server.listen_addr().expect("addr after start");

    server.stop();
    server.join().await;

    assert_eq!(server.listen_addr(), Some(addr));
}
