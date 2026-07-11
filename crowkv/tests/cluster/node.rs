use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::rpc::KvBatchItem;
use std::net::SocketAddr;
use std::sync::Arc;

fn sample_group(my_id: u64, leader_id: u64, leader_endpoint: &str) -> PxGroup {
    let remote_replicas = if my_id == leader_id {
        vec![]
    } else {
        vec![PxRemoteReplica::new(leader_id, leader_endpoint.to_string())]
    };
    let role = if my_id == leader_id { PxLocalReplicaRole::Leader } else { PxLocalReplicaRole::Follower };
    let local_replica = PxLocalReplica::new(my_id, role);

    let mut group = PxGroup::new(1, local_replica);
    group.set_remote_replicas(remote_replicas);
    group.set_leader_id(leader_id);
    group
}

#[tokio::test]
async fn kv_ops_apply_locally_for_single_leader() {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    let group = sample_group(1, 1, "127.0.0.1:0");
    store.add_group(group);

    let resp = store.kv_put(1, b"k1".to_vec(), b"v1".to_vec(), 11, 1, 101, 1001).await;
    assert!(resp.ok, "leader should accept put");

    let group = store.get_group(1).unwrap();
    let replica = group.local_replica();
    assert_eq!(replica.learner.store().get("k1".as_bytes()).map(|v| v.clone()), Some(b"v1".to_vec()));

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
    assert_eq!(replica.learner.store().get("k1".as_bytes()).map(|v| v.clone()), Some(b"v2".to_vec()));
    assert_eq!(replica.learner.store().get("k2".as_bytes()).map(|v| v.clone()), Some(b"v2".to_vec()));

    let delete_resp = store.kv_delete(1, b"k1".to_vec(), 11, 3, 103, 1003).await;
    assert!(delete_resp.ok);

    let group = store.get_group(1).unwrap();
    let replica = group.local_replica();
    assert!(replica.learner.store().get("k1".as_bytes()).is_none());
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
    group.set_leader_id(42);
    store.add_group(group);

    let resp = store.kv_put(1, b"k".to_vec(), b"v".to_vec(), 12, 1, 201, 2001).await;
    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, "127.0.0.1:4444");
}

#[tokio::test]
async fn classic_prepare_and_accept_track_state() {
    let node = PxLocalReplica::new(9, PxLocalReplicaRole::Leader);
    let ballot = PxBallot::new(1, node.id);

    let prepare_reply = node.on_prepare(5, ballot).await;
    assert!(matches!(prepare_reply, crowkv::paxos::roles::PxPrepareReply::Promised { .. }));

    let entry = PxLogEntry {
        slot: 5,
        ballot,
        term: 0,
        kind: PxLogEntryKind::Write,
        payload: Arc::new(b"payload".to_vec()),
        client_id: None,
        seq: None,
    };

    let accept_reply = node.on_accept(entry.clone()).await;
    assert!(matches!(accept_reply, crowkv::paxos::roles::PxAcceptReply::Accepted { .. }));

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
async fn node_server_lifecycle_sets_listen_addr() {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    let server = Arc::new(store);

    let replica = PxLocalReplica::new(11, PxLocalReplicaRole::Leader);
    let remote_replicas = vec![];
    let mut group = PxGroup::new(1, replica);
    group.set_remote_replicas(remote_replicas);
    group.set_leader_id(11);

    server.add_group(group);

    assert!(server.listen_addr().is_none(), "not started yet");

    assert!(server.start().await, "server should start");
    let addr = server.listen_addr().expect("addr after start");

    server.stop();
    server.join().await;

    assert_eq!(server.listen_addr(), Some(addr));
}
