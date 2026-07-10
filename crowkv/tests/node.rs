use crowkv::group::group::{PxGroup, PxGroupConfig, PxGroupMember};
use crowkv::node::server::NodeServer;
use crowkv::node::{PxNode, PxNodeRole, PxPaxosMode};
use crowkv::paxos::roles::{Ballot as PxBallot, LogEntry, LogEntryKind};
use crowkv::rpc::KvBatchItem;

fn sample_group(my_id: u64, leader_id: u64, leader_endpoint: &str) -> PxGroup {
    PxGroup::new(
        PxGroupConfig {
            group_id: 1,
            members: vec![PxGroupMember {
                node_id: leader_id,
                endpoint: leader_endpoint.to_string(),
                voting: true,
            }],
            quorum_size: 1,
            config_version: 1,
        },
        leader_id,
        my_id,
    )
}

#[tokio::test]
async fn kv_ops_apply_locally_for_single_leader() {
    let node = PxNode::new(1, PxNodeRole::Leader, PxPaxosMode::Leader);

    let resp = node
        .kv_put(b"k1".to_vec(), b"v1".to_vec(), 11, 1, 101, 1001)
        .await;
    assert!(resp.ok, "leader should accept put");
    assert_eq!(
        node.learner.store().get("k1".as_bytes()).map(|v| v.clone()),
        Some(b"v1".to_vec())
    );

    let batch_resp = node
        .kv_batch_write(
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
    assert_eq!(
        node.learner.store().get("k1".as_bytes()).map(|v| v.clone()),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        node.learner.store().get("k2".as_bytes()).map(|v| v.clone()),
        Some(b"v2".to_vec())
    );

    let delete_resp = node.kv_delete(b"k1".to_vec(), 11, 3, 103, 1003).await;
    assert!(delete_resp.ok);
    assert!(node.learner.store().get("k1".as_bytes()).is_none());
}

#[tokio::test]
async fn follower_redirects_with_leader_hint() {
    let mut node = PxNode::new(7, PxNodeRole::Follower, PxPaxosMode::Leader);
    node.with_group(sample_group(7, 42, "127.0.0.1:4444"));

    let resp = node
        .kv_put(b"k".to_vec(), b"v".to_vec(), 12, 1, 201, 2001)
        .await;
    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, "127.0.0.1:4444");
}

#[tokio::test]
async fn classic_prepare_and_accept_track_state() {
    let node = PxNode::new(9, PxNodeRole::Leader, PxPaxosMode::Classic);
    let ballot = PxBallot::new(1, node.id);

    let prepare_reply = node.on_prepare(5, ballot).await;
    assert!(matches!(
        prepare_reply,
        crowkv::paxos::roles::PrepareReply::Promised { .. }
    ));

    let entry = LogEntry {
        slot: 5,
        ballot,
        term: 0,
        kind: LogEntryKind::Write,
        payload: b"payload".to_vec(),
        client_id: None,
        seq: None,
    };

    let accept_reply = node.on_accept(entry.clone()).await;
    assert!(matches!(
        accept_reply,
        crowkv::paxos::roles::AcceptReply::Accepted { .. }
    ));

    let accepted = node.accepted_at(5).await.expect("accepted entry present");
    assert_eq!(accepted.payload, entry.payload);
    let promised = node.promised_at(5).await.expect("promised ballot present");
    assert_eq!(promised.round, ballot.round);
    assert!(node.is_leader());
}

#[tokio::test]
async fn role_can_be_changed_for_tests() {
    let mut node = PxNode::new(21, PxNodeRole::Follower, PxPaxosMode::Leader);
    assert!(!node.is_leader());

    node.set_role(PxNodeRole::Leader);
    assert!(node.is_leader());
}

#[tokio::test]
async fn node_server_lifecycle_sets_listen_addr() {
    let node = PxNode::new(11, PxNodeRole::Leader, PxPaxosMode::Leader);
    assert!(node.listen_addr().is_none(), "not started yet");

    assert!(node.start().await, "server should start");
    let addr = node.listen_addr().expect("addr after start");

    node.stop();
    node.join().await;

    // listen_addr remains recorded even after shutdown so tests can assert binding.
    assert_eq!(node.listen_addr(), Some(addr));
}
