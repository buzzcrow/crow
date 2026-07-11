use crate::testkit::cluster::start_cluster_classic;
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::rpc::KvSetRequest;

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(1);
    payload.push(0);
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

#[tokio::test]
async fn kv_put_retries_next_slot_when_slot_has_prior_accepted_value() {
    let cluster = start_cluster_classic(&[0, 1, 2], 0).await;

    let stale_payload = encode_put_payload(b"stale", b"value");
    let followers = cluster.followers();
    let follower = followers.first().expect("follower present");
    // Step 10.7: unary `Accept` is retired. Seed the follower's slot 1
    // by driving the local acceptor directly (the on_accept path the
    // gRPC handler used to wrap). This keeps the test focused on the
    // proposer's slot-retry behaviour rather than RPC plumbing.
    let follower_group = follower.get_group(1).expect("group exists on follower");
    let follower_replica = follower_group.local_replica();
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(10, 99),
        term: 0,
        kind: PxLogEntryKind::Write,
        payload: bytes::Bytes::from(stale_payload.clone()),
        client_id: None,
        seq: None,
    };
    let reply = follower_replica.on_accept(entry).await;
    assert!(
        matches!(reply, crowkv::paxos::roles::PxAcceptReply::Accepted { .. }),
        "preload accept should succeed: {reply:?}"
    );

    let leader = cluster.leader();
    let mut kv = cluster.kv_client(leader).await;
    let put_resp = kv
        .put(KvSetRequest {
            version: 1,
            key: b"my-key".to_vec(),
            value: b"my-value".to_vec(),
            seq: 1,
            ttl_ms: 0,
            client_id: 12,
            request_id: 201,
            request_create_ms: 2001,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(put_resp.ok, "put should succeed after slot retry");
    assert!(
        put_resp.revision >= 2,
        "client value should be retried on a later slot"
    );

    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let slot1 = replica.accepted_at(1).await.expect("slot 1 accepted");
        assert_eq!(
            *slot1.payload, stale_payload,
            "slot 1 must preserve pre-existing accepted value"
        );
    }
    for node in cluster.nodes() {
        let group = node.get_group(1).expect("group exists");
        let replica = group.local_replica();
        let value = replica.learner.engine_get(b"my-key".as_slice()).map(|(_, v)| v);
        assert_eq!(value.as_deref(), Some(b"my-value".as_slice()));
    }

    drop(kv);
    cluster.shutdown().await;
}
