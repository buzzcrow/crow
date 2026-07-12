//! Transparent leader-forwarding for `KvService::Get` and
//! `KvService::Scan`.
//!
//! These tests pin the contract from `kv_service.rs`:
//!
//! 1. A follower whose local learner store is empty still returns the
//!    leader's committed value when the client calls `Get`/`Scan` —
//!    proving the forward path engaged.
//! 2. The `x-crowkv-forwarded: 1` metadata header disables the
//!    forwarder on the receiving node, so a misrouted request can hop
//!    at most once and never enters an infinite loop.
//!
//! The trick we use to make "the forward fired" observable is to run a
//! `put` on the leader, wait for the standard Paxos learn to propagate
//! to the follower (so the cluster-wide invariant is preserved), then
//! manually clear the follower's `learner.engine()`. After the clear, a
//! local read on the follower must return `not_found = true`. If the
//! follower's `Get` instead returns the value, the forward has run.

use crate::testkit::cluster::start_cluster;
use crowkv::cluster::KvServer;
use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvGetRequest, KvScanRequest, KvSetRequest};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

const FORWARD_HEADER: &str = "x-crowkv-forwarded";

fn with_forward_header<T>(mut req: Request<T>) -> Request<T> {
    let v: MetadataValue<_> = "1".parse().expect("static metadata value");
    req.metadata_mut().insert(FORWARD_HEADER, v);
    req
}

#[tokio::test]
async fn follower_get_forwards_to_leader_after_local_clear() {
    let cluster = start_cluster(&[0, 1], 0).await;
    let leader = cluster.leader();
    let follower = cluster
        .followers()
        .into_iter()
        .next()
        .expect("at least one follower");

    // 1. Write through the leader.
    let mut leader_client = cluster.kv_client(leader).await;
    let put = leader_client
        .put(KvSetRequest {
            version: 1,
            key: b"fk".to_vec(),
            value: b"fv".to_vec(),
            ttl_ms: 0,
            request_id: 101,
            request_create_ms: 1001,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(put.ok, "leader put should succeed");

    // 2. Confirm Paxos has propagated the value to the follower.
    let follower_group = follower.get_group(1).expect("group 1 on follower");
    assert!(
        follower_group
            .local_replica()
            .learner
            .engine_get(b"fk".as_slice())
            .is_some(),
        "expected paxos to propagate value to follower's learner"
    );

    // 3. Clear the follower's local learner store. After this, a
    //    local-only read on the follower would return not_found.
    follower_group.local_replica().learner.engine().clear();
    assert_eq!(follower_group.local_replica().learner.live_key_count(), 0);

    // 4. Read via the follower's RPC. The transparent forward should
    //    return the leader's value, not not_found.
    let mut follower_client = cluster.kv_client(follower).await;
    let resp = follower_client
        .get(KvGetRequest {
            version: 1,
            key: b"fk".to_vec(),
            request_id: 102,
            request_create_ms: 1002,
            group_id: 1,
            read_mode: 0,
            client_slot: 0,
        })
        .await
        .expect("kv get on follower")
        .into_inner();

    assert!(
        resp.ok,
        "follower forward should produce ok response, got {resp:?}"
    );
    assert!(!resp.not_found, "follower forward should not surface not_found");
    assert_eq!(
        resp.value,
        b"fv".to_vec(),
        "follower must return the leader's value via forward"
    );

    drop(leader_client);
    drop(follower_client);
    cluster.shutdown().await;
}

#[tokio::test]
async fn follower_scan_forwards_to_leader_after_local_clear() {
    let cluster = start_cluster(&[0, 1], 0).await;
    let leader = cluster.leader();
    let follower = cluster.followers().into_iter().next().expect("follower");

    let mut leader_client = cluster.kv_client(leader).await;
    for (i, (k, v)) in [
        (b"a1".to_vec(), b"v1".to_vec()),
        (b"a2".to_vec(), b"v2".to_vec()),
        (b"b1".to_vec(), b"v3".to_vec()),
    ]
    .into_iter()
    .enumerate()
    {
        // Distinct seq per write: these are three separate requests, not
        // retries, so the dedup cache must not collapse them.
        let seq = (i as u64) + 1;
        let _ = leader_client
            .put(KvSetRequest {
                version: 1,
                key: k,
                value: v,
                seq,
                ttl_ms: 0,
                client_id: 11,
                request_id: 200 + seq,
                request_create_ms: 1100,
                group_id: 1,
            })
            .await
            .expect("kv put");
    }

    // Wait for propagation, then clear follower's store.
    let follower_group = follower.get_group(1).expect("group");
    follower_group.local_replica().learner.engine().clear();

    let mut follower_client = cluster.kv_client(follower).await;
    let resp = follower_client
        .scan(KvScanRequest {
            version: 1,
            group_id: 1,
            prefix: b"a".to_vec(),
            limit: 0,
            request_id: 201,
            request_create_ms: 1101,
            read_mode: 0,
        })
        .await
        .expect("kv scan on follower")
        .into_inner();

    assert!(resp.ok, "follower scan forward should succeed");
    assert!(!resp.truncated);
    let keys: Vec<Vec<u8>> = resp.items.iter().map(|i| i.key.clone()).collect();
    assert_eq!(
        keys,
        vec![b"a1".to_vec(), b"a2".to_vec()],
        "scan forward must return leader's prefix-matching keys in sorted order"
    );

    drop(leader_client);
    drop(follower_client);
    cluster.shutdown().await;
}

#[tokio::test]
async fn forwarded_request_does_not_re_forward() {
    // A request that already carries the loop-guard header must NOT be
    // re-forwarded, guaranteeing forwarding hops are bounded to one.
    // Because the receiving node is a follower, a *linearizable* read
    // cannot be proven fresh locally, so the follower returns a
    // not-leader redirect rather than serving stale local state. A
    // re-forward (loop-guard failure) would instead have returned the
    // leader's value, so the redirect is what proves the guard worked.
    let cluster = start_cluster(&[0, 1], 0).await;
    let leader = cluster.leader();
    let follower = cluster.followers().into_iter().next().expect("follower");

    // Seed a value through the leader, then clear the follower's local
    // store so that a local-only read would return not_found.
    let mut leader_client = cluster.kv_client(leader).await;
    let _ = leader_client
        .put(KvSetRequest {
            version: 1,
            key: b"loop".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            request_id: 300,
            request_create_ms: 1200,
            client_id: 0,
            seq: 0,
            group_id: 1,
        })
        .await
        .expect("kv put");
    follower
        .get_group(1)
        .expect("group")
        .local_replica()
        .learner
        .engine()
        .clear();

    // Send Get directly to the follower with the forward header set.
    // The follower must NOT re-forward; it returns a not-leader redirect
    // for the linearizable read instead of serving its (cleared) store.
    let follower_addr = follower.listen_addr().expect("follower listening");
    let channel = Channel::from_shared(format!("http://{follower_addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect follower");
    let mut client: KvServiceClient<Channel> = KvServiceClient::new(channel);

    let req = with_forward_header(Request::new(KvGetRequest {
        version: 1,
        key: b"loop".to_vec(),
        request_id: 301,
        request_create_ms: 1201,
        group_id: 1,
        read_mode: 0,
        client_slot: 0,
    }));
    let resp = client
        .get(req)
        .await
        .expect("kv get with forward header")
        .into_inner();

    assert!(
        !resp.ok,
        "forwarded linearizable read on a follower must not be served locally, got {resp:?}"
    );
    assert!(
        !resp.not_found,
        "linearizable read must redirect (not_leader), not fabricate not_found from the cleared local store: {resp:?}"
    );
    assert!(
        !resp.not_leader_hint.is_empty(),
        "redirect should carry the leader hint so the client can retry: {resp:?}"
    );

    drop(leader_client);
    drop(client);
    cluster.shutdown().await;
}
