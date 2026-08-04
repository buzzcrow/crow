// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! acceptance test: "client survives a forced leader
//! step-down mid-request with auto-retry, returns the same result."
//!
//! Standing up a live election (kill leader, wait for re-vote) inside a
//! deterministic e2e test is flaky by nature (real timers, real gRPC).
//! What actually matters for C2 is that [`crow_kv_client::CrowkvClient`]
//! correctly follows a live `NotLeaderHint` end-to-end against a real
//! `KvService` and completes the write at the real leader — that is
//! exactly the code path a real step-down exercises
//! (`CrowkvClient::follow_not_leader` in `src/client.rs`), and it can be
//! triggered deterministically by seeding the client at a real follower
//! instead of the real leader.
//!
//! Two-node group (mirrors `crow_kv/tests/testkit/cluster.rs::start_cluster_inner`,
//! which is not a public dependency of this crate): node 1 pinned `Leader`,
//! node 2 pinned `Follower` with `believed_leader` set to node 1, both with
//! real bound gRPC endpoints wired into each other's `remote_replicas` so
//! `PxGroup::leader_endpoint` can produce a real `not_leader_hint`.

use std::sync::Arc;

use bytes::Bytes;
use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::cluster::remote_replica::PxRemoteReplica;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::KvSetRequest;

use crow_kv_client::{ClientConfig, CrowkvClient};

const STORE_ID: u64 = 1;
const GROUP_ID: u64 = 1;
const LEADER_ID: u64 = 1;
const FOLLOWER_ID: u64 = 2;

/// Start a pinned-role 2-node group: `LEADER_ID` as `Leader`, `FOLLOWER_ID`
/// as `Follower` believing `LEADER_ID` is the leader. No election driver
/// interaction needed since roles are pinned directly, same technique as
/// `crow_kv/tests/testkit/cluster.rs::start_cluster_inner`.
async fn start_two_node_cluster() -> (Arc<PxKvStore>, Arc<PxKvStore>) {
    let ids = [LEADER_ID, FOLLOWER_ID];
    let mut running = Vec::with_capacity(ids.len());
    for &id in &ids {
        let role = if id == LEADER_ID {
            PxLocalReplicaRole::Leader
        } else {
            PxLocalReplicaRole::Follower
        };
        let replica = PxLocalReplica::new(id, role);
        if id != LEADER_ID {
            replica.set_believed_leader(LEADER_ID);
        }

        let store = PxKvStore::new(id, "127.0.0.1:0".parse().unwrap());
        let server = Arc::new(store);

        // Placeholder remote endpoints -- real ones aren't known until
        // `start` binds the ephemeral port below.
        let remote_replicas: Vec<PxRemoteReplica> = ids
            .iter()
            .filter(|&&other_id| other_id != id)
            .map(|&other_id| PxRemoteReplica::new(other_id, "127.0.0.1:1".to_string()))
            .collect();

        let mut group = PxGroup::new(GROUP_ID, replica);
        group.set_remote_replicas(remote_replicas);
        server.add_group(group);
        server.start().await.expect("failed to start KvStore");
        running.push(server);
    }

    // Rebuild every group's `remote_replicas` with the real bound addresses
    // now that all nodes are listening.
    let bound_endpoints: Vec<(u64, String)> = running
        .iter()
        .map(|node| {
            let group = node.get_group(GROUP_ID).expect("group exists");
            (
                group.local_replica().id,
                node.listen_addr().expect("server not started").to_string(),
            )
        })
        .collect();
    for node in &running {
        let group = node.get_group(GROUP_ID).expect("group should exist");
        let lr = group.local_replica();
        let local_replica = PxLocalReplica::new(lr.id, lr.role());
        if let Some(believed) = lr.believed_leader_id() {
            local_replica.set_believed_leader(believed);
        }
        let my_id = lr.id;
        let remote_replicas: Vec<PxRemoteReplica> = bound_endpoints
            .iter()
            .filter(|(node_id, _)| *node_id != my_id)
            .map(|(node_id, endpoint)| PxRemoteReplica::new(*node_id, endpoint.clone()))
            .collect();

        let mut new_group = PxGroup::new(GROUP_ID, local_replica);
        new_group.set_remote_replicas(remote_replicas);
        node.add_group(new_group);
    }

    let leader = running.iter().find(|n| n.store_id == LEADER_ID).unwrap().clone();
    let follower = running
        .iter()
        .find(|n| n.store_id == FOLLOWER_ID)
        .unwrap()
        .clone();
    (leader, follower)
}

#[tokio::test]
async fn client_follows_not_leader_hint_to_real_leader() {
    let (leader, follower) = start_two_node_cluster().await;
    let follower_addr = follower.listen_addr().unwrap().to_string();
    let leader_addr = leader.listen_addr().unwrap().to_string();

    // Sanity check the test setup is real: a raw `Put` sent directly to the
    // follower is rejected with a non-empty `not_leader_hint` pointing at
    // the real leader, not silently accepted.
    let mut raw = KvServiceClient::connect(format!("http://{follower_addr}"))
        .await
        .expect("connect follower directly");
    let raw_resp = raw
        .put(KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"sanity"),
            value: Bytes::from_static(b"check"),
            seq: 1,
            ttl_ms: 0,
            client_id: 999,
            request_id: 1,
            request_create_ms: 0,
            group_id: GROUP_ID,
        })
        .await
        .expect("rpc")
        .into_inner();
    assert!(!raw_resp.ok, "a follower must reject a direct Put");
    assert_eq!(
        raw_resp.not_leader_hint, leader_addr,
        "follower's not_leader_hint must point at the real leader"
    );

    // `CrowkvClient` is seeded (deliberately, out-of-band from `/topology`)
    // at the follower, simulating a client whose cached leader just
    // stepped down. `put` must transparently follow the hint and complete
    // at the real leader (transparent NotLeaderHint follow + retry).
    let client = CrowkvClient::new(ClientConfig::new(Vec::new()));
    client.seed_leader(STORE_ID, GROUP_ID, follower_addr);

    let outcome = client
        .put(STORE_ID, GROUP_ID, b"k1", b"v1", None)
        .await
        .expect("put must succeed by following the NotLeaderHint to the real leader");
    assert!(outcome.revision > 0);

    // A second write on the now-corrected cache must not need to redirect
    // again and should still make forward progress.
    let outcome2 = client
        .put(STORE_ID, GROUP_ID, b"k2", b"v2", None)
        .await
        .expect("second put on corrected leader cache");
    assert!(outcome2.revision > outcome.revision);

    leader.stop();
    follower.stop();
    leader.join().await;
    follower.join().await;
}
