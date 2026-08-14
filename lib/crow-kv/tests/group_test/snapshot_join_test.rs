// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! New-member snapshot join (`PxGroup::join_via_snapshot`):
//! a fresh, still-empty replica pulls
//! a snapshot from an existing cluster's leader over the real
//! `SnapshotService` gRPC, instead of replaying full Paxos history from
//! slot 1.

use crate::common::cluster::start_cluster;
use crate::test_util::compare_dyn;
use bytes::Bytes;
use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::{KvServer, PxLocalReplica, PxLocalReplicaRole};
use crow_kv::rpc::KvSetRequest;

#[tokio::test]
async fn fresh_replica_joins_via_snapshot_and_matches_leader_state() {
    let cluster = start_cluster(&[0, 1], 0).await;
    let leader = cluster.leader();
    let mut client = cluster.kv_client(leader).await;

    for (i, (k, v)) in [
        (&b"a"[..], &b"1"[..]),
        (&b"b"[..], &b"2"[..]),
        (&b"c"[..], &b"3"[..]),
    ]
    .into_iter()
    .enumerate()
    {
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::copy_from_slice(k),
                value: Bytes::copy_from_slice(v),
                ttl_ms: 0,
                request_id: 200 + i as u64,
                request_create_ms: 0,
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("kv put")
            .into_inner();
        assert!(resp.ok);
    }

    let leader_endpoint = leader.listen_addr().expect("leader started").to_string();
    let leader_group = leader.get_group(1).expect("leader group exists");

    // A fresh, standalone replica -- deliberately never wired into the
    // cluster's topology (matches `join_via_snapshot`'s precondition: no
    // peer can send it Accept/Heartbeat RPCs while it's joining).
    let new_replica = PxLocalReplica::new(99, PxLocalReplicaRole::Follower);
    let new_group = PxGroup::new(1, new_replica);

    let at_slot = new_group
        .join_via_snapshot(&leader_endpoint)
        .await
        .expect("snapshot join should succeed");
    assert!(at_slot > 0);

    // The new replica's engine must now match the leader's exactly.
    let diff = compare_dyn(
        new_group.local_replica().learner.engine(),
        leader_group.local_replica().learner.engine(),
    );
    assert!(
        diff.is_empty(),
        "post-join state should match leader exactly: {diff:?}"
    );
    assert_eq!(
        new_group
            .local_replica()
            .learner
            .engine_get(b"a")
            .await
            .map(|(_, v)| v),
        Some(b"1".to_vec())
    );
    assert_eq!(
        new_group
            .local_replica()
            .learner
            .engine_get(b"c")
            .await
            .map(|(_, v)| v),
        Some(b"3".to_vec())
    );

    // Frontier seeded to the leader's contiguous-applied at export time:
    // the new replica can skip straight to normal repair for anything
    // beyond it, no full replay needed.
    assert_eq!(
        new_group.local_replica().contiguous_applied(),
        leader_group.local_replica().contiguous_applied(),
    );
    assert!(new_group.local_replica().contiguous_applied() > 0);

    cluster.shutdown().await;
}
