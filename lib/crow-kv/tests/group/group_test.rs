// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::replica::Replica;
use crow_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use std::net::SocketAddr;

fn sample_group() -> PxGroup {
    let remote_replicas = vec![PxRemoteReplica::new(2, "127.0.0.1:2".to_string())];
    let local_replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    let mut group = PxGroup::new(1, local_replica);
    group.set_remote_replicas(remote_replicas);
    group
}

#[test]
fn endpoint_update_and_lookup() {
    let mut group = sample_group();
    // leader_endpoint() returns None since local is leader
    assert_eq!(group.leader_endpoint().as_deref(), None);
    assert_eq!(group.member_endpoint(2), Some("127.0.0.1:2"));

    group.update_member_endpoint(2, "127.0.0.1:22");
    assert_eq!(group.member_endpoint(2), Some("127.0.0.1:22"));
}

#[test]
fn group_adds_remote_replicas_for_all_non_local_members() {
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let remote_replicas = vec![PxRemoteReplica::new(2, "127.0.0.1:2".to_string())];
    let mut group = PxGroup::new(1, replica);
    group.set_remote_replicas(remote_replicas);
    store.add_group(group);

    let group = store.get_group(1).expect("group should be registered");
    assert!(group.local_replica().id == 1);
    // With Vec-based indexing, node ID 2 creates a vector of length 3 (indices 0, 1, 2)
    assert_eq!(group.remote_replica_count(), 3);
    let remote = group.get_remote_replica(2).expect("remote replica exists");
    assert_eq!(remote.endpoint(), Some("127.0.0.1:2"));
}

#[test]
fn group_remote_replica_scale_shape_supports_large_membership() {
    let remote_replicas: Vec<_> = (1..99)
        .map(|id| PxRemoteReplica::new(id, format!("127.0.0.1:{}", 10_000 + id)))
        .collect();
    let local_replica = PxLocalReplica::new(0, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(99, local_replica);
    group.set_remote_replicas(remote_replicas);
    let store = PxKvStore::new(0, SocketAddr::from(([127, 0, 0, 1], 0)));
    store.add_group(group);

    let group = store.get_group(99).expect("group should be registered");
    // With Vec-based indexing, node IDs 1-98 create a vector of length 99 (indices 0-98)
    assert_eq!(group.remote_replica_count(), 99);
    assert!(group.get_remote_replica(98).is_some());
}

#[test]
fn group_local_replica_is_leader_reflects_role() {
    let group = sample_group(); // local replica id=1, role=Leader
    assert!(group.local_replica().is_leader());

    let remote_replicas = vec![
        PxRemoteReplica::new(1, "127.0.0.1:1".to_string()),
        PxRemoteReplica::new(2, "127.0.0.1:2".to_string()),
    ];
    let local_replica = PxLocalReplica::new(2, PxLocalReplicaRole::Follower);
    let mut follower_group = PxGroup::new(2, local_replica);
    follower_group.set_remote_replicas(remote_replicas);
    assert!(!follower_group.local_replica().is_leader());
}
