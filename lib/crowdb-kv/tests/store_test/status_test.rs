// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology status tests for `PxKvStore::status()`. Covers composition
//! of per-layer statuses, cheap kv-store stats, and per-remote metrics surface.

use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv::cluster::remote_replica::PxRemoteReplica;

#[test]
fn status_empty_store() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let snap = store.status();
    assert_eq!(snap.store_id, 1);
    assert!(snap.groups.is_empty());
}

#[test]
fn status_single_group_no_remotes() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        7,
        PxLocalReplica::new(3, PxLocalReplicaRole::Leader),
    ));
    let snap = store.status();
    assert_eq!(snap.groups.len(), 1);
    let g = &snap.groups[0];
    assert_eq!(g.group_id, 7);
    assert_eq!(g.local_replica.id, 3);
    assert_eq!(g.local_replica.role, "leader");
    assert!(g.local_replica.voting);
    assert_eq!(g.local_replica.kv_store.key_count, 0);
    assert!(g.remotes.is_empty());
}

#[test]
fn status_with_remote_zero_metrics() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:65500".to_string()));
    store.add_group(group);
    let snap = store.status();
    let g = &snap.groups[0];
    assert_eq!(g.remotes.len(), 1);
    let r = &g.remotes[0];
    assert_eq!(r.id, 2);
    assert_eq!(r.endpoint, "127.0.0.1:65500");
    assert_eq!(r.metrics.rpc_count, 0);
    assert_eq!(r.metrics.err_count, 0);
    assert_eq!(r.metrics.last_rtt_ms, 0);
}
