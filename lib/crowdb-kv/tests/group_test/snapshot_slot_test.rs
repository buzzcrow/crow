// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group durable-snapshot-watermark computation: the
//! published `group_snapshot_slot` is `min(local WalEngine::snapshot_slot,
//! max(voting peer durable_snapshot_slot))` -- "durable on the leader plus
//! at least one peer" (`snapshot_slot`), gossiped piggybacked on the same heartbeat round as
//! `group_safe_slot`. Unlike `group_safe_slot` (which takes the *min* over
//! every voting peer), `group_snapshot_slot` takes the *max* over peers --
//! only one peer beyond the leader needs to durably have a slot, so a
//! straggler peer must not hold this watermark back the way it holds back
//! `group_safe_slot`.
//!
//! Drives the crate-internal peer-durable injection through the
//! `test-util` feature hook on `PxGroup` (mirrors `safe_slot_test.rs`).

use std::path::PathBuf;
use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_kv::wal::wal_engine::WalEngine;
use crowdb_kv::wal::{IoBackend, MemBlockDevice, WalConfig};

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::MemBlock(MemBlockDevice::new()))
}

/// A minimal real `WalEngine`, just so `PxLocalReplica::wal` is `Some` and
/// `WalEngine::snapshot_slot`'s getter/setter (plain atomics) are
/// exercised for real, matching what `group_maintenance::run_pass` writes
/// to in production -- no segments/replay needed for this test.
async fn wal_with_snapshot_slot(replica_id: u64, slot: u64) -> Arc<WalEngine> {
    let backend = sim_backend();
    let config = WalConfig {
        wal_disks: vec![PathBuf::from(format!("/wal{replica_id}"))],
        ..Default::default()
    };
    let wal = WalEngine::create(backend, config, replica_id).await.unwrap();
    wal.set_snapshot_slot(slot);
    wal
}

#[tokio::test]
async fn group_snapshot_slot_is_zero_without_a_local_wal() {
    // No local WAL attached at all: this replica has never durably
    // snapshotted, so the watermark must stay 0 no matter what peers
    // report -- it floors the min regardless of peer progress.
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:2".to_string()));

    assert_eq!(group.group_snapshot_slot(), 0);
    group.note_peer_durable_for_tests(2, 100);
    assert_eq!(group.group_snapshot_slot(), 0);
}

#[tokio::test]
async fn group_snapshot_slot_is_min_of_local_and_max_peer_durable() {
    // Local WAL durably caught up through slot 5, attached before the
    // replica is wrapped in a group (`set_wal` needs `&mut PxLocalReplica`;
    // `PxGroup::local_replica` only ever hands out `&PxLocalReplica`).
    let mut local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    local.set_wal(wal_with_snapshot_slot(1, 5).await);
    let mut group = PxGroup::new(1, local);
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:2".to_string()));
    group.add_remote_replica(PxRemoteReplica::new(3, "127.0.0.1:3".to_string()));

    // No peer has reported yet: absent peers count as 0, so the max-over-peers
    // is 0 and the watermark stays 0 even though the local WAL is at 5.
    assert_eq!(group.group_snapshot_slot(), 0);

    // Peer 2 reports durable=3 -> min(local 5, max(3)) = 3. Unlike
    // group_safe_slot, peer 3's silence (implicit 0) does NOT hold this back:
    // only the *best* peer needs to be a witness.
    group.note_peer_durable_for_tests(2, 3);
    assert_eq!(group.group_snapshot_slot(), 3);

    // Peer 3 reports durable=4, a *better* witness than peer 2 -> min(5, max(3,4)) = 4.
    group.note_peer_durable_for_tests(3, 4);
    assert_eq!(group.group_snapshot_slot(), 4);

    // Peer 2 now reports durable=9, past the local cap -> min(5, max(9,4)) = 5.
    group.note_peer_durable_for_tests(2, 9);
    assert_eq!(
        group.group_snapshot_slot(),
        5,
        "the leader's own durable slot caps the watermark even when a peer is far ahead"
    );

    // A peer regression cannot pull the published watermark backwards.
    group.note_peer_durable_for_tests(3, 0);
    assert_eq!(group.group_snapshot_slot(), 5);
}

#[tokio::test]
async fn group_snapshot_slot_resets_on_new_leader_tenure() {
    let mut local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    local.set_wal(wal_with_snapshot_slot(1, 5).await);
    let mut group = PxGroup::new(1, local);
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:2".to_string()));
    group.note_peer_durable_for_tests(2, 5);
    assert_eq!(group.group_snapshot_slot(), 5);

    group.reset_safe_slot_tracking_for_tests();

    // Confirms peer state was actually cleared, not just the published
    // atomic: a peer that never re-reports after the reset keeps the
    // watermark at 0 even though the local WAL is still durably at 5.
    assert_eq!(
        group.group_snapshot_slot(),
        0,
        "a fresh leader tenure must not inherit the previous tenure's watermark or stale peer state"
    );
}
