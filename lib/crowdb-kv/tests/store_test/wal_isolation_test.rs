// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-group WAL path isolation within a single `PxKvStore`.
//!
//! Groups within a store share the same `wal_disks` root but each group's
//! WAL segments live in an isolated subdirectory (`{wal_root}/group{group_id}/`).
//! This test verifies that writes to one group's WAL do not leak into another
//! group's replay — the "path isolation" half of the per-group WAL-root
//! isolation contract. True disk isolation (different physical disks per
//! group) remains blocked on a store-level config change.

use std::path::PathBuf;
use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::WalConfig;
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::{IoBackend, WalEngine, WalRecordFormat};

const REPLICA_ID: u64 = 1;

async fn create_file_wal(wal_root: PathBuf, group_id: u64) -> Arc<WalEngine> {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_root);
    config.wal_record_format = WalRecordFormat::Binary;
    WalEngine::create(backend, config, group_id)
        .await
        .expect("create file-backed wal")
}

fn leader_group_with_wal(group_id: u64, wal: Arc<WalEngine>) -> PxGroup {
    let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
    replica.set_wal(wal);
    PxGroup::new(group_id, replica)
}

/// Two groups in a single store share the same WAL root directory. After
/// writing distinct keys to each group, replaying group 1's WAL must recover
/// only group 1's records, and replaying group 2's WAL must recover only
/// group 2's records — no cross-group leakage.
#[tokio::test]
async fn per_group_wal_path_isolation() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_root = tmp.path().join("wal");

    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());

    // Create two groups with WALs sharing the same root. Each WalEngine
    // appends `group{group_id}` to the root, so group 1's segments land in
    // `{wal_root}/group1/` and group 2's in `{wal_root}/group2/`.
    let wal1 = create_file_wal(wal_root.clone(), 1).await;
    let wal2 = create_file_wal(wal_root.clone(), 2).await;
    store.add_group(leader_group_with_wal(1, wal1));
    store.add_group(leader_group_with_wal(2, wal2));

    // Write distinct keys to each group.
    assert!(
        store.kv_put(1, b"iso-g1-key", b"val-g1", 7, 1, 1, 1).await.ok,
        "group 1 put should succeed"
    );
    assert!(
        store.kv_put(2, b"iso-g2-key", b"val-g2", 7, 1, 2, 2).await.ok,
        "group 2 put should succeed"
    );

    // Seal all segments so the on-disk WAL is complete for replay.
    for gid in [1, 2] {
        store
            .get_group(gid)
            .unwrap()
            .local_replica()
            .wal()
            .expect("wal attached")
            .seal_all()
            .await
            .expect("seal");
    }

    // Replay each group independently from the shared root.
    let backend = Arc::new(IoBackend::File);
    let disks = vec![wal_root.clone()];

    let replay1 = replay_group(&backend, &disks, 1).await.expect("replay group 1");
    let replay2 = replay_group(&backend, &disks, 2).await.expect("replay group 2");

    // Each replay recovers only its own group's records.
    assert!(
        !replay1.records.is_empty(),
        "group 1 replay should recover records"
    );
    assert!(
        !replay2.records.is_empty(),
        "group 2 replay should recover records"
    );

    // Verify no cross-group leakage: every record in replay1 belongs to
    // group 1, and every record in replay2 belongs to group 2.
    for record in &replay1.records {
        assert_eq!(
            record.group_id, 1,
            "group 1 replay must not contain records from another group"
        );
    }
    for record in &replay2.records {
        assert_eq!(
            record.group_id, 2,
            "group 2 replay must not contain records from another group"
        );
    }

    // The on-disk directories must be isolated: group1/ and group2/ exist
    // as siblings under the shared root.
    assert!(
        wal_root.join("group1").exists(),
        "group 1 WAL directory must exist under the shared root"
    );
    assert!(
        wal_root.join("group2").exists(),
        "group 2 WAL directory must exist under the shared root"
    );
}
