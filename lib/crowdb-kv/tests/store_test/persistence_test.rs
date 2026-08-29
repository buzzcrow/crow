// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Store-layer persistence round-trip.
//!
//! Drive KV through the `PxKvStore` public API (`kv_put` / `kv_delete`) against
//! a single-leader, WAL-backed group, then drop the whole store to simulate a
//! restart. Reopening a fresh store whose group is rebuilt from the on-disk WAL
//! (`replay_group` + `restore_from_replay`, the same wiring server startup uses)
//! must serve every committed value — and respect committed deletes — through
//! `kv_get`.
//!
//! This sits one layer above `replica::persistence`: it proves the store's
//! routing + KV API reload correctly on top of the replica's durable state. It
//! uses the embedded library only (no `crowdb-kv-server` HTTP process), so it lives
//! in `crowdb_kv`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::WalConfig;
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::{IoBackend, WalEngine, WalRecordFormat};

const GROUP: u64 = 1;
const REPLICA_ID: u64 = 1;

async fn create_file_wal(wal_dir: PathBuf) -> Arc<WalEngine> {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_dir);
    config.wal_record_format = WalRecordFormat::Binary;
    WalEngine::create(backend, config, GROUP)
        .await
        .expect("create file-backed wal")
}

/// Rebuild a single-leader group from the WAL directory, restoring committed
/// state and resuming slot allocation past the recovered frontier.
async fn reopen_group(wal_dir: &Path) -> PxGroup {
    let backend = Arc::new(IoBackend::File);
    let disks = vec![wal_dir.to_path_buf()];
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");

    let next_seg = replay.max_segment_id.saturating_add(1).max(1);
    let mut config = WalConfig::with_root(wal_dir.to_path_buf());
    config.wal_record_format = WalRecordFormat::Binary;
    let wal = WalEngine::create_with_next_segment_id(backend, config, GROUP, next_seg)
        .await
        .expect("create file-backed wal");

    let mut replica = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore replica");
    replica.set_wal(wal);

    let group = PxGroup::new(GROUP, replica);
    let next_slot = group
        .local_replica()
        .highest_seen_slot()
        .max(group.local_replica().last_chosen_slot())
        .max(group.local_replica().contiguous_applied())
        .saturating_add(1)
        .max(1);
    group.set_next_slot(next_slot);
    group
}

#[tokio::test]
async fn store_reloads_kv_through_public_api_after_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    // ── write phase: a single-leader WAL-backed store, then close ──
    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(wal);
        let group = PxGroup::new(GROUP, replica);

        let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
        store.add_group(group);

        assert!(store.kv_put(GROUP, b"alpha", b"1", 7, 1, 1, 1).await.ok);
        assert!(store.kv_put(GROUP, b"beta", b"2", 7, 2, 2, 2).await.ok);
        assert!(store.kv_put(GROUP, b"alpha", b"3", 7, 3, 3, 3).await.ok); // overwrite
        assert!(store.kv_delete(GROUP, b"beta", 7, 4, 4, 4).await.ok); // committed delete

        // Flush sealed segments before the simulated restart.
        store
            .get_group(GROUP)
            .unwrap()
            .local_replica()
            .wal()
            .expect("wal attached")
            .seal_all()
            .await
            .expect("seal");
        // `store` (and its WAL) drop here.
    }

    // ── reopen phase: fresh store rebuilt from the on-disk WAL ──
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(reopen_group(&wal_dir).await);

    // WAL replay now fully restores the learner: every accepted entry is
    // replayed into the state machine. alpha = "3" (slot 3 overwrites slot 1),
    // beta was deleted in slot 4.
    let alpha = store.kv_get(GROUP, b"alpha", 3, 0, 10, 10).await;
    assert!(
        alpha.ok && !alpha.not_found && alpha.value.as_ref() == b"3",
        "alpha should be '3' after replay: {alpha:?}"
    );

    let beta = store.kv_get(GROUP, b"beta", 3, 0, 11, 11).await;
    assert!(
        !beta.ok || beta.not_found,
        "beta should be deleted after replay: {beta:?}"
    );
}
