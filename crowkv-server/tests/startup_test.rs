use std::sync::Arc;

use bytes::Bytes;
use crowkv::cluster::group::ProposeResult;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::local_replica::PxLocalReplicaRole;
use crowkv::common::config::{PxElectionConfig, WalConfig};
use crowkv::kv::CrowtreeBackend;
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine};
use crowkv_server::startup::{create_group_with_wal, store_wal_root};
use crowkv_server::store_registry::KvEngineKind;

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1);
    buf.push(0);
    let key_len = u32::try_from(key.len()).expect("key length exceeds u32");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    let value_len = u32::try_from(value.len()).expect("value length exceeds u32");
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

#[tokio::test]
async fn create_group_with_wal_restores_and_resumes_at_next_slot() {
    let temp = tempfile::tempdir().unwrap();
    let wal_root = temp.path().join("wal-root");
    let config_root = temp.path().join("conf-root");
    let backend = Arc::new(IoBackend::detect());
    let store_id = 9;
    let group_id = 11;
    let replica_id = 7;

    let config = WalConfig::with_root(store_wal_root(&wal_root, store_id));
    let wal = WalEngine::create(backend.clone(), config.clone(), group_id)
        .await
        .unwrap();

    wal.append(&WALRecord::from_promised(
        group_id,
        2,
        1,
        PxBallot::new(2, replica_id),
    ))
    .await
    .unwrap();

    let accepted_entry = PxLogEntry {
        slot: 2,
        ballot: PxBallot::new(3, replica_id),
        term: 3,
        payload: Bytes::from(encode_put_payload(b"restore-key", b"restore-value")),
    };
    wal.append(&WALRecord::from_accepted(group_id, &accepted_entry))
        .await
        .unwrap();
    wal.append(&WALRecord::from_vote_granted(group_id, 5, 99))
        .await
        .unwrap();
    wal.seal_all().await.unwrap();

    let data_root = temp.path().join("data-root");
    let group = create_group_with_wal(
        store_id,
        group_id,
        replica_id,
        PxLocalReplicaRole::Leader,
        PxElectionConfig::for_tests(),
        &wal_root,
        &config_root,
        backend.clone(),
        KvEngineKind::Memory,
        &data_root,
        CrowtreeBackend::File,
    )
    .await
    .unwrap();

    let replica = group.local_replica();
    assert_eq!(replica.current_term(), 5);
    assert_eq!(replica.voted_for(), Some(99));
    assert_eq!(replica.accepted_at(2).await, Some(accepted_entry.clone()));
    assert_eq!(replica.promised_at(1).await, Some(PxBallot::new(2, replica_id)));
    assert_eq!(
        replica.learner.engine_get(b"restore-key").await.map(|(_, v)| v),
        Some(b"restore-value".to_vec()),
        "WAL replay applies accepted slot 2 to the learner"
    );

    replica.become_leader();
    group.stamp_proposing_term(replica.current_term());

    let result = group
        .propose(encode_put_payload(b"new-key", b"new-value"), Some(55), Some(1))
        .await;
    match result {
        ProposeResult::Chosen { slot } => assert_eq!(slot, 3),
        other => panic!("expected chosen proposal after restore, got {other:?}"),
    }

    let replay = replay_group(&backend, &config.wal_disks, group_id).await.unwrap();
    assert!(replay.records.iter().any(|record| {
        record.slot == 3 && matches!(record.record_type, crowkv::wal::record::RecordType::Accepted)
    }));
}

/// `--kv-engine crowtree` end-to-end: a group backed by a durable
/// `CrowtreeEngine` file survives a simulated process restart (drop the
/// group, then call `create_group_with_wal` again against the same
/// `wal_root`/`data_root`) with its KV state intact -- via full WAL replay
/// into a fresh `CrowtreeEngine::open` at the same file
/// (`PxLocalReplica::restore_from_replay_with_engine`), not by any
/// resume-from-last-applied-slot shortcut (not implemented; see /// #20's note on why that needs separate, careful frontier-seeding work).
/// Parameterized over [`CrowtreeBackend`] so the same
/// scenario covers both the default buffered-file backend and the raw
/// `O_DIRECT` block-device backend.
async fn crowtree_engine_persists_across_restart(crowtree_backend: CrowtreeBackend) {
    let temp = tempfile::tempdir().unwrap();
    let wal_root = temp.path().join("wal-root");
    let config_root = temp.path().join("conf-root");
    let data_root = temp.path().join("data-root");
    let backend = Arc::new(IoBackend::detect());
    let store_id = 21;
    let group_id = 5;
    let replica_id = 1;

    let group = create_group_with_wal(
        store_id,
        group_id,
        replica_id,
        PxLocalReplicaRole::Leader,
        PxElectionConfig::for_tests(),
        &wal_root,
        &config_root,
        backend.clone(),
        KvEngineKind::Crowtree,
        &data_root,
        crowtree_backend,
    )
    .await
    .unwrap();

    // The durable crowtree file was created under data_root, not left at the
    // default in-memory (no file) path.
    let ct_path = crowkv_server::startup::store_crowtree_path(&data_root, store_id, group_id);
    assert!(
        ct_path.exists(),
        "expected a durable crowtree file at {}",
        ct_path.display()
    );

    group.local_replica().become_leader();
    group.stamp_proposing_term(group.local_replica().current_term());
    let result = group
        .propose(encode_put_payload(b"ct-key", b"ct-value"), Some(1), Some(1))
        .await;
    match result {
        ProposeResult::Chosen { slot } => assert_eq!(slot, 1),
        other => panic!("expected chosen proposal, got {other:?}"),
    }
    assert_eq!(
        group
            .local_replica()
            .learner
            .engine_get(b"ct-key")
            .await
            .map(|(_, v)| v),
        Some(b"ct-value".to_vec())
    );

    // Simulate a process restart: drop the group (closes the crowtree file
    // handle via `Crowtree`'s `Drop`), then rebuild from the same WAL +
    // crowtree file.
    drop(group);

    let restarted = create_group_with_wal(
        store_id,
        group_id,
        replica_id,
        PxLocalReplicaRole::Leader,
        PxElectionConfig::for_tests(),
        &wal_root,
        &config_root,
        backend.clone(),
        KvEngineKind::Crowtree,
        &data_root,
        crowtree_backend,
    )
    .await
    .unwrap();

    assert_eq!(
        restarted
            .local_replica()
            .learner
            .engine_get(b"ct-key")
            .await
            .map(|(_, v)| v),
        Some(b"ct-value".to_vec()),
        "crowtree-backed KV state must survive a simulated restart"
    );
}

#[tokio::test]
async fn create_group_with_wal_crowtree_engine_persists_across_restart() {
    crowtree_engine_persists_across_restart(CrowtreeBackend::File).await;
}

/// : same scenario, through `BlockPageStore` (`O_DIRECT`)
/// instead of the default `FilePageStore`.
#[tokio::test]
async fn create_group_with_wal_crowtree_block_backend_persists_across_restart() {
    crowtree_engine_persists_across_restart(CrowtreeBackend::Block).await;
}
