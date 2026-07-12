use std::sync::Arc;

use bytes::Bytes;
use crowkv::cluster::group::ProposeResult;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::local_replica::PxLocalReplicaRole;
use crowkv::common::config::{PxElectionConfig, WalConfig};
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine};
use crowkv_server::startup::{create_group_with_wal, store_wal_root};

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
        kind: PxLogEntryKind::Write,
        payload: Bytes::from(encode_put_payload(b"restore-key", b"restore-value")),
        client_id: Some(42),
        seq: Some(9),
    };
    wal.append(&WALRecord::from_accepted(group_id, &accepted_entry))
        .await
        .unwrap();
    wal.append(&WALRecord::from_vote_granted(group_id, 5, 99))
        .await
        .unwrap();
    wal.seal_all().await.unwrap();

    let group = create_group_with_wal(
        store_id,
        group_id,
        replica_id,
        PxLocalReplicaRole::Leader,
        PxElectionConfig::for_tests(),
        &wal_root,
        backend.clone(),
    )
    .await
    .unwrap();

    let replica = group.local_replica();
    assert_eq!(replica.current_term(), 5);
    assert_eq!(replica.voted_for(), Some(99));
    assert_eq!(replica.accepted_at(2).await, Some(accepted_entry.clone()));
    assert_eq!(replica.promised_at(1).await, Some(PxBallot::new(2, replica_id)));
    assert_eq!(replica.learner.engine_get(b"restore-key"), None);

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
    assert_eq!(replay.dedup_cache.get(&55), Some(&(1, 3)));
}
