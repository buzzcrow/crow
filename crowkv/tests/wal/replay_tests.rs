//! Replay engine tests (W10-W13) — `SimDisk` backend.

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::wal::record::{RecordType, WALRecord};
use crowkv::wal::replay::{encode_dedup_checkpoint, replay_group};
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{BlockDevice, IoBackend, WalConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::BlockDevice(BlockDevice::new()))
}

fn test_config(disks: &[PathBuf]) -> WalConfig {
    WalConfig {
        wal_disks: disks.to_owned(),
        wal_segment_size: 1024 * 1024,
        ..Default::default()
    }
}

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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_empty_returns_empty_result() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    // Create the directory so replay doesn't fail.
    backend
        .create_dir_all(&PathBuf::from("/wal/group1"))
        .await
        .unwrap();

    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert!(result.records.is_empty());
    assert_eq!(result.max_segment_id, 0);
    assert_eq!(result.current_term, 0);
    assert!(result.voted_for.is_none());
    assert!(result.dedup_cache.is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_recovers_records() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Write 5 accepted records.
    for slot in 1..=5 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 3,
            kind: PxLogEntryKind::Write,
            payload: Bytes::from(format!("v{slot}")),
            client_id: Some(10),
            seq: Some(slot),
        };
        let record = WALRecord::from_accepted(1, &entry);
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    // Replay.
    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert_eq!(result.records.len(), 5);
    assert_eq!(result.current_term, 3);
    assert!(result.max_segment_id >= 1);
    assert_eq!(result.index.slot_count(), 5);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_rebuilds_current_term() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Write records at increasing terms.
    for (i, term) in [5, 10, 15].iter().enumerate() {
        let record = WALRecord::from_promised(1, *term, (i + 1) as u64, PxBallot::new(0, 1));
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert_eq!(result.current_term, 15);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_rebuilds_voted_for() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // VoteGranted at term 5 for node 42.
    let r1 = WALRecord::from_vote_granted(1, 5, 42);
    wal.append(&r1).await.unwrap();

    // VoteGranted at term 10 for node 99.
    let r2 = WALRecord::from_vote_granted(1, 10, 99);
    wal.append(&r2).await.unwrap();

    wal.seal_all().await.unwrap();

    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert_eq!(result.current_term, 10);
    assert_eq!(result.voted_for, Some(99));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replica_persists_self_vote_for_replay() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    let mut replica = PxLocalReplica::new(7, PxLocalReplicaRole::Follower);
    replica.set_wal(wal.clone());
    replica.become_candidate(11);
    replica.persist_current_vote().await;
    wal.seal_all().await.unwrap();

    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert_eq!(result.current_term, 11);
    assert_eq!(result.voted_for, Some(7));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_rebuilds_dedup_cache() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Write accepted records with client_id/seq.
    for slot in 1..=3 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            kind: PxLogEntryKind::Write,
            payload: Bytes::from("data"),
            client_id: Some(100),
            seq: Some(slot),
        };
        let record = WALRecord::from_accepted(1, &entry);
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert_eq!(result.dedup_cache.len(), 1);
    let (last_seq, last_slot) = result.dedup_cache[&100];
    assert_eq!(last_seq, 3);
    assert_eq!(last_slot, 3);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_dedup_checkpoint_plus_subsequent() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Write a dedup checkpoint.
    let mut cache: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    cache.insert(100, (5, 5)); // client 100, seq 5, slot 5
    let payload = encode_dedup_checkpoint(&cache);
    let checkpoint = WALRecord {
        record_type: RecordType::DedupCheckpoint,
        group_id: 1,
        term: 1,
        slot: 0,
        ballot: PxBallot::new(0, 0),
        payload: Bytes::from(payload),
    };
    wal.append(&checkpoint).await.unwrap();

    // Write a subsequent accepted record for client 100 at seq 6.
    let entry = PxLogEntry {
        slot: 6,
        ballot: PxBallot::new(0, 1),
        term: 1,
        kind: PxLogEntryKind::Write,
        payload: Bytes::from("data"),
        client_id: Some(100),
        seq: Some(6),
    };
    let record = WALRecord::from_accepted(1, &entry);
    wal.append(&record).await.unwrap();
    wal.seal_all().await.unwrap();

    let result = replay_group(&backend, &disks, 1).await.unwrap();
    let (last_seq, last_slot) = result.dedup_cache[&100];
    assert_eq!(last_seq, 6);
    assert_eq!(last_slot, 6);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_1000_records_deterministic() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Write 1000 records.
    for slot in 1..=1000 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            kind: PxLogEntryKind::Write,
            payload: Bytes::from(format!("v{slot}")),
            client_id: Some(1),
            seq: Some(slot),
        };
        let record = WALRecord::from_accepted(1, &entry);
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    // Replay and verify determinism.
    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert_eq!(result.records.len(), 1000);
    assert_eq!(result.current_term, 1);
    assert_eq!(result.index.slot_count(), 1000);

    // Dedup: client 1 should have last_seq=1000, last_slot=1000.
    let (last_seq, last_slot) = result.dedup_cache[&1];
    assert_eq!(last_seq, 1000);
    assert_eq!(last_slot, 1000);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn restore_from_replay_rebuilds_live_replica_state() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    let promised = WALRecord::from_promised(1, 2, 1, PxBallot::new(2, 7));
    wal.append(&promised).await.unwrap();

    let accepted_entry = PxLogEntry {
        slot: 2,
        ballot: PxBallot::new(3, 7),
        term: 3,
        kind: PxLogEntryKind::Write,
        payload: Bytes::from(encode_put_payload(b"restore-key", b"restore-value")),
        client_id: Some(42),
        seq: Some(9),
    };
    let accepted = WALRecord::from_accepted(1, &accepted_entry);
    wal.append(&accepted).await.unwrap();

    let vote = WALRecord::from_vote_granted(1, 5, 99);
    wal.append(&vote).await.unwrap();
    wal.seal_all().await.unwrap();

    let replay = replay_group(&backend, &disks, 1).await.unwrap();
    let restored = PxLocalReplica::restore_from_replay(7, PxLocalReplicaRole::Follower, &replay)
        .await
        .unwrap();

    assert_eq!(restored.current_term(), 5);
    assert_eq!(restored.voted_for(), Some(99));
    assert_eq!(restored.role(), PxLocalReplicaRole::Follower);
    assert_eq!(restored.promised_at(1).await, Some(PxBallot::new(2, 7)));
    assert_eq!(restored.accepted_at(2).await, Some(accepted_entry.clone()));
    assert_eq!(restored.contiguous_chosen(), 0);
    assert_eq!(restored.last_chosen_slot(), 2);
    assert_eq!(
        restored.learner.engine_get(b"restore-key"),
        Some((2, b"restore-value".to_vec()))
    );
    assert_eq!(restored.learner.dedup_lookup(42, 9), Some(2));
    assert_eq!(restored.learner.dedup_lookup(42, 8), Some(2));
    assert_eq!(restored.learner.dedup_lookup(42, 10), None);
}
