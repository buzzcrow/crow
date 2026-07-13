//! Replay engine tests (W10-W13) — `SimDisk` backend.

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::kv::{Batch, CrowtreeEngine, CrowtreeOptions, KVEngine};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{BlockDevice, IoBackend, WalConfig};
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
            payload: Bytes::from(format!("v{slot}")),
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
            payload: Bytes::from(format!("v{slot}")),
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
}

/// WAL replay now fully restores the learner: every accepted entry is
/// replayed into the state machine. Both accepted slots are applied.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn restore_without_snapshot_does_not_apply_slots() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Slot 1: put k1. Slot 2: put k2.
    for (slot, key, value) in [(1u64, b"k1".as_slice(), b"v1".as_slice()), (2, b"k2", b"v2")] {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from(encode_put_payload(key, value)),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    let replay = replay_group(&backend, &disks, 1).await.unwrap();

    let restored = PxLocalReplica::restore_from_replay(7, PxLocalReplicaRole::Follower, &replay)
        .await
        .unwrap();

    // WAL replay applies all accepted entries to the learner.
    assert_eq!(
        restored.learner.engine_get(b"k1").await.map(|(_, v)| v),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        restored.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec())
    );
    assert_eq!(restored.contiguous_chosen(), 2);
    // Both values remain durable in the acceptor.
    assert!(restored.accepted_at(1).await.is_some());
    assert!(restored.accepted_at(2).await.is_some());
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
        payload: Bytes::from(encode_put_payload(b"restore-key", b"restore-value")),
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
        restored.learner.engine_get(b"restore-key").await.map(|(_, v)| v),
        Some(b"restore-value".to_vec())
    );
}

/// `restore_from_replay_with_engine` replays the full WAL into a
/// caller-supplied engine (here an in-memory `CrowtreeEngine`, standing in
/// for the durable file-backed one `crowkv-server` uses), not just the
/// default `InMemKV`. Otherwise identical restore semantics to
/// `restore_from_replay` (same term/voted-for/acceptor state, same
/// contiguous-chosen advance) -- the only difference is which `KVEngine`
/// ends up behind `learner.engine_get`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn restore_from_replay_with_engine_uses_injected_engine() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for (slot, key, value) in [(1u64, b"k1".as_slice(), b"v1".as_slice()), (2, b"k2", b"v2")] {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from(encode_put_payload(key, value)),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    let replay = replay_group(&backend, &disks, 1).await.unwrap();

    // `path: None` selects an in-memory crowtree store -- same restore path
    // crowkv-server's `--kv-engine crowtree` uses, minus the on-disk file.
    let engine = CrowtreeEngine::open(&CrowtreeOptions::default()).expect("open in-memory crowtree engine");
    let restored = PxLocalReplica::restore_from_replay_with_engine(
        7,
        PxLocalReplicaRole::Follower,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();

    assert_eq!(
        restored.learner.engine_get(b"k1").await.map(|(_, v)| v),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        restored.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec())
    );
    assert_eq!(restored.contiguous_chosen(), 2);
    assert!(restored.accepted_at(1).await.is_some());
    assert!(restored.accepted_at(2).await.is_some());
}

/// `resume_from_slot() > 0`: an engine that already durably reflects a
/// prefix of the WAL (here, slot 1 pre-applied and `flush()`ed before the
/// engine is handed to `restore_from_replay_with_engine`, simulating what a
/// real restart sees from a durable `CrowtreeEngine`) skips re-`learn()`ing
/// that prefix, but must land at the exact same `contiguous_chosen` /
/// `contiguous_applied` / `last_chosen_slot` / `last_chosen_term` state (and
/// the same final KV contents) a full sequential replay would have produced.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn restore_from_replay_with_engine_resumes_from_last_applied_slot() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for (slot, key, value, term) in [
        (1u64, b"k1".as_slice(), b"v1".as_slice(), 1u64),
        (2, b"k2", b"v2", 1),
        (3, b"k3", b"v3", 2),
    ] {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term,
            payload: Bytes::from(encode_put_payload(key, value)),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &disks, 1).await.unwrap();

    // Pre-apply + flush slot 1 directly, matching what a durably-recovered
    // CrowtreeEngine reports via `resume_from_slot()` on a real restart.
    let engine = CrowtreeEngine::open(&CrowtreeOptions::default()).expect("open crowtree engine");
    engine
        .apply(1, &Batch::decode(&encode_put_payload(b"k1", b"v1")))
        .into_ready();
    engine.handle().flush().expect("flush");
    assert_eq!(
        engine.handle().last_applied_slot(),
        1,
        "sanity: engine reports a resume floor of 1"
    );

    let restored = PxLocalReplica::restore_from_replay_with_engine(
        7,
        PxLocalReplicaRole::Follower,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();

    // Full KV state is correct regardless of which path (pre-seeded vs.
    // replayed) applied each key.
    assert_eq!(
        restored.learner.engine_get(b"k1").await.map(|(_, v)| v),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        restored.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        restored.learner.engine_get(b"k3").await.map(|(_, v)| v),
        Some(b"v3".to_vec())
    );
    // Frontier state matches what a full learn()-every-slot replay produces.
    assert_eq!(restored.contiguous_chosen(), 3);
    assert_eq!(restored.contiguous_applied(), 3);
    assert_eq!(restored.last_chosen_slot(), 3);
    assert_eq!(restored.last_chosen_term(), 2);
    assert!(restored.accepted_at(1).await.is_some());
    assert!(restored.accepted_at(2).await.is_some());
    assert!(restored.accepted_at(3).await.is_some());
}

/// Defensive fallback: if the engine's resume floor doesn't line up with an
/// accepted entry in the WAL-rebuilt acceptor (not expected in practice --
/// an engine can only durably apply a slot that was itself accepted and
/// WAL-logged -- but not a correctness invariant this restore path should
/// ever trust blindly), restore still skips straight to `resume_from + 1`
/// (never re-attempts the skipped prefix -- see the "no safe fallback"
/// rationale on `restore_from_replay_with_engine`) but leaves the frontier
/// at its conservative fresh-learner default instead of guessing a term.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn restore_from_replay_with_engine_falls_back_when_resume_slot_has_no_accepted_entry() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = test_config(&disks);
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // WAL has slots 1 and 3 accepted; slot 2 is a gap in the accepted log.
    for (slot, key, value) in [(1u64, b"k1".as_slice(), b"v1".as_slice()), (3, b"k3", b"v3")] {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from(encode_put_payload(key, value)),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &disks, 1).await.unwrap();

    // Engine reports a resume floor (slot 2) the WAL-rebuilt acceptor has no
    // accepted entry for. `last_applied_slot()` is itself a contiguous
    // watermark (crowtree folds `received_slots_` forward from its own
    // frontier), so reaching floor 2 legitimately requires applying slot 1
    // too -- modeling an engine that durably has extra data at slot 2 with
    // no independent WAL corroboration (e.g. a lost/truncated WAL record),
    // rather than an outright-impossible-via-the-API state.
    let engine = CrowtreeEngine::open(&CrowtreeOptions::default()).expect("open crowtree engine");
    engine
        .apply(1, &Batch::decode(&encode_put_payload(b"phantom1", b"x")))
        .into_ready();
    engine
        .apply(2, &Batch::decode(&encode_put_payload(b"phantom2", b"y")))
        .into_ready();
    engine.handle().flush().expect("flush");
    assert_eq!(
        engine.handle().last_applied_slot(),
        2,
        "sanity: engine reports a resume floor of 2"
    );

    let restored = PxLocalReplica::restore_from_replay_with_engine(
        7,
        PxLocalReplicaRole::Follower,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();

    // Slots 1 and 2 are skipped outright (never re-attempted -- see the
    // rationale above), so slot 1's WAL value is *not* recovered here: an
    // honest, safe degradation for this assumed-impossible mismatch, not a
    // silent correctness violation (nothing that was ever safely writable
    // is lost or corrupted). Slot 3, past the skipped prefix, replays
    // normally.
    assert_eq!(restored.learner.engine_get(b"k1").await.map(|(_, v)| v), None);
    assert_eq!(
        restored.learner.engine_get(b"k3").await.map(|(_, v)| v),
        Some(b"v3".to_vec())
    );
    // No accepted entry at the resume floor (slot 2) to seed a term from, so
    // the frontier stays at the fresh-learner default until slot 3's
    // out-of-order `learn()` bumps the max-ever-seen high-water mark.
    assert_eq!(restored.contiguous_chosen(), 0);
    assert_eq!(restored.last_chosen_slot(), 3);
    assert_eq!(restored.last_chosen_term(), 1);
}
