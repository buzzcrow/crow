//! Engine-durability + WAL GC maintenance loop (`group_maintenance`)
//! integration test: verifies `PxGroup::run_maintenance_pass_for_tests`
//! wires a real `CrowtreeEngine`'s `persist_snapshot` into the group's WAL
//! `snapshot_slot`, and that WAL segment GC only fires once the group
//! safe-slot (not just the engine snapshot) allows it.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::PxElectionConfig;
use crowkv::kv::{CrowtreeEngine, CrowtreeOptions};
use crowkv::paxos::roles::{Learner, PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{BlockDevice, IoBackend, WalConfig};

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::BlockDevice(BlockDevice::new()))
}

/// A file-backed `CrowtreeEngine`: `persist_snapshot` (`Crowtree::snapshot`)
/// requires a real `page_store` and fails (`invalid_argument: no
/// page_store`) for the default in-memory (`path: None`) engine, so tests
/// exercising it need a real durable file.
fn open_file_engine(dir: &std::path::Path) -> CrowtreeEngine {
    let path = dir.join("data.ctdb");
    CrowtreeEngine::open(&CrowtreeOptions {
        path: Some(path.display().to_string()),
        ..Default::default()
    })
    .unwrap()
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

/// Learn slots `1..=upto` with real `Put` payloads (unlike a `NoOp`/empty
/// payload, this actually drives `KVEngine::apply` on the replica's engine,
/// which is what a durable engine's `persist_snapshot` reflects).
async fn apply_through_with_engine(replica: &PxLocalReplica, upto: u64) {
    for slot in 1..=upto {
        replica
            .learner
            .learn(
                PxLogEntry {
                    slot,
                    ballot: PxBallot::new(0, 1),
                    term: 1,
                    payload: Bytes::from(encode_put_payload(format!("k{slot}").as_bytes(), b"v")),
                },
                None,
                None,
            )
            .await;
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn maintenance_pass_persists_snapshot_and_gcs_wal_segments_once_safe() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200, // small segments so several accumulate
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=20u64 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let seg_count_before = wal.index().lock().segments().count();
    assert!(seg_count_before >= 2, "need multiple segments for GC test");

    // Real file-backed CrowtreeEngine, attached to a fresh replica with an
    // empty replay result.
    let engine_dir = tempfile::tempdir().unwrap();
    let engine = open_file_engine(engine_dir.path());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(wal.clone());

    // Drive the engine to durably reflect slots 1..=15 (real Put payloads).
    apply_through_with_engine(&replica, 15).await;
    assert_eq!(replica.contiguous_applied(), 15);

    let group = PxGroup::new(1, replica);

    // No real peers, so the recompute uses the local replica's own
    // contiguous_applied as the group safe-slot.
    group.note_peer_applied_for_tests(999, 999);
    assert_eq!(group.group_safe_slot(), 15);

    group.run_maintenance_pass_for_tests().await;

    // `persist_snapshot` durably covers exactly the 15 slots that were
    // `learn()`ed with real payloads, and that's what gets fed into the
    // WAL's own snapshot_slot marker.
    assert_eq!(wal.snapshot_slot(), 15);

    // GC watermark is min(engine_snapshot=15, safe_slot=15) = 15: segments
    // fully below slot 15 are unlinked.
    let seg_count_after = wal.index().lock().segments().count();
    assert!(
        seg_count_after < seg_count_before,
        "wal segments below the maintenance-pass watermark should be GC'd"
    );

    // Replay after GC should still work (only surviving segments).
    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert!(!result.records.is_empty());
}

/// plan-tree #20 follow-up: the maintenance loop's tick interval is a
/// normal per-group tunable (`PxElectionConfig::maintenance_tick_ms`), not
/// a hardcoded constant. Configure a very short tick, start the *real*
/// periodic loop (not `run_maintenance_pass_for_tests`'s direct call), and
/// confirm a pass actually ran once paused virtual time crosses it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn maintenance_loop_uses_configured_tick_interval() {
    let backend = sim_backend();
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal")],
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    let engine_dir = tempfile::tempdir().unwrap();
    let engine = open_file_engine(engine_dir.path());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(wal.clone());
    apply_through_with_engine(&replica, 15).await;

    let mut group = PxGroup::new(1, replica);
    group.set_election_config(PxElectionConfig {
        maintenance_tick_ms: 5,
        ..PxElectionConfig::for_tests()
    });
    group.note_peer_applied_for_tests(999, 999);
    let group = Arc::new(group);

    assert_eq!(wal.snapshot_slot(), 0, "nothing persisted before the loop runs");

    group.start_engine_maintenance_loop().await;
    // Let the freshly spawned task actually run up to its `sleep(tick)`
    // call (registering the timer) before advancing paused virtual time --
    // `tokio::spawn` only schedules it, it doesn't run synchronously.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    // Advance past the configured 5ms tick, then yield again so the woken
    // task actually gets to run its maintenance pass.
    tokio::time::advance(Duration::from_millis(10)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        wal.snapshot_slot(),
        15,
        "the periodic loop should have run a maintenance pass using the configured tick"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn maintenance_pass_does_not_gc_wal_when_safe_slot_lags_snapshot() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=20u64 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let seg_count_before = wal.index().lock().segments().count();

    let engine_dir = tempfile::tempdir().unwrap();
    let engine = open_file_engine(engine_dir.path());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/empty")], 1)
        .await
        .unwrap();
    let mut replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    replica.set_wal(wal.clone());

    // This replica's own engine is durably caught up through slot 15, but a
    // (simulated) lagging voting peer has only applied through slot 2 --
    // `group_safe_slot` must hold GC back regardless of engine progress.
    apply_through_with_engine(&replica, 15).await;
    let mut group = PxGroup::new(1, replica);
    group.add_remote_replica(crowkv::cluster::PxRemoteReplica::new(
        2,
        "127.0.0.1:2".to_string(),
    ));
    group.note_peer_applied_for_tests(2, 2);
    assert_eq!(group.group_safe_slot(), 2);

    group.run_maintenance_pass_for_tests().await;

    // The engine still persists its own snapshot regardless (purely local
    // decision)...
    assert_eq!(wal.snapshot_slot(), 15);
    // ...but WAL GC stays fully blocked by the lagging peer's safe_slot.
    let seg_count_after = wal.index().lock().segments().count();
    assert_eq!(
        seg_count_after, seg_count_before,
        "a lagging voting peer's safe_slot must hold WAL GC back even though this replica's engine is far ahead"
    );
}
