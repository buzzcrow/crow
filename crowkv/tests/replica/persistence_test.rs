//! Replica-layer persistence round-trip.
//!
//! Build a `PxLocalReplica` backed by a real `File` WAL, commit a sequence of
//! KV writes through the replica's own durability path (`on_accept` logs the
//! accepted value; `learn_chosen` applies it and persists the durable-commit
//! watermark), then drop the replica to simulate a process restart. Rebuilding
//! from the on-disk WAL via `replay_group` + `restore_from_replay` must reload
//! the committed KV state, the applied frontier, and the per-slot accepted log.
//!
//! This is the replica-level analogue of the WAL `restore_*` tests: it proves
//! the layer the local replica is responsible for — "do KV here, close, reopen,
//! and find the state again".

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::WalConfig;
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine, WalRecordFormat};

const GROUP: u64 = 1;
const REPLICA_ID: u64 = 7;

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // op = PUT
    buf.push(0); // flags
    let key_len = u32::try_from(key.len()).expect("key length exceeds u32");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    let value_len = u32::try_from(value.len()).expect("value length exceeds u32");
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

fn write_entry(slot: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, REPLICA_ID),
        term: 1,
        kind: PxLogEntryKind::Write,
        payload: Bytes::from(encode_put_payload(key, value)),
        client_id: Some(1),
        seq: Some(slot),
    }
}

async fn create_file_wal(wal_dir: PathBuf) -> Arc<WalEngine> {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_dir);
    config.wal_record_format = WalRecordFormat::Binary;
    WalEngine::create(backend, config, GROUP)
        .await
        .expect("create file-backed wal")
}

#[tokio::test]
async fn wal_backed_replica_reloads_committed_kv_after_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    // ── write phase: commit three slots through the replica, then close ──
    // Slot 3 overwrites "alpha" so we also prove highest-slot-wins survives.
    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        for (slot, key, value) in [
            (1u64, b"alpha".as_slice(), b"1".as_slice()),
            (2, b"beta", b"2"),
            (3, b"alpha", b"3"),
        ] {
            let entry = write_entry(slot, key, value);
            let reply = replica.on_accept(entry.clone()).await;
            assert!(
                matches!(reply, crowkv::paxos::roles::PxAcceptReply::Accepted { .. }),
                "slot {slot} should be accepted: {reply:?}"
            );
            replica.learn_chosen(&entry).await;
        }
        assert_eq!(replica.contiguous_applied(), 3, "applied frontier at slot 3");
        wal.seal_all().await.expect("seal");
        // `replica` and `wal` drop here — the in-memory state is gone.
    }

    // ── reopen phase: rebuild purely from the on-disk WAL ──
    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    assert_eq!(
        replay.durable_commit_watermark, 3,
        "watermark persisted across restart"
    );

    let restored = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore replica");

    // Committed KV reloaded: the slot-3 overwrite wins for "alpha".
    assert_eq!(
        restored.learner.engine_get(b"alpha").map(|(_, v)| v),
        Some(b"3".to_vec()),
        "overwrite survives restart"
    );
    assert_eq!(
        restored.learner.engine_get(b"beta").map(|(_, v)| v),
        Some(b"2".to_vec()),
        "earlier write survives restart"
    );
    // Applied frontier and per-slot accepted log are reloaded too.
    assert_eq!(restored.contiguous_applied(), 3);
    assert!(restored.accepted_at(1).await.is_some());
    assert!(restored.accepted_at(3).await.is_some());
}
