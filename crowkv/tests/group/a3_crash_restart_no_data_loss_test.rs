// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A3 — single-node crash/restart no-data-loss (real fsync).
//!
//! Drives the durable WAL path over the real `File` backend rooted in a
//! `tempfile` directory: append records under the ack contract (each
//! `WalEngine::append` fdatasyncs before resolving), then simulate a process
//! crash by dropping the engine *without* a clean `seal_all`. A fresh backend
//! then `replay_group`s the on-disk segments and `PxLocalReplica`
//! `restore_from_replay` rebuilds a replica; every accepted value, the term,
//! `voted_for`, the promise, and the dedup cache must survive intact.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{IoBackend, WalConfig};

const GROUP: u64 = 1;

/// Encode an engine PUT payload (mirrors the engine's wire format used by the
/// learner when applying chosen writes).
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

fn accepted_write(slot: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(3, 7),
        term: 3,
        payload: Bytes::from(encode_put_payload(key, value)),
    }
}

#[tokio::test]
async fn single_node_crash_restart_preserves_committed_state() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let wal_root = tmp.path().join("data").join("wal");
    let disks = vec![PathBuf::from(&wal_root)];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 1024 * 1024,
        ..Default::default()
    };

    // Records to persist, keyed by slot for post-restart verification.
    let entries: Vec<(PxLogEntry, Vec<u8>, Vec<u8>)> = (1..=5u64)
        .map(|slot| {
            let key = format!("key-{slot}").into_bytes();
            let value = format!("value-{slot}").into_bytes();
            (accepted_write(slot, &key, &value), key, value)
        })
        .collect();
    // A promise on a higher slot and a granted vote at a later term.
    let promise_slot = 6u64;
    let promise_ballot = PxBallot::new(4, 7);

    // ── Pre-crash: write everything durably through the ack contract. ──
    {
        let backend = Arc::new(IoBackend::File);
        let wal = WalEngine::create(backend, config.clone(), GROUP)
            .await
            .expect("create wal engine");

        for (entry, _, _) in &entries {
            wal.append(&WALRecord::from_accepted(GROUP, entry))
                .await
                .expect("append accepted");
        }
        wal.append(&WALRecord::from_promised(GROUP, 4, promise_slot, promise_ballot))
            .await
            .expect("append promise");
        wal.append(&WALRecord::from_vote_granted(GROUP, 5, 99))
            .await
            .expect("append vote");

        // Simulate a crash: drop the engine WITHOUT seal_all(), so the active
        // segment has no footer — exactly the unclean-shutdown shape.
        drop(wal);
    }

    // ── Restart: a fresh backend replays the on-disk segments. ──
    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay group");

    let restored = PxLocalReplica::restore_from_replay(7, PxLocalReplicaRole::Follower, &replay)
        .await
        .expect("restore replica");

    // Election state survived.
    assert_eq!(restored.current_term(), 5, "current_term survives crash");
    assert_eq!(restored.voted_for(), Some(99), "voted_for survives crash");
    assert_eq!(restored.role(), PxLocalReplicaRole::Follower);

    // The outstanding promise survived.
    assert_eq!(
        restored.promised_at(promise_slot).await,
        Some(promise_ballot),
        "promise ballot survives crash"
    );

    assert_eq!(
        restored.highest_seen_slot(),
        6,
        "highest seen slot survives crash"
    );
    assert_eq!(
        restored.last_chosen_slot(),
        5,
        "replay replays all accepted entries into the learner"
    );
    assert_eq!(
        restored.contiguous_chosen(),
        5,
        "replay advances chosen frontier for contiguous accepted slots"
    );
    for (entry, key, value) in &entries {
        assert_eq!(
            restored.accepted_at(entry.slot).await,
            Some(entry.clone()),
            "accepted entry for slot {} survives",
            entry.slot
        );
        let got = restored.learner.engine_get(key).await;
        assert_eq!(
            got.map(|(_, v)| v),
            Some(value.clone()),
            "engine should have value after replay for {:?}",
            String::from_utf8_lossy(key)
        );
    }
}
