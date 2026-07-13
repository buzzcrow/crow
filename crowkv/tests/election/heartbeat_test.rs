// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Heartbeat handler tests for `PxLocalReplica`.
//!
//! `handle_heartbeat` is the follower-side handler for leader heartbeats.
//! It adopts a higher term, records the leader id, extends vote lockout,
//! and applies committed entries up to the leader's commit point.

use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::replica::{HeartbeatRequestPayload, ReplicaHandler};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};

fn heartbeat(term: u64, leader_id: u64, commit_slot: u64) -> HeartbeatRequestPayload {
    HeartbeatRequestPayload {
        term,
        leader_id,
        prev_log_slot: 0,
        prev_log_term: 0,
        committed_safe_slot: commit_slot,
        lease_grant_until_ms_mono: 0,
        t_send_ms_mono: 0,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn write_entry(slot: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    let mut payload = Vec::new();
    payload.push(1u8); // op_count
    payload.push(0u8); // Put
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: bytes::Bytes::from(payload),
    }
}

#[tokio::test]
async fn heartbeat_adopts_higher_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    assert_eq!(replica.current_term_snapshot(), 0);

    let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(5, 2, 0), 1)
        .await
        .expect("heartbeat reply");

    assert!(reply.success);
    assert_eq!(replica.current_term_snapshot(), 5, "term adopted from heartbeat");
}

#[tokio::test]
async fn heartbeat_records_leader_id() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 7, 0), 1)
        .await
        .expect("reply");

    assert_eq!(replica.believed_leader_id(), Some(7));
    assert_eq!(replica.role(), PxLocalReplicaRole::Follower);
}

#[tokio::test]
async fn heartbeat_rejects_lower_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);

    let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(3, 2, 0), 1)
        .await
        .expect("reply");

    assert!(!reply.success, "lower term heartbeat → reject");
    assert_eq!(reply.term, 5, "reply reports current term");
    assert_eq!(
        replica.believed_leader_id(),
        None,
        "leader id not updated on reject"
    );
}

#[tokio::test]
async fn heartbeat_accepts_equal_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(3);

    let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(3, 2, 0), 1)
        .await
        .expect("reply");

    assert!(reply.success, "equal term heartbeat → accept");
    assert_eq!(replica.believed_leader_id(), Some(2));
}

#[tokio::test]
async fn heartbeat_applies_committed_entries_up_to_commit_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Accept entries at slots 1–3 but don't learn them (follower path).
    for slot in 1..=3u64 {
        let entry = write_entry(slot, b"k", &format!("v{slot}").into_bytes());
        let _ = replica.on_accept(entry).await;
    }

    // Heartbeat with commit_slot=3 should apply slots 1–3.
    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 3), 1)
        .await
        .expect("reply");

    assert_eq!(
        replica.contiguous_applied(),
        3,
        "follower applies up to commit_slot"
    );
    assert_eq!(
        replica.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"v3".to_vec()),
        "latest value applied"
    );
}

#[tokio::test]
async fn heartbeat_does_not_apply_beyond_accepted_log() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Accept only slot 1.
    let entry = write_entry(1, b"k", b"v1");
    let _ = replica.on_accept(entry).await;

    // Heartbeat claims commit_slot=5 but we only have slot 1.
    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 5), 1)
        .await
        .expect("reply");

    assert_eq!(
        replica.contiguous_applied(),
        1,
        "applied up to the first hole in accepted log"
    );
}

#[tokio::test]
async fn heartbeat_idempotent_for_repeated_commit_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let entry = write_entry(1, b"k", b"v1");
    let _ = replica.on_accept(entry).await;

    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 1), 1)
        .await
        .expect("reply");
    assert_eq!(replica.contiguous_applied(), 1);

    // Repeated heartbeat with same commit_slot — no change.
    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 1), 1)
        .await
        .expect("reply");
    assert_eq!(replica.contiguous_applied(), 1);
}

#[tokio::test]
async fn heartbeat_from_leader_stepping_down_via_higher_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    replica.become_leader();
    assert!(replica.is_leader());

    // A heartbeat from a higher-term leader should step us down.
    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(10, 2, 0), 1)
        .await
        .expect("reply");

    assert_eq!(replica.role(), PxLocalReplicaRole::Follower, "stepped down");
    assert_eq!(replica.current_term_snapshot(), 10);
    assert_eq!(replica.believed_leader_id(), Some(2));
}
