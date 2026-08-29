// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Term fencing tests: `on_prepare` and `on_accept` reject stale-term
//! requests and adopt higher terms.
//!
//! These tests were moved from `replica/election_test.rs` to the election
//! unit binary since term fencing is election-state logic, not Paxos
//! acceptor logic.

use bytes::Bytes;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

fn write_entry(slot: u64, term: u64, payload: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 0),
        term,
        payload: Bytes::copy_from_slice(payload),
    }
}

// ── on_prepare term fencing ──────────────────────────────────

#[tokio::test]
async fn prepare_rejects_stale_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);

    let reply = replica.on_prepare(1, PxBallot::new(1, 1), 1).await;
    assert!(matches!(reply, PxPrepareReply::TermStale { new_term: 5, .. }));
}

#[tokio::test]
async fn prepare_adopts_higher_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    assert_eq!(replica.current_term_snapshot(), 0);

    let reply = replica.on_prepare(1, PxBallot::new(1, 1), 9).await;
    assert!(matches!(reply, PxPrepareReply::Promised { .. }));
    assert_eq!(replica.current_term_snapshot(), 9);
}

#[tokio::test]
async fn prepare_forwards_to_acceptor_on_equal_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(3);

    let reply = replica.on_prepare(1, PxBallot::new(1, 1), 3).await;
    assert!(
        matches!(reply, PxPrepareReply::Promised { .. }),
        "equal term → forward to acceptor"
    );
    assert_eq!(replica.current_term_snapshot(), 3, "term unchanged");
}

// ── on_accept term fencing ───────────────────────────────────

#[tokio::test]
async fn accept_rejects_stale_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);

    let stale = write_entry(1, 1, b"old");
    let reply = replica.on_accept(&stale).await;
    match reply {
        PxAcceptReply::TermStale { new_term, .. } => {
            assert_eq!(new_term, 5);
        }
        other => panic!("expected TermStale, got {other:?}"),
    }
}

#[tokio::test]
async fn accept_adopts_higher_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    assert_eq!(replica.current_term_snapshot(), 0);

    let higher = write_entry(1, 9, b"v");
    let reply = replica.on_accept(&higher).await;
    assert!(
        matches!(reply, PxAcceptReply::Accepted { .. }),
        "higher-term accept should proceed after adoption"
    );
    assert_eq!(replica.current_term_snapshot(), 9);
}

#[tokio::test]
async fn accept_forwards_on_equal_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(3);

    let entry = write_entry(1, 3, b"v");
    let reply = replica.on_accept(&entry).await;
    assert!(
        matches!(reply, PxAcceptReply::Accepted { .. }),
        "equal term → forward to acceptor"
    );
    assert_eq!(replica.current_term_snapshot(), 3, "term unchanged");
}
