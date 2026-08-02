// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Group safe-slot computation: the published safe-slot is the minimum
//! contiguous-applied across the local replica and every voting peer that has
//! reported, and it only advances (never regresses) within a tenure.
//!
//! Drives the crate-internal peer-applied injection through the `test-util`
//! feature hook on `PxGroup`.

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::{PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::paxos::roles::{Learner, PxBallot, PxLogEntry, SlotIndex};

/// Drive the local learner's contiguous-applied watermark to `upto` by
/// learning slots `1..=upto` (empty `NoOp` payloads; only the frontier
/// matters here).
async fn apply_through(replica: &PxLocalReplica, upto: SlotIndex) {
    for slot in 1..=upto {
        replica
            .learner
            .learn(
                PxLogEntry {
                    slot,
                    ballot: PxBallot::new(0, 0),
                    term: 0,
                    payload: bytes::Bytes::new(),
                },
                &[],
            )
            .await;
    }
}

#[tokio::test]
async fn group_safe_slot_is_min_applied_across_voting_members() {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:2".to_string()));
    group.add_remote_replica(PxRemoteReplica::new(3, "127.0.0.1:3".to_string()));

    // Local replica is applied up to slot 5.
    apply_through(group.local_replica(), 5).await;

    // No peer has reported yet: an unheard peer counts as 0, so the
    // safe-slot must not advance past it.
    assert_eq!(group.group_safe_slot(), 0);

    // Peer 2 reports applied=4, but peer 3 is still silent -> stays 0.
    group.note_peer_applied_for_tests(2, 4);
    assert_eq!(group.group_safe_slot(), 0);

    // Peer 3 reports applied=3 -> min(local 5, p2 4, p3 3) = 3.
    group.note_peer_applied_for_tests(3, 3);
    assert_eq!(group.group_safe_slot(), 3);

    // Peer 3 catches up to 6 -> min(5, 4, 6) = 4.
    group.note_peer_applied_for_tests(3, 6);
    assert_eq!(group.group_safe_slot(), 4);

    // A peer regression cannot pull the published safe-slot backwards.
    group.note_peer_applied_for_tests(2, 1);
    assert_eq!(group.group_safe_slot(), 4);
}
