//! Proposer sliding-window admission and background-repair tests.
//!
//! These drive crate-internal mechanisms (the proposer-window semaphore and a
//! single repair step) through the `test-util` feature hooks on `PxGroup`,
//! using a single-voter leader group so quorum is 1 and no peer RPCs are
//! needed.

use crowkv::cluster::group::{ProposeResult, PxGroup};
use crowkv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::PaxosConfig;
use crowkv::paxos::roles::{Learner, PxBallot, PxLogEntry, PxLogEntryKind};

/// Single-voter leader group: quorum is 1, so propose / repair complete
/// against the local acceptor with no peer RPCs.
fn single_leader_group() -> PxGroup {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    PxGroup::new(1, local)
}

#[tokio::test]
async fn propose_returns_busy_when_window_is_full() {
    let group = single_leader_group();

    // Exhaust every window permit so the next admission must fail fast.
    let mut held = Vec::new();
    for _ in 0..PaxosConfig::DEFAULT.proposer_window {
        held.push(group.proposer_window().try_acquire().expect("window permit"));
    }
    match group.propose(b"v".to_vec(), Some(1), Some(1)).await {
        ProposeResult::Busy => {}
        other => panic!("expected Busy with a full window, got {other:?}"),
    }

    // Releasing the permits reopens admission.
    held.clear();
    match group.propose(b"v".to_vec(), Some(1), Some(2)).await {
        ProposeResult::Chosen { .. } => {}
        other => panic!("expected Chosen after window drained, got {other:?}"),
    }
}

#[tokio::test]
async fn repair_once_fills_gap_and_advances_frontier() {
    let group = single_leader_group();

    // Learn slot 2 directly, leaving slot 1 as an abandoned gap: the
    // contiguous frontier stays at 0 while the highest-seen slot is 2.
    group.local_replica().learner.learn(PxLogEntry {
        slot: 2,
        ballot: PxBallot::new(1, 1),
        term: 0,
        kind: PxLogEntryKind::Write,
        payload: bytes::Bytes::from_static(b""),
        client_id: None,
        seq: None,
    });
    assert_eq!(group.local_replica().contiguous_chosen(), 0, "gap below slot 2");
    assert_eq!(group.local_replica().last_chosen_slot(), 2);

    // One repair step closes slot 1; the frontier then drains through the
    // already-learned slot 2.
    assert_eq!(
        group.repair_once_for_tests().await,
        Some(1),
        "repair should fill the gap at slot 1"
    );
    assert_eq!(
        group.local_replica().contiguous_chosen(),
        2,
        "frontier advances past the filled gap and the trailing learned slot"
    );

    // A second repair has nothing to do.
    assert_eq!(
        group.repair_once_for_tests().await,
        None,
        "no gap remains, so repair is a no-op"
    );
}
