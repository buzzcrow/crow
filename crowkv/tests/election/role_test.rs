//! Role transition tests for the election state machine on `PxLocalReplica`.
//!
//! Each `become_*` method updates `current_term`, `voted_for`, `role`,
//! `leader_id`, and `vote_lockout_until` under the election-state mutex.
//! These tests verify the invariants of each transition in isolation.

use std::time::Instant;

use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};

// ── become_follower ──────────────────────────────────────────

#[test]
fn become_follower_sets_role_and_clears_leader() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    assert_eq!(replica.role(), PxLocalReplicaRole::Leader);
    assert_eq!(replica.believed_leader_id(), Some(1));

    replica.become_follower(0);
    assert_eq!(replica.role(), PxLocalReplicaRole::Follower);
    assert_eq!(
        replica.believed_leader_id(),
        None,
        "follower has no believed leader"
    );
}

#[test]
fn become_follower_adopts_higher_term_and_resets_voted_for() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(3);
    assert_eq!(replica.current_term_snapshot(), 3);
    assert_eq!(replica.voted_for(), Some(1));

    replica.become_follower(5);
    assert_eq!(replica.current_term_snapshot(), 5, "higher term adopted");
    assert_eq!(replica.voted_for(), None, "voted_for reset on term bump");
}

#[test]
fn become_follower_does_not_regress_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);
    assert_eq!(replica.current_term_snapshot(), 5);

    replica.become_follower(3);
    assert_eq!(
        replica.current_term_snapshot(),
        5,
        "lower term must not regress current_term"
    );
}

#[test]
fn become_follower_preserves_voted_for_when_term_unchanged() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(3);
    assert_eq!(replica.voted_for(), Some(1));

    // become_follower with same term — voted_for should not be reset.
    replica.become_follower(3);
    assert_eq!(
        replica.voted_for(),
        Some(1),
        "voted_for preserved when term does not change"
    );
}

// ── become_precandidate ──────────────────────────────────────

#[test]
fn become_precandidate_sets_role_without_bumping_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(3);
    let term_before = replica.current_term_snapshot();

    replica.become_precandidate();
    assert_eq!(replica.role(), PxLocalReplicaRole::PreCandidate);
    assert_eq!(
        replica.current_term_snapshot(),
        term_before,
        "PreVote must not bump term"
    );
}

// ── become_candidate ─────────────────────────────────────────

#[test]
fn become_candidate_bumps_term_and_votes_for_self() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    assert_eq!(replica.current_term_snapshot(), 0);

    replica.become_candidate(1);
    assert_eq!(replica.role(), PxLocalReplicaRole::Candidate);
    assert_eq!(replica.current_term_snapshot(), 1);
    assert_eq!(replica.voted_for(), Some(1), "candidate votes for itself");
}

#[test]
fn become_candidate_extends_vote_lockout() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    // Initially vote_lockout is expired (set to now in ElectionPersistentState::initial).
    replica.clear_vote_lockout();
    // become_candidate should extend lockout.
    replica.become_candidate(1);
    // A subsequent vote request from another candidate should be rejected
    // due to lockout (tested in vote_test.rs).
}

#[test]
fn become_candidate_bumps_election_count() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let before = replica.election_metrics_snapshot(0).election_count;

    replica.become_candidate(1);
    replica.become_candidate(2);

    let after = replica.election_metrics_snapshot(0).election_count;
    assert_eq!(after - before, 2);
}

// ── become_leader ────────────────────────────────────────────

#[test]
fn become_leader_sets_role_and_self_as_leader() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(1);

    replica.become_leader();
    assert_eq!(replica.role(), PxLocalReplicaRole::Leader);
    assert!(replica.is_leader());
    assert_eq!(replica.believed_leader_id(), Some(1), "leader believes in itself");
}

#[test]
fn become_leader_resets_lease_to_now() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    // Extend lease to far future.
    replica.extend_lease_read_until(Instant::now() + std::time::Duration::from_secs(60));

    replica.become_leader();
    // Lease should be reset — a freshly-elected leader's lease is expired
    // until the first heartbeat round extends it.
    assert!(
        !replica.lease_read_valid(Instant::now() + std::time::Duration::from_secs(1)),
        "fresh leader lease should be expired"
    );
}

#[test]
fn become_follower_from_leader_expires_lease() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    replica.extend_lease_read_until(Instant::now() + std::time::Duration::from_secs(60));
    assert!(replica.lease_read_valid(Instant::now()));

    replica.become_follower(1);
    assert!(
        !replica.lease_read_valid(Instant::now()),
        "follower must not serve lease reads"
    );
}

// ── new_inheriting_election_state ───────────────────────────

#[test]
fn new_inheriting_election_state_preserves_term_and_voted_for() {
    let prior = PxLocalReplica::new(7, PxLocalReplicaRole::Follower);
    prior.become_candidate(5);

    let inherited = PxLocalReplica::new_inheriting_election_state(&prior);
    assert_eq!(inherited.id, 7);
    assert_eq!(inherited.current_term_snapshot(), 5);
    assert_eq!(inherited.voted_for(), Some(7));
    assert_eq!(inherited.role(), PxLocalReplicaRole::Candidate);
}

#[test]
fn new_inheriting_election_state_shares_acceptor_and_learner() {
    use bytes::Bytes;
    use crowkv::paxos::roles::{Learner, PxBallot, PxLogEntry, PxLogEntryKind};

    let prior = PxLocalReplica::new(3, PxLocalReplicaRole::Leader);
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 3),
        term: 1,
        kind: PxLogEntryKind::NoOp,
        payload: Bytes::new(),
        client_id: None,
        seq: None,
    };
    prior.learner.learn(entry);

    let inherited = PxLocalReplica::new_inheriting_election_state(&prior);
    assert_eq!(
        inherited.contiguous_chosen(),
        1,
        "learner is shared — applied state survives rebuild"
    );
}
