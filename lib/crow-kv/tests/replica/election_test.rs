// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Step 12a: election-related unit tests.
//!
//! These cover gaps not already exercised by inline `#[cfg(test)]` tests
//! in `crow_kv/src/cluster/election.rs`. Inline migration is left for a
//! future pass per the workflow rule "migrate existing inline tests
//! when you next touch the file".

use crow_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crow_kv::cluster::replica::{ReplicaHandler, VoteRequestPayload};
use crow_kv::paxos::learner::PxLearner;
use crow_kv::paxos::roles::{Learner, PxAcceptReply, PxBallot, PxLogEntry};

/// A `NoOp` entry should advance the chosen / applied
/// watermarks without inserting any key into the store. (Bulk Phase-1
/// gap fill: a NoOp entry advances chosen/applied without a KV mutation.)
#[test]
fn noop_apply_path() {
    let learner = PxLearner::new();
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 0),
        term: 0,
        // Empty payload decodes to an empty batch — no Puts / Deletes
        // reach the engine.
        payload: bytes::Bytes::new(),
    };
    let before = learner.live_key_count();
    learner.learn(entry, &[]);
    let after = learner.live_key_count();
    assert_eq!(before, after, "NoOp must not mutate the KV store");
    assert_eq!(
        learner.contiguous_chosen(),
        1,
        "NoOp should advance contiguous_chosen"
    );
    assert_eq!(
        learner.contiguous_applied(),
        1,
        "NoOp should advance contiguous_applied alongside chosen"
    );
}

/// `PreVote` replies do not advance the candidate's `current_term`. Only
/// the `RequestVote` path (after a successful `PreVote` majority) does.
#[tokio::test]
async fn prevote_does_not_bump_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let term_before = replica.current_term_snapshot();
    let _ = <PxLocalReplica as ReplicaHandler>::on_pre_vote(
        &replica,
        VoteRequestPayload {
            term: term_before + 5,
            candidate_id: 2,
            accepted_log_tip_slot: 0,
            accepted_log_tip_term: 0,
        },
        1,
    )
    .await;
    assert_eq!(
        replica.current_term_snapshot(),
        term_before,
        "PreVote must never bump current_term"
    );
}

/// Step 8 acceptor fence: once a replica adopts a higher term, an
/// `Accept` with a strictly lower term reaches `PxAcceptReply::TermStale`
/// and is not applied.
#[tokio::test]
async fn term_fencing_in_acceptor_rejects_old_term() {
    let replica = PxLocalReplica::new(3, PxLocalReplicaRole::Follower);
    // Adopt term=5 via the follower transition.
    replica.become_follower(5);
    assert_eq!(replica.current_term_snapshot(), 5);

    // Now attempt an Accept stamped at term=1.
    let stale = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 0),
        term: 1,
        payload: bytes::Bytes::from_static(b"old"),
    };
    let reply = replica.on_accept(&stale).await;
    match reply {
        PxAcceptReply::TermStale { new_term, .. } => {
            assert_eq!(new_term, 5, "TermStale should report the replica's current_term");
        }
        other => panic!("expected TermStale, got {other:?}"),
    }
}

/// Same fence, but `term > current_term` adopts the new term and
/// forwards to the acceptor (i.e. accept proceeds, not rejected).
#[tokio::test]
async fn term_fencing_in_acceptor_adopts_higher_term() {
    let replica = PxLocalReplica::new(4, PxLocalReplicaRole::Follower);
    assert_eq!(replica.current_term_snapshot(), 0);

    let higher = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 0),
        term: 9,
        payload: bytes::Bytes::from_static(b"v"),
    };
    let reply = replica.on_accept(&higher).await;
    assert!(
        matches!(reply, PxAcceptReply::Accepted { .. }),
        "higher-term Accept should proceed after adoption: {reply:?}"
    );
    assert_eq!(
        replica.current_term_snapshot(),
        9,
        "current_term must be adopted from the higher-term Accept"
    );
}

#[tokio::test]
async fn request_vote_rejects_candidate_missing_higher_accepted_log_tip() {
    let voter = PxLocalReplica::new(7, PxLocalReplicaRole::Follower);
    let accepted = PxLogEntry {
        slot: 10,
        ballot: PxBallot::new(0, 7),
        term: 4,
        payload: bytes::Bytes::from_static(b"v"),
    };
    let reply = voter.on_accept(&accepted).await;
    assert!(matches!(reply, PxAcceptReply::Accepted { .. }));
    let term = voter.current_term_snapshot();

    let vote = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &voter,
        VoteRequestPayload {
            term,
            candidate_id: 9,
            accepted_log_tip_slot: 0,
            accepted_log_tip_term: 0,
        },
        1,
    )
    .await
    .expect("request vote reply");

    assert!(
        !vote.granted,
        "candidate missing higher accepted log tip must be rejected"
    );
}

#[tokio::test]
async fn request_vote_grants_candidate_with_matching_accepted_log_tip_even_if_learner_is_cold() {
    let voter = PxLocalReplica::new(8, PxLocalReplicaRole::Follower);
    let accepted = PxLogEntry {
        slot: 11,
        ballot: PxBallot::new(0, 8),
        term: 6,
        payload: bytes::Bytes::from_static(b"v2"),
    };
    let reply = voter.on_accept(&accepted).await;
    assert!(matches!(reply, PxAcceptReply::Accepted { .. }));
    assert_eq!(
        voter.last_chosen_slot(),
        0,
        "accepted-only state keeps learner cold"
    );
    let term = voter.current_term_snapshot();

    let vote = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &voter,
        VoteRequestPayload {
            term,
            candidate_id: 10,
            accepted_log_tip_slot: 11,
            accepted_log_tip_term: 6,
        },
        1,
    )
    .await
    .expect("request vote reply");

    assert!(
        vote.granted,
        "matching accepted log tip should satisfy up-to-date check"
    );
}
