//! Step 12a: election-related unit tests.
//!
//! These cover gaps not already exercised by inline `#[cfg(test)]` tests
//! in `crowkv/src/cluster/election.rs`. Inline migration is left for a
//! future pass per the workflow rule "migrate existing inline tests
//! when you next touch the file".

use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::replica::{ReplicaHandler, VoteRequestPayload};
use crowkv::paxos::learner::PxLearner;
use crowkv::paxos::roles::{Learner, PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind};

/// A `PxLogEntryKind::NoOp` entry should advance the chosen / applied
/// watermarks without inserting any key into the store. (See
/// `doc/design/design-leader-election.md` §4 bulk Phase-1 gap fill.)
#[test]
fn noop_apply_path() {
    let learner = PxLearner::new();
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 0),
        term: 0,
        kind: PxLogEntryKind::NoOp,
        // Empty payload triggers the `apply_payload` early return —
        // no Puts / Deletes are decoded.
        payload: bytes::Bytes::new(),
        client_id: None,
        seq: None,
    };
    let before = learner.store().len();
    learner.learn(entry);
    let after = learner.store().len();
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
            last_chosen_slot: 0,
            last_chosen_term: 0,
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
        kind: PxLogEntryKind::Write,
        payload: bytes::Bytes::from_static(b"old"),
        client_id: None,
        seq: None,
    };
    let reply = replica.on_accept(stale).await;
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
        kind: PxLogEntryKind::Write,
        payload: bytes::Bytes::from_static(b"v"),
        client_id: None,
        seq: None,
    };
    let reply = replica.on_accept(higher).await;
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
