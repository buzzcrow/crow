//! Integration tests for the Paxos acceptor.
//!
//! Mirrors `doc/test-design-consensus.md` §2 rows plus invariant C2 cases.

mod testkit;

use crowkv::kv::types::{LogEntryKind, PxLogEntry};
use crowkv::paxos::acceptor::{AcceptReply, Acceptor, PrepareReply};
use crowkv::paxos::types::{PxBallot, PxSlot};

fn entry(slot: PxSlot, ballot: PxBallot, payload: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot,
        term: ballot.round,
        kind: LogEntryKind::Write,
        payload: payload.to_vec(),
        client_id: Some(1),
        seq: Some(1),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn prepare_promise() {
    let mut acc = Acceptor::new();
    let reply = acc.prepare(7, PxBallot::new(1, 1)).await;
    assert_eq!(
        reply,
        PrepareReply::Promised {
            slot: 7,
            accepted: None,
        }
    );
    assert_eq!(acc.promised_at(7), Some(PxBallot::new(1, 1)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reject_lower_ballot() {
    let mut acc = Acceptor::new();
    let _ = acc.prepare(7, PxBallot::new(2, 1)).await;
    let reply = acc.prepare(7, PxBallot::new(1, 1)).await;
    assert_eq!(
        reply,
        PrepareReply::Rejected {
            slot: 7,
            current_promised: PxBallot::new(2, 1),
        }
    );
    assert_eq!(acc.promised_at(7), Some(PxBallot::new(2, 1)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn accept_after_promise() {
    let mut acc = Acceptor::new();
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let e = entry(7, PxBallot::new(1, 1), b"v1");
    let reply = acc.accept(e.clone()).await;
    assert_eq!(
        reply,
        AcceptReply::Accepted {
            slot: 7,
            ballot: PxBallot::new(1, 1),
        }
    );
    assert_eq!(acc.accepted_at(7), Some(&e));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn accept_rejects_lower_ballot() {
    let mut acc = Acceptor::new();
    let _ = acc.prepare(7, PxBallot::new(5, 1)).await;
    let stale = entry(7, PxBallot::new(4, 1), b"stale");
    let reply = acc.accept(stale).await;
    assert_eq!(
        reply,
        AcceptReply::Rejected {
            slot: 7,
            current_promised: PxBallot::new(5, 1),
        }
    );
    assert!(acc.accepted_at(7).is_none());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn prepare_returns_previously_accepted_value() {
    let mut acc = Acceptor::new();
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let v1 = entry(7, PxBallot::new(1, 1), b"v1");
    let _ = acc.accept(v1.clone()).await;
    let reply = acc.prepare(7, PxBallot::new(2, 2)).await;
    assert_eq!(
        reply,
        PrepareReply::Promised {
            slot: 7,
            accepted: Some(v1),
        }
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn ballot_ordering_is_total() {
    assert!(PxBallot::new(1, 99) < PxBallot::new(2, 0));
    assert!(PxBallot::new(2, 1) < PxBallot::new(2, 2));
    assert_eq!(PxBallot::new(3, 7), PxBallot::new(3, 7));
}
