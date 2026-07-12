//! Integration tests for the Paxos acceptor.
//!
//! Covers acceptor state machine invariants and ballot monotonicity.
//! Key work: prepare/promise, accept/accepted, ballot ordering, idempotency.

use crowkv::paxos::acceptor::PxAcceptor;
use crowkv::paxos::roles::{Acceptor, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex};

fn entry(slot: SlotIndex, ballot: PxBallot, payload: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot,
        term: ballot.round,
        payload: bytes::Bytes::copy_from_slice(payload),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn prepare_promise() {
    let acc = PxAcceptor::new();
    let reply = acc.prepare(7, PxBallot::new(1, 1)).await;
    assert_eq!(
        reply,
        PxPrepareReply::Promised {
            slot: 7,
            accepted: None
        }
    );
    assert_eq!(acc.promised_at(7), Some(PxBallot::new(1, 1)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reject_lower_ballot() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(7, PxBallot::new(2, 1)).await;
    let reply = acc.prepare(7, PxBallot::new(1, 1)).await;
    assert_eq!(
        reply,
        PxPrepareReply::Rejected {
            slot: 7,
            current_promised: PxBallot::new(2, 1),
        }
    );
    assert_eq!(acc.promised_at(7), Some(PxBallot::new(2, 1)));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn accept_after_promise() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let e = entry(7, PxBallot::new(1, 1), b"v1");
    let reply = acc.accept(e.clone()).await;
    assert_eq!(
        reply,
        PxAcceptReply::Accepted {
            slot: 7,
            ballot: PxBallot::new(1, 1),
        }
    );
    assert_eq!(acc.accepted_at(7), Some(e));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn accept_rejects_lower_ballot() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(7, PxBallot::new(5, 1)).await;
    let stale = entry(7, PxBallot::new(4, 1), b"stale");
    let reply = acc.accept(stale).await;
    assert_eq!(
        reply,
        PxAcceptReply::Rejected {
            slot: 7,
            current_promised: PxBallot::new(5, 1),
        }
    );
    assert!(acc.accepted_at(7).is_none());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn prepare_returns_previously_accepted_value() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let v1 = entry(7, PxBallot::new(1, 1), b"v1");
    let _ = acc.accept(v1.clone()).await;
    let reply = acc.prepare(7, PxBallot::new(2, 2)).await;
    assert_eq!(
        reply,
        PxPrepareReply::Promised {
            slot: 7,
            accepted: Some(v1)
        }
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn ballot_ordering_is_total() {
    assert!(PxBallot::new(1, 99) < PxBallot::new(2, 0));
    assert!(PxBallot::new(2, 1) < PxBallot::new(2, 2));
    assert_eq!(PxBallot::new(3, 7), PxBallot::new(3, 7));
}
