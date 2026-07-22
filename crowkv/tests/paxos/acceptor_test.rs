// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

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
    let reply = acc.accept(&e).await;
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
    let reply = acc.accept(&stale).await;
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
    let _ = acc.accept(&v1).await;
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn equal_ballot_accept_is_idempotent() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let e1 = entry(7, PxBallot::new(1, 1), b"v1");
    let reply1 = acc.accept(&e1).await;
    assert_eq!(
        reply1,
        PxAcceptReply::Accepted {
            slot: 7,
            ballot: PxBallot::new(1, 1),
        }
    );
    // Second accept with the same ballot must also return Accepted (C2 invariant).
    let e2 = entry(7, PxBallot::new(1, 1), b"v2");
    let reply2 = acc.accept(&e2).await;
    assert_eq!(
        reply2,
        PxAcceptReply::Accepted {
            slot: 7,
            ballot: PxBallot::new(1, 1),
        }
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn accept_without_prior_prepare() {
    let acc = PxAcceptor::new();
    let e = entry(7, PxBallot::new(3, 2), b"v1");
    let reply = acc.accept(&e).await;
    assert_eq!(
        reply,
        PxAcceptReply::Accepted {
            slot: 7,
            ballot: PxBallot::new(3, 2),
        }
    );
    assert_eq!(acc.accepted_at(7), Some(e));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn multi_slot_isolation() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let e7 = entry(7, PxBallot::new(1, 1), b"v7");
    let _ = acc.accept(&e7).await;

    let _ = acc.prepare(8, PxBallot::new(2, 2)).await;
    let e8 = entry(8, PxBallot::new(2, 2), b"v8");
    let _ = acc.accept(&e8).await;

    // Slot 7 state is untouched by slot 8 operations.
    assert_eq!(acc.promised_at(7), Some(PxBallot::new(1, 1)));
    assert_eq!(acc.accepted_at(7), Some(e7));
    // Slot 8 state is independent.
    assert_eq!(acc.promised_at(8), Some(PxBallot::new(2, 2)));
    assert_eq!(acc.accepted_at(8), Some(e8));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn highest_seen_slot_is_monotonic_max() {
    let acc = PxAcceptor::new();
    let _ = acc.prepare(3, PxBallot::new(1, 1)).await;
    let _ = acc.prepare(7, PxBallot::new(1, 1)).await;
    let _ = acc.prepare(5, PxBallot::new(1, 1)).await;
    assert_eq!(acc.highest_seen_slot(), 7);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn accepted_log_tip_returns_highest_accepted_slot() {
    let acc = PxAcceptor::new();
    // No accepts yet.
    assert!(acc.accepted_log_tip().is_none());

    let _ = acc.accept(&entry(1, PxBallot::new(1, 1), b"v1")).await;
    let _ = acc.accept(&entry(3, PxBallot::new(1, 1), b"v3")).await;
    let _ = acc.accept(&entry(5, PxBallot::new(1, 1), b"v5")).await;

    let tip = acc.accepted_log_tip();
    assert_eq!(tip, Some((5, 1)));
}
