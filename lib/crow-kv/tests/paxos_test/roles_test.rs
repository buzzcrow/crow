// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Edge-case unit tests for `PxLogEntry` / `PxBallot`.
//!
//! The acceptor tests touch ballot ordering incidentally; these tests exercise
//! the type surface directly: ballot total ordering and entry equality
//! semantics.

use bytes::Bytes;
use crow_kv::paxos::roles::{PxBallot, PxLogEntry};

// ── PxBallot ordering ────────────────────────────────────────

#[test]
fn ballot_orders_by_round_first_then_leader_id() {
    assert!(PxBallot::new(0, 99) < PxBallot::new(1, 0));
    assert!(PxBallot::new(1, 0) < PxBallot::new(1, 1));
    assert!(PxBallot::new(2, 5) < PxBallot::new(3, 0));
}

#[test]
fn ballot_equal_when_round_and_leader_id_match() {
    assert_eq!(PxBallot::new(3, 7), PxBallot::new(3, 7));
    assert_ne!(PxBallot::new(3, 7), PxBallot::new(3, 8));
    assert_ne!(PxBallot::new(3, 7), PxBallot::new(4, 7));
}

#[test]
fn ballot_zero_is_the_minimum() {
    assert!(PxBallot::new(0, 0) <= PxBallot::new(0, 0));
    assert!(PxBallot::new(0, 0) < PxBallot::new(0, 1));
    assert!(PxBallot::new(0, 0) < PxBallot::new(1, 0));
}

#[test]
fn ballot_tie_break_uses_leader_id() {
    // Same round, different leader_id: lower leader_id wins.
    let a = PxBallot::new(5, 1);
    let b = PxBallot::new(5, 2);
    assert!(a < b);
    assert!(b > a);
}

// ── PxLogEntry equality ──────────────────────────────────────

fn entry(slot: u64, payload: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, 1),
        term: 1,
        payload: Bytes::copy_from_slice(payload),
    }
}

#[test]
fn log_entry_equality_compares_all_fields() {
    let a = entry(1, b"v");
    let b = entry(1, b"v");
    assert_eq!(a, b);

    // Different slot.
    assert_ne!(a, entry(2, b"v"));
    // Different payload.
    assert_ne!(a, entry(1, b"w"));
    // Different ballot.
    assert_ne!(
        a,
        PxLogEntry {
            ballot: PxBallot::new(2, 1),
            ..a.clone()
        }
    );
    // Different term.
    assert_ne!(a, PxLogEntry { term: 2, ..a.clone() });
}

#[test]
fn noop_entry_has_empty_payload() {
    let e = entry(1, &[]);
    assert!(e.payload.is_empty());
}

#[test]
fn write_entry_carries_kv_payload() {
    let e = entry(1, b"some-payload");
    assert!(!e.payload.is_empty());
    assert_eq!(e.payload.as_ref(), b"some-payload");
}
