//! Edge-case unit tests for `PxLogEntry` / `PxBallot` / `PxLogEntryKind`.
//!
//! The acceptor tests touch ballot ordering incidentally; these tests exercise
//! the type surface directly: ballot total ordering, kind discriminants, and
//! entry equality semantics.

use bytes::Bytes;
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};

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

// ── PxLogEntryKind discriminants ─────────────────────────────

#[test]
fn log_entry_kind_equality() {
    assert_eq!(PxLogEntryKind::Write, PxLogEntryKind::Write);
    assert_eq!(PxLogEntryKind::NoOp, PxLogEntryKind::NoOp);
    assert_eq!(PxLogEntryKind::ConfigChange, PxLogEntryKind::ConfigChange);
    assert_eq!(PxLogEntryKind::DedupCheckpoint, PxLogEntryKind::DedupCheckpoint);
    assert_ne!(PxLogEntryKind::Write, PxLogEntryKind::NoOp);
    assert_ne!(PxLogEntryKind::Write, PxLogEntryKind::ConfigChange);
    assert_ne!(PxLogEntryKind::Write, PxLogEntryKind::DedupCheckpoint);
    assert_ne!(PxLogEntryKind::NoOp, PxLogEntryKind::ConfigChange);
    assert_ne!(PxLogEntryKind::NoOp, PxLogEntryKind::DedupCheckpoint);
    assert_ne!(PxLogEntryKind::ConfigChange, PxLogEntryKind::DedupCheckpoint);
}

// ── PxLogEntry equality ──────────────────────────────────────

fn entry(
    slot: u64,
    kind: PxLogEntryKind,
    payload: &[u8],
    client_id: Option<u64>,
    seq: Option<u64>,
) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, 1),
        term: 1,
        kind,
        payload: Bytes::copy_from_slice(payload),
        client_id,
        seq,
    }
}

#[test]
fn log_entry_equality_compares_all_fields() {
    let a = entry(1, PxLogEntryKind::Write, b"v", Some(1), Some(1));
    let b = entry(1, PxLogEntryKind::Write, b"v", Some(1), Some(1));
    assert_eq!(a, b);

    // Different slot.
    assert_ne!(a, entry(2, PxLogEntryKind::Write, b"v", Some(1), Some(1)));
    // Different kind.
    assert_ne!(a, entry(1, PxLogEntryKind::NoOp, b"v", Some(1), Some(1)));
    // Different payload.
    assert_ne!(a, entry(1, PxLogEntryKind::Write, b"w", Some(1), Some(1)));
    // Different client_id.
    assert_ne!(a, entry(1, PxLogEntryKind::Write, b"v", Some(2), Some(1)));
    // Different seq.
    assert_ne!(a, entry(1, PxLogEntryKind::Write, b"v", Some(1), Some(2)));
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
    let e = entry(1, PxLogEntryKind::NoOp, &[], None, None);
    assert!(e.payload.is_empty());
}

#[test]
fn write_entry_carries_kv_payload() {
    let e = entry(1, PxLogEntryKind::Write, b"some-payload", Some(1), Some(1));
    assert!(!e.payload.is_empty());
    assert_eq!(e.payload.as_ref(), b"some-payload");
}

#[test]
fn config_change_entry_kind_is_distinct() {
    let e = entry(1, PxLogEntryKind::ConfigChange, b"cfg", None, None);
    assert_eq!(e.kind, PxLogEntryKind::ConfigChange);
    assert_ne!(e.kind, PxLogEntryKind::Write);
}

#[test]
fn dedup_checkpoint_entry_kind_is_distinct() {
    let e = entry(1, PxLogEntryKind::DedupCheckpoint, b"ckpt", None, None);
    assert_eq!(e.kind, PxLogEntryKind::DedupCheckpoint);
    assert_ne!(e.kind, PxLogEntryKind::Write);
}
