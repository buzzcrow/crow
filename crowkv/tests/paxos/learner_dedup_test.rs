//! Standalone unit tests for `PxLearner` dedup cache and watermark tracking.
//!
//! The existing `learner_test.rs` covers `note_chosen` (the peer-notice path).
//! These tests exercise the `learn` path (full payload apply) and the dedup
//! cache that `learn` populates, plus the contiguous-chosen / contiguous-applied
//! watermark advancement including out-of-order gap fill.

use bytes::Bytes;
use crowkv::paxos::learner::PxLearner;
use crowkv::paxos::roles::{Learner, PxBallot, PxLogEntry};

fn write_entry(slot: u64, payload: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 0),
        term: 1,
        payload: Bytes::copy_from_slice(payload),
    }
}

fn noop_entry(slot: u64) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 0),
        term: 1,
        payload: Bytes::new(),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1u8); // op_count
    buf.push(0u8); // Put
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

// ── Dedup cache ──────────────────────────────────────────────

#[test]
fn dedup_lookup_returns_none_for_fresh_client() {
    let learner = PxLearner::new();
    assert!(learner.dedup_lookup(1, 1).is_none());
}

#[test]
fn dedup_lookup_returns_none_for_client_id_zero() {
    let learner = PxLearner::new();
    // client_id == 0 is the "no client" sentinel and never dedups.
    let entry = write_entry(1, &encode_put(b"k", b"v"));
    learner.learn(entry, Some(0), Some(1));
}

#[test]
fn dedup_lookup_returns_slot_for_already_applied_seq() {
    let learner = PxLearner::new();
    let entry = write_entry(5, &encode_put(b"k", b"v"));
    learner.learn(entry, Some(42), Some(3));
    assert_eq!(
        learner.dedup_lookup(42, 3),
        Some(5),
        "exact seq match returns commit slot"
    );
    assert_eq!(
        learner.dedup_lookup(42, 2),
        Some(5),
        "lower seq also returns commit slot (already applied)"
    );
}

#[test]
fn dedup_lookup_returns_none_for_higher_seq() {
    let learner = PxLearner::new();
    let entry = write_entry(5, &encode_put(b"k", b"v"));
    learner.learn(entry, Some(42), Some(3));
    assert!(
        learner.dedup_lookup(42, 4).is_none(),
        "higher seq has not been applied yet"
    );
}

#[test]
fn dedup_tracks_highest_seq_per_client() {
    let learner = PxLearner::new();
    // Apply seq=1 at slot 1, then seq=5 at slot 3 for the same client.
    learner.learn(write_entry(1, &encode_put(b"a", b"1")), Some(7), Some(1));
    learner.learn(write_entry(3, &encode_put(b"a", b"5")), Some(7), Some(5));

    // seq=5 is the highest; lookup for seq <= 5 returns slot 3.
    assert_eq!(learner.dedup_lookup(7, 5), Some(3));
    assert_eq!(
        learner.dedup_lookup(7, 1),
        Some(3),
        "older seq maps to latest slot"
    );

    // A lower seq applied later does not regress the dedup record.
    learner.learn(write_entry(4, &encode_put(b"a", b"2")), Some(7), Some(2));
    assert_eq!(learner.dedup_lookup(7, 5), Some(3), "highest seq slot unchanged");
}

#[test]
fn dedup_is_per_client() {
    let learner = PxLearner::new();
    learner.learn(write_entry(1, &encode_put(b"k", b"v1")), Some(10), Some(1));
    learner.learn(write_entry(2, &encode_put(b"k", b"v2")), Some(20), Some(1));
    assert_eq!(learner.dedup_lookup(10, 1), Some(1));
    assert_eq!(learner.dedup_lookup(20, 1), Some(2));
}

#[test]
fn dedup_ignores_entries_without_client_id() {
    let learner = PxLearner::new();
    let entry = write_entry(1, &encode_put(b"k", b"v"));
    learner.learn(entry, None, None);
    // No client_id → no dedup entry.
    assert!(learner.dedup_lookup(1, 1).is_none());
}

// ── Contiguous watermark tracking ────────────────────────────

#[test]
fn learn_advances_contiguous_chosen_and_applied_in_order() {
    let learner = PxLearner::new();
    for slot in 1..=3u64 {
        learner.learn(write_entry(slot, &encode_put(b"k", b"v")), None, None);
    }
    assert_eq!(learner.contiguous_chosen(), 3);
    assert_eq!(learner.contiguous_applied(), 3);
}

#[test]
fn learn_out_of_order_does_not_advance_contiguous_until_gap_filled() {
    let learner = PxLearner::new();
    // Learn slot 3 before slot 1 and 2 — gap at 1 blocks contiguous.
    learner.learn(write_entry(3, &encode_put(b"k", b"v3")), None, None);
    assert_eq!(learner.contiguous_chosen(), 0);
    assert_eq!(learner.contiguous_applied(), 0);
    assert_eq!(learner.last_chosen_slot(), 3);

    // Fill slot 1 — contiguous jumps to 1, but 2 is still missing.
    learner.learn(write_entry(1, &encode_put(b"k", b"v1")), None, None);
    assert_eq!(learner.contiguous_chosen(), 1);

    // Fill slot 2 — contiguous drains the out-of-order map and jumps to 3.
    learner.learn(write_entry(2, &encode_put(b"k", b"v2")), None, None);
    assert_eq!(learner.contiguous_chosen(), 3);
    assert_eq!(learner.contiguous_applied(), 3);
}

#[test]
fn learn_is_idempotent_for_repeated_slot() {
    let learner = PxLearner::new();
    learner.learn(write_entry(1, &encode_put(b"k", b"first")), Some(1), Some(1));
    // Re-learn the same slot — watermarks must not advance past 1.
    learner.learn(write_entry(1, &encode_put(b"k", b"second")), Some(1), Some(1));
    assert_eq!(learner.contiguous_chosen(), 1);
    assert_eq!(learner.contiguous_applied(), 1);
}

#[test]
fn noop_entry_advances_watermark_without_mutating_kv() {
    let learner = PxLearner::new();
    learner.learn(write_entry(1, &encode_put(b"k", b"v")), None, None);
    learner.learn(noop_entry(2), None, None);
    assert_eq!(learner.contiguous_chosen(), 2);
    assert_eq!(learner.contiguous_applied(), 2);
    assert_eq!(learner.live_key_count(), 1, "NoOp must not add keys");
}

#[test]
fn last_chosen_term_tracks_latest_learned_slot() {
    let learner = PxLearner::new();
    let mut entry = write_entry(1, &encode_put(b"k", b"v"));
    entry.term = 5;
    learner.learn(entry, Some(42), Some(3));
    assert_eq!(learner.last_chosen_term(), 5);

    let mut entry2 = write_entry(2, &encode_put(b"k", b"v2"));
    entry2.term = 9;
    learner.learn(entry2, None, None);
    assert_eq!(learner.last_chosen_slot(), 2);
    assert_eq!(learner.last_chosen_term(), 9);
}
