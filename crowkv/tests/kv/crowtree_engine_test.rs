//! `CrowtreeEngine` behavior: same shared `KVEngine` conformance suite as
//! `mem_kv_test.rs` (see `conformance.rs`), proving `CrowtreeEngine` and
//! `InMemKV` satisfy the identical `KVEngine` contract.

use crowkv::kv::{CrowtreeEngine, CrowtreeOptions};

use super::conformance;

fn open() -> CrowtreeEngine {
    CrowtreeEngine::open(&CrowtreeOptions::default()).unwrap()
}

#[test]
fn highest_slot_wins_regardless_of_apply_order() {
    conformance::highest_slot_wins_regardless_of_apply_order(&open());
}

#[test]
fn equal_slot_is_idempotent_noop() {
    conformance::equal_slot_is_idempotent_noop(&open());
}

#[test]
fn delete_writes_tombstone() {
    conformance::delete_writes_tombstone(&open());
}

#[test]
fn intra_batch_last_occurrence_wins() {
    conformance::intra_batch_last_occurrence_wins(&open());
}

#[test]
fn scan_is_ordered_prefix_filtered_and_truncates() {
    conformance::scan_is_ordered_prefix_filtered_and_truncates(&open());
}

#[test]
fn compare_is_empty_for_identical_state_and_detects_divergence() {
    conformance::compare_is_empty_for_identical_state_and_detects_divergence(&open(), &open());
}

/// Cross-engine parity (design's strongest correctness gate): apply the same
/// op stream to `InMemKV` and `CrowtreeEngine`, `compare()` must be empty.
#[test]
fn parity_with_in_mem_kv_after_identical_op_stream() {
    use crowkv::kv::{InMemKV, KVEngine};

    let mem = InMemKV::new();
    let ct = open();
    let mut slot = 0u64;
    for round in 0..20u32 {
        slot += 1;
        let key = format!("key{:03}", round % 7).into_bytes();
        let b = if round % 5 == 4 {
            conformance::batch(vec![conformance::del(&key)])
        } else {
            let value = format!("v{round}").into_bytes();
            conformance::batch(vec![conformance::put(&key, &value)])
        };
        mem.apply(slot, &b);
        ct.apply(slot, &b);
        assert!(mem.compare(&ct).is_empty(), "diverged after round {round}");
    }
}
