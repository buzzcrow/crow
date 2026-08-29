// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `PxLearner::note_chosen` (Step 10.6a) — peer-side `ChosenNotice`
//! receipt should advance `last_chosen_slot` / `last_chosen_term` only,
//! never the contiguous watermarks.

use crowdb_kv::paxos::learner::PxLearner;

#[test]
fn note_chosen_advances_high_water_mark() {
    let learner = PxLearner::new();
    assert_eq!(learner.last_chosen_slot(), 0);
    assert_eq!(learner.last_chosen_term(), 0);

    let advanced = learner.note_chosen(7, 3);
    assert!(advanced);
    assert_eq!(learner.last_chosen_slot(), 7);
    assert_eq!(learner.last_chosen_term(), 3);
}

#[test]
fn note_chosen_does_not_advance_for_lower_or_equal_slot() {
    let learner = PxLearner::new();
    assert!(learner.note_chosen(5, 1));
    assert!(!learner.note_chosen(5, 2), "equal slot: no advance");
    assert!(!learner.note_chosen(3, 9), "lower slot: no advance");
    assert_eq!(learner.last_chosen_slot(), 5);
    // Term not bumped because the slot did not advance.
    assert_eq!(learner.last_chosen_term(), 1);
}

#[test]
fn note_chosen_does_not_touch_contiguous_watermarks() {
    let learner = PxLearner::new();
    assert!(learner.note_chosen(10, 4));
    // Contiguous watermarks remain at 0 because no payload was applied.
    assert_eq!(learner.contiguous_chosen(), 0);
    assert_eq!(learner.contiguous_applied(), 0);
}

#[test]
fn note_chosen_is_idempotent_under_concurrent_callers() {
    use std::sync::Arc;
    use std::thread;

    let learner = Arc::new(PxLearner::new());
    let mut handles = Vec::new();
    for s in 1..=64 {
        let l = learner.clone();
        handles.push(thread::spawn(move || {
            l.note_chosen(s, 1);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(learner.last_chosen_slot(), 64);
    assert_eq!(learner.last_chosen_term(), 1);
}
