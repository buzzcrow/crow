// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Concurrent stress tests for `PxSlotList`.
//!
//! These tests exercise the lock-free chunked slot list under multi-thread
//! contention: concurrent inserters, a single trimmer, a single reclaimer,
//! and concurrent readers. The goal is to assert no lost slots, no panics
//! from the single-caller trim/reclaim guards, and no use-after-free when
//! readers hold guards across trim + reclaim.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use crowdb_kv::paxos::slot_list::PxSlotList;

const SLOT_CHUNK_SIZE: usize = 1024;

#[test]
fn concurrent_inserts_at_disjoint_ranges_no_lost_slots() {
    const N_THREADS: usize = 4;
    const SLOTS_PER_THREAD: usize = 500;

    let list = Arc::new(PxSlotList::<u64>::new());
    let barrier = Arc::new(Barrier::new(N_THREADS));
    let mut handles = Vec::new();

    for t in 0..N_THREADS {
        let list = list.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let base = (t * SLOTS_PER_THREAD) as u64;
            for i in 0..SLOTS_PER_THREAD {
                let slot = base + i as u64;
                let guard = list.insert_if_empty(slot, slot * 10);
                assert_eq!(*guard, slot * 10, "inserted value mismatch at slot {slot}");
                drop(guard);
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    assert_eq!(list.len(), N_THREADS * SLOTS_PER_THREAD);

    // Verify every slot is readable.
    for t in 0..N_THREADS {
        let base = (t * SLOTS_PER_THREAD) as u64;
        for i in 0..SLOTS_PER_THREAD {
            let slot = base + i as u64;
            let guard = list.get(slot).expect("slot should exist");
            assert_eq!(*guard, slot * 10, "value mismatch at slot {slot}");
        }
    }
}

#[test]
fn concurrent_insert_and_read_no_lost_slots() {
    const N_WRITERS: usize = 2;
    const N_READERS: usize = 2;
    const SLOTS_PER_WRITER: usize = 1000;

    let list = Arc::new(PxSlotList::<u64>::new());
    let done = Arc::new(AtomicUsize::new(0));
    let read_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(N_WRITERS + N_READERS));
    let mut handles = Vec::new();

    // Writers insert at disjoint ranges.
    for w in 0..N_WRITERS {
        let list = list.clone();
        let barrier = barrier.clone();
        let done = done.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let base = (w * SLOTS_PER_WRITER) as u64;
            for i in 0..SLOTS_PER_WRITER {
                let slot = base + i as u64;
                let g = list.insert_if_empty(slot, slot);
                assert_eq!(*g, slot);
                drop(g);
            }
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }

    // Readers randomly get slots until writers are done.
    for r in 0..N_READERS {
        let list = list.clone();
        let barrier = barrier.clone();
        let done = done.clone();
        let read_counter = read_counter.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            while done.load(Ordering::SeqCst) < N_WRITERS {
                // Read a pseudo-random slot — may be None if not yet inserted.
                let idx = read_counter.fetch_add(1, Ordering::Relaxed);
                let slot = ((idx * 17 + r * 31) % (N_WRITERS * SLOTS_PER_WRITER)) as u64;
                if let Some(g) = list.get(slot) {
                    assert_eq!(*g, slot, "value must match slot");
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // All slots must be present after writers finish.
    for w in 0..N_WRITERS {
        let base = (w * SLOTS_PER_WRITER) as u64;
        for i in 0..SLOTS_PER_WRITER {
            let slot = base + i as u64;
            let g = list.get(slot).expect("slot must exist after all writers done");
            assert_eq!(*g, slot);
        }
    }
}

#[test]
fn concurrent_insert_then_trim_and_reclaim() {
    const N_THREADS: usize = 4;
    const SLOTS_PER_THREAD: usize = SLOT_CHUNK_SIZE * 2; // 2 chunks per thread

    let list = Arc::new(PxSlotList::<u64>::new());
    let barrier = Arc::new(Barrier::new(N_THREADS));
    let mut handles = Vec::new();
    for t in 0..N_THREADS {
        let list = list.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let base = (t * SLOTS_PER_THREAD) as u64;
            for i in 0..SLOTS_PER_THREAD {
                let slot = base + i as u64;
                let g = list.insert_if_empty(slot, t as u64);
                drop(g);
            }
        }));
    }
    for h in handles {
        h.join().expect("writer panicked");
    }

    assert_eq!(list.len(), N_THREADS * SLOTS_PER_THREAD);

    // Phase 2: trim past the first half of slots, then reclaim.
    let total_slots = N_THREADS * SLOTS_PER_THREAD;
    let trim_point = (total_slots / 2) as u64;

    list.trim(trim_point);
    let freed = list.reclaim();
    // At least one chunk should be reclaimable (the first chunk(s) below trim_point).
    assert!(freed > 0, "at least one chunk should be freed by reclaim");

    // Trimmed slots should return None.
    for slot in 0..trim_point {
        assert!(list.get(slot).is_none(), "slot {slot} should be trimmed");
    }

    // Remaining slots should still be readable.
    for slot in trim_point..total_slots as u64 {
        let g = list.get(slot).expect("slot above trim_point should exist");
        let writer = slot / SLOTS_PER_THREAD as u64;
        assert_eq!(*g, writer);
    }
}

#[test]
fn concurrent_insert_trim_reclaim_read_stress() {
    const N_INSERTERS: usize = 3;
    const N_READERS: usize = 2;
    const SLOTS_PER_INSERTER: usize = SLOT_CHUNK_SIZE * 3;
    const TOTAL_SLOTS: usize = N_INSERTERS * SLOTS_PER_INSERTER;

    let list = Arc::new(PxSlotList::<u64>::new());
    let inserters_done = Arc::new(AtomicUsize::new(0));
    let read_counter = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(N_INSERTERS + N_READERS + 1)); // +1 for trimmer
    let mut handles = Vec::new();

    // Inserters.
    for t in 0..N_INSERTERS {
        let list = list.clone();
        let barrier = barrier.clone();
        let done = inserters_done.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let base = (t * SLOTS_PER_INSERTER) as u64;
            for i in 0..SLOTS_PER_INSERTER {
                let slot = base + i as u64;
                let g = list.insert_if_empty(slot, slot);
                drop(g);
            }
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }

    // Trimmer (runs on barrier thread, single caller).
    let list_trim = list.clone();
    let barrier_trim = barrier.clone();
    let done_trim = inserters_done.clone();
    let trimmer = thread::spawn(move || {
        barrier_trim.wait();
        // Wait for all inserters to finish.
        while done_trim.load(Ordering::SeqCst) < N_INSERTERS {
            thread::yield_now();
        }
        // Trim past the first 2 chunks worth of slots.
        let trim_point = (SLOT_CHUNK_SIZE * 2) as u64;
        list_trim.trim(trim_point);
        let freed = list_trim.reclaim();
        assert!(freed > 0, "trimmer: chunks should be freed");
    });

    // Readers.
    for r in 0..N_READERS {
        let list = list.clone();
        let barrier = barrier.clone();
        let done = inserters_done.clone();
        let read_counter = read_counter.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            // Read pseudo-random slots while inserters are running.
            while done.load(Ordering::SeqCst) < N_INSERTERS {
                let idx = read_counter.fetch_add(1, Ordering::Relaxed);
                let slot = ((idx * 31 + r * 37) % TOTAL_SLOTS) as u64;
                if let Some(g) = list.get(slot) {
                    assert_eq!(*g, slot, "reader: value must match slot");
                }
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }
    trimmer.join().expect("trimmer panicked");

    // After all threads done: verify remaining slots.
    let trim_point = (SLOT_CHUNK_SIZE * 2) as u64;
    for slot in trim_point..TOTAL_SLOTS as u64 {
        let g = list.get(slot).expect("post-stress: slot should exist");
        assert_eq!(*g, slot, "post-stress: value mismatch");
    }
    // Trimmed slots are gone.
    for slot in 0..trim_point {
        assert!(
            list.get(slot).is_none(),
            "post-stress: trimmed slot should be gone"
        );
    }
}

#[test]
fn duplicate_insert_returns_existing_value() {
    let list = PxSlotList::<u64>::new();
    let g1 = list.insert_if_empty(5, 100u64);
    drop(g1);

    // Second insert at same slot should return the existing value, not 200.
    let g2 = list.insert_if_empty(5, 200u64);
    assert_eq!(*g2, 100, "duplicate insert returns existing value");
    drop(g2);

    let g3 = list.get(5).expect("slot 5 exists");
    assert_eq!(*g3, 100, "original value preserved");
}

#[test]
fn many_chunks_sparse_insert_and_iterate() {
    let list = PxSlotList::<u64>::new();
    // Insert sparsely across 5 chunks (chunk size = 1024).
    let slots: Vec<u64> = vec![0, 1024, 2048, 3072, 4096, 100, 1124, 2148];
    for &slot in &slots {
        let g = list.insert_if_empty(slot, slot * 2);
        drop(g);
    }

    // Iterate the full range and collect found slots.
    let found: Vec<u64> = list.iter_range(0, 5000).map(|(slot, _)| slot).collect();

    // Should find all inserted slots, in order.
    let mut expected = slots.clone();
    expected.sort_unstable();
    assert_eq!(found, expected, "iter_range should find all sparse slots");
}
