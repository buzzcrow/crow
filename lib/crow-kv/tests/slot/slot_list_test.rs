// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]

//! Integration tests for `PxSlotList` — lock-free chunked sparse array.
//!
//! Covers general-value tests and `PxSlotNode` integration scenarios.

use crow_kv::paxos::roles::{PxBallot, PxLogEntry};
use crow_kv::paxos::slot_list::PxSlotList;
use crow_kv::paxos::slot_node::PxSlotNode;

const SLOT_CHUNK_SIZE: usize = 1024;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
// ---------- general value tests ----------

#[test]
fn new_is_empty() {
    let list: PxSlotList<u64> = PxSlotList::new();
    assert!(list.is_empty());
    assert_eq!(list.len(), 0);
    assert_eq!(list.trim_slot(), 0);
}

#[test]
fn insert_and_get() {
    let list = PxSlotList::new();
    let guard = list.insert_if_empty(5, 42u64);
    assert_eq!(*guard, 42);
    drop(guard);

    let guard2 = list.get(5).unwrap();
    assert_eq!(*guard2, 42);
    assert_eq!(list.len(), 1);
}

#[test]
fn get_nonexistent_returns_none() {
    let list: PxSlotList<u64> = PxSlotList::new();
    assert!(list.get(0).is_none());
    assert!(list.get(999).is_none());
}

#[test]
fn get_below_trim_returns_none() {
    let list = PxSlotList::new();
    list.insert_if_empty(10, 100u64);
    list.trim(11);
    assert!(list.get(10).is_none());
    assert_eq!(list.trim_slot(), 11);
}

#[test]
fn insert_duplicate_returns_existing() {
    let list = PxSlotList::new();
    let g1 = list.insert_if_empty(3, "first");
    assert_eq!(*g1, "first");
    drop(g1);

    let g2 = list.insert_if_empty(3, "second");
    assert_eq!(*g2, "first");
    drop(g2);

    assert_eq!(list.len(), 1);
}

#[test]
fn multiple_slots_and_chunks() {
    let list = PxSlotList::new();
    let count = SLOT_CHUNK_SIZE as u64 + 10;
    for i in 0..count {
        list.insert_if_empty(i, i * 2);
    }
    assert_eq!(list.len(), usize::try_from(count).unwrap());

    for i in 0..count {
        let guard = list.get(i).unwrap();
        assert_eq!(*guard, i * 2);
    }
}

#[test]
fn get_tail_finds_from_back() {
    let list = PxSlotList::new();
    let n = SLOT_CHUNK_SIZE as u64 * 3 + 5;
    for i in 0..n {
        list.insert_if_empty(i, i);
    }
    let guard = list.get_tail(n - 1).unwrap();
    assert_eq!(*guard, n - 1);
}

#[test]
fn get_tail_ptr_allows_atomic_access() {
    let list = PxSlotList::new();
    list.insert_if_empty(7, 99u64);

    let ptr_guard = list.get_tail_ptr(7).unwrap();
    let ptr = ptr_guard.load(Ordering::Acquire);
    assert!(!ptr.is_null());
    unsafe {
        assert_eq!(*ptr, 99);
    }
}

#[test]
fn trim_removes_chunks_and_updates_len() {
    let list = PxSlotList::new();
    let count = SLOT_CHUNK_SIZE as u64 * 2;
    for i in 0..count {
        list.insert_if_empty(i, i);
    }
    assert_eq!(list.len(), usize::try_from(count).unwrap());

    let mid = SLOT_CHUNK_SIZE as u64;
    list.trim(mid);
    assert_eq!(list.trim_slot(), mid);

    for i in 0..mid {
        assert!(list.get(i).is_none());
    }
    for i in mid..count {
        let guard = list.get(i).unwrap();
        assert_eq!(*guard, i);
    }

    // len is only decremented after reclaim
    let _ = list.reclaim();
    assert_eq!(list.len(), usize::try_from(count - mid).unwrap());
}

#[test]
fn reclaim_frees_retired_chunks() {
    let list = PxSlotList::new();
    for i in 0..(SLOT_CHUNK_SIZE as u64 * 2) {
        list.insert_if_empty(i, i);
    }
    list.trim(SLOT_CHUNK_SIZE as u64);
    let freed = list.reclaim();
    assert_eq!(freed, 1);

    let freed2 = list.reclaim();
    assert_eq!(freed2, 0);
}

#[test]
fn sparse_insert_and_get() {
    let list = PxSlotList::new();
    list.insert_if_empty(0, "a");
    list.insert_if_empty(5, "b");
    list.insert_if_empty(1024, "c");

    assert_eq!(*list.get(0).unwrap(), "a");
    assert!(list.get(1).is_none());
    assert!(list.get(2).is_none());
    assert_eq!(*list.get(5).unwrap(), "b");
    assert_eq!(*list.get(1024).unwrap(), "c");
}

#[test]
fn iter_range_returns_present_slots_only() {
    let list = PxSlotList::new();
    list.insert_if_empty(1, 10u64);
    list.insert_if_empty(3, 30u64);
    list.insert_if_empty(8, 80u64);

    let items: Vec<(u64, u64)> = list
        .iter_range(0, 10)
        .map(|(slot, guard)| (slot, *guard))
        .collect();

    assert_eq!(items, vec![(1, 10), (3, 30), (8, 80)]);
}

#[test]
fn iter_range_respects_trim_watermark() {
    let list = PxSlotList::new();
    for i in 0..6u64 {
        list.insert_if_empty(i, i);
    }
    list.trim(3);

    let items: Vec<(u64, u64)> = list
        .iter_range(0, 6)
        .map(|(slot, guard)| (slot, *guard))
        .collect();

    assert_eq!(items, vec![(3, 3), (4, 4), (5, 5)]);
}

#[test]
fn drop_destroys_values() {
    struct DropCounter {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    {
        let list = PxSlotList::new();
        for i in 0..(SLOT_CHUNK_SIZE as u64 + 3) {
            let guard = list.insert_if_empty(
                i,
                DropCounter {
                    drops: Arc::clone(&drops),
                },
            );
            drop(guard);
        }
    }

    assert_eq!(drops.load(Ordering::Relaxed), SLOT_CHUNK_SIZE + 3);
}

#[test]
fn guard_ref_counts_chunk() {
    let list = PxSlotList::new();
    // Create two chunks so we can retire the first one
    list.insert_if_empty(0, 0u64);
    list.insert_if_empty(SLOT_CHUNK_SIZE as u64, 1u64);

    // Hold a live guard on a slot in the first chunk
    let g1 = list.get(0).unwrap();

    // Trim at the second chunk start - first chunk should be retired
    list.trim(SLOT_CHUNK_SIZE as u64);
    // Reclaim cannot free the first chunk because g1 still holds a ref
    assert_eq!(list.reclaim(), 0);

    drop(g1);
    // Now reclaim should free the retired first chunk
    assert_eq!(list.reclaim(), 1);
}

// ---------- slot_node integration tests ----------

fn make_entry(slot: u64, ballot: PxBallot) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot,
        term: ballot.round,
        payload: bytes::Bytes::from_static(&[1, 2, 3]),
    }
}

#[test]
fn slot_node_insert_and_promise() {
    let list = PxSlotList::new();
    let guard = list.insert_if_empty(5, PxSlotNode::default());
    let node: &PxSlotNode = &guard;

    let ballot = PxBallot::new(1, 1);
    let result = node.cas_promised(null_mut(), ballot);
    assert!(result.is_ok());

    assert_eq!(node.promised(), Some(&ballot));
    drop(guard);
}

#[test]
fn slot_node_accept_after_promise() {
    let list = PxSlotList::new();
    let guard = list.insert_if_empty(7, PxSlotNode::default());
    let node: &PxSlotNode = &guard;

    let ballot = PxBallot::new(2, 1);
    let entry = make_entry(7, ballot);

    node.cas_promised(null_mut(), ballot).unwrap();
    node.cas_accepted(null_mut(), entry.clone()).unwrap();

    assert_eq!(node.accepted(), Some(&entry));
    drop(guard);
}

#[test]
fn slot_node_duplicate_insert_returns_same() {
    let list = PxSlotList::new();
    let g1 = list.insert_if_empty(3, PxSlotNode::default());
    let node1: &PxSlotNode = &g1;
    node1.cas_promised(null_mut(), PxBallot::new(1, 1)).unwrap();
    drop(g1);

    let g2 = list.insert_if_empty(3, PxSlotNode::default());
    let node2: &PxSlotNode = &g2;
    // The node returned by duplicate insert is the same one
    assert_eq!(node2.promised(), Some(&PxBallot::new(1, 1)));
    drop(g2);
}

#[test]
fn slot_node_multiple_slots() {
    let list = PxSlotList::new();
    for i in 0..50u64 {
        let guard = list.insert_if_empty(i, PxSlotNode::default());
        let node: &PxSlotNode = &guard;
        node.cas_promised(null_mut(), PxBallot::new(i, i)).unwrap();
        drop(guard);
    }

    for i in 0..50u64 {
        let guard = list.get(i).unwrap();
        let node: &PxSlotNode = &guard;
        assert_eq!(node.promised(), Some(&PxBallot::new(i, i)));
    }
}

#[test]
fn slot_node_trim_and_reclaim_with_live_refs() {
    let list = PxSlotList::new();
    for i in 0..(SLOT_CHUNK_SIZE as u64) {
        let guard = list.insert_if_empty(i, PxSlotNode::default());
        let node: &PxSlotNode = &guard;
        node.cas_promised(null_mut(), PxBallot::new(i, 1)).unwrap();
        drop(guard);
    }

    // Hold a read guard on one slot in the first chunk
    let live_guard = list.get(5).unwrap();

    list.trim(SLOT_CHUNK_SIZE as u64);
    // reclaim should not free the first chunk because live_guard holds a ref
    let freed = list.reclaim();
    assert_eq!(freed, 0);

    drop(live_guard);
    // Now reclaim should free it
    let freed2 = list.reclaim();
    assert_eq!(freed2, 1);
}

// ---------- concurrent stress tests ----------

#[test]
fn concurrent_insert_at_disjoint_ranges() {
    use std::sync::Arc;
    use std::thread;

    let list = Arc::new(PxSlotList::new());
    let n_threads = 8;
    let per_thread = 200u64;
    let mut handles = Vec::new();
    for t in 0..n_threads {
        let l = Arc::clone(&list);
        handles.push(thread::spawn(move || {
            let base = t * per_thread;
            for i in 0..per_thread {
                let slot = base + i;
                let guard = l.insert_if_empty(slot, slot);
                assert_eq!(*guard, slot);
                drop(guard);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let total = n_threads * per_thread;
    assert_eq!(list.len(), usize::try_from(total).unwrap());
    for s in 0..total {
        assert_eq!(*list.get(s).unwrap(), s);
    }
}

#[test]
fn concurrent_insert_and_read() {
    use std::sync::Arc;
    use std::thread;

    let list = Arc::new(PxSlotList::new());
    let total = 2000u64;
    let writer = {
        let l = Arc::clone(&list);
        thread::spawn(move || {
            for i in 0..total {
                let guard = l.insert_if_empty(i, i * 3);
                drop(guard);
            }
        })
    };
    let reader = {
        let l = Arc::clone(&list);
        thread::spawn(move || {
            for s in 0..1000u64 {
                let slot = s % total;
                if let Some(g) = l.get(slot) {
                    assert!(*g == slot * 3 || *g == 0);
                }
            }
        })
    };
    writer.join().unwrap();
    reader.join().unwrap();
    assert_eq!(list.len(), usize::try_from(total).unwrap());
}

#[test]
fn concurrent_insert_trim_reclaim() {
    use std::sync::Arc;
    use std::thread;

    let list = Arc::new(PxSlotList::new());
    let chunk = SLOT_CHUNK_SIZE as u64;

    // Phase 1: insert all slots first (no trimming during insert).
    {
        let l = Arc::clone(&list);
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let l = Arc::clone(&l);
            handles.push(thread::spawn(move || {
                for i in 0..chunk {
                    let slot = t * chunk + i;
                    let guard = l.insert_if_empty(slot, slot);
                    drop(guard);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    // Phase 2: trim concurrently from multiple threads (reclaim is single-threaded).
    {
        let l = Arc::clone(&list);
        let mut handles = Vec::new();
        for t in 1..=3u64 {
            let l = Arc::clone(&l);
            handles.push(thread::spawn(move || {
                l.trim(t * chunk);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Reclaim after all trims complete (reclaim must be single-threaded).
        let _ = l.reclaim();
    }

    // Final trim at the end.
    list.trim(chunk * 4);
    let _ = list.reclaim();
}

#[test]
fn multiple_guards_pin_same_chunk() {
    let list = PxSlotList::new();
    list.insert_if_empty(5, 100u64);
    list.insert_if_empty(6, 200u64);

    let g1 = list.get(5).unwrap();
    let g2 = list.get(6).unwrap();

    list.trim(SLOT_CHUNK_SIZE as u64);
    assert_eq!(list.reclaim(), 0, "chunk not freed: 2 live guards");

    drop(g1);
    assert_eq!(list.reclaim(), 0, "chunk not freed: 1 live guard");

    drop(g2);
    assert_eq!(list.reclaim(), 1, "chunk freed after all guards dropped");
}

#[test]
fn progressive_trim_across_chunks() {
    let list = PxSlotList::new();
    let chunk = SLOT_CHUNK_SIZE as u64;
    for i in 0..(chunk * 3) {
        list.insert_if_empty(i, i);
    }

    list.trim(chunk);
    assert_eq!(list.reclaim(), 1);

    list.trim(chunk * 2);
    assert_eq!(list.reclaim(), 1);

    list.trim(chunk * 3);
    assert_eq!(list.reclaim(), 1);
}
