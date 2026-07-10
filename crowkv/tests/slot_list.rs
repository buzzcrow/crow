#![allow(unsafe_code)]

//! Integration tests for `SlotList` — lock-free chunked sparse array.
//!
//! Covers general-value tests and `PxSlotNode` integration scenarios.

use crowkv::paxos::slot_list::SlotList;

const SLOT_CHUNK_SIZE: usize = 1024;
use crowkv::paxos::slot_node::{
    LogEntryKind,
    PxBallot,
    PxLogEntry,
    PxSlotNode,
};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ---------- general value tests ----------

#[test]
fn new_is_empty() {
    let list: SlotList<u64> = SlotList::new();
    assert!(list.is_empty());
    assert_eq!(list.len(), 0);
    assert_eq!(list.trim_slot(), 0);
}

#[test]
fn insert_and_get() {
    let list = SlotList::new();
    let guard = list.insert_if_empty(5, 42u64);
    assert_eq!(*guard, 42);
    drop(guard);

    let guard2 = list.get(5).unwrap();
    assert_eq!(*guard2, 42);
    assert_eq!(list.len(), 1);
}

#[test]
fn get_nonexistent_returns_none() {
    let list: SlotList<u64> = SlotList::new();
    assert!(list.get(0).is_none());
    assert!(list.get(999).is_none());
}

#[test]
fn get_below_trim_returns_none() {
    let list = SlotList::new();
    list.insert_if_empty(10, 100u64);
    list.trim(11);
    assert!(list.get(10).is_none());
    assert_eq!(list.trim_slot(), 11);
}

#[test]
fn insert_duplicate_returns_existing() {
    let list = SlotList::new();
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
    let list = SlotList::new();
    let count = SLOT_CHUNK_SIZE as u64 + 10;
    for i in 0..count {
        list.insert_if_empty(i, i * 2);
    }
    assert_eq!(list.len(), count as usize);

    for i in 0..count {
        let guard = list.get(i).unwrap();
        assert_eq!(*guard, i * 2);
    }
}

#[test]
fn get_tail_finds_from_back() {
    let list = SlotList::new();
    let n = SLOT_CHUNK_SIZE as u64 * 3 + 5;
    for i in 0..n {
        list.insert_if_empty(i, i);
    }
    let guard = list.get_tail(n - 1).unwrap();
    assert_eq!(*guard, n - 1);
}

#[test]
fn get_tail_ptr_allows_atomic_access() {
    let list = SlotList::new();
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
    let list = SlotList::new();
    let count = SLOT_CHUNK_SIZE as u64 * 2;
    for i in 0..count {
        list.insert_if_empty(i, i);
    }
    assert_eq!(list.len(), count as usize);

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
    assert_eq!(list.len(), (count - mid) as usize);
}

#[test]
fn reclaim_frees_retired_chunks() {
    let list = SlotList::new();
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
    let list = SlotList::new();
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
    let list = SlotList::new();
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
    let list = SlotList::new();
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
        let list = SlotList::new();
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
    let list = SlotList::new();
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
        kind: LogEntryKind::Write,
        payload: vec![1, 2, 3],
        client_id: Some(1),
        seq: Some(1),
    }
}

#[test]
fn slot_node_insert_and_promise() {
    let list = SlotList::new();
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
    let list = SlotList::new();
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
    let list = SlotList::new();
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
    let list = SlotList::new();
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
    let list = SlotList::new();
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

