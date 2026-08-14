// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Reclamation watermark tests: long-lived read guards prevent chunk reclamation
//! until the guard is dropped.

use crow_kv::paxos::slot_list::PxSlotList;

const SLOT_CHUNK_SIZE: usize = 1024;

#[test]
fn long_lived_guard_prevents_reclaim_until_dropped() {
    let list = PxSlotList::<u64>::new();

    // Insert slots in the first chunk (0..1024).
    for slot in 0..SLOT_CHUNK_SIZE as u64 {
        let g = list.insert_if_empty(slot, slot);
        drop(g);
    }
    assert_eq!(list.len(), SLOT_CHUNK_SIZE);

    // Hold a read guard on slot 0 — this pins the first chunk.
    let guard = list.get(0).expect("slot 0 exists");

    // Trim past the entire first chunk.
    list.trim(SLOT_CHUNK_SIZE as u64);

    // The slot is trimmed, so get() returns None now.
    assert!(list.get(0).is_none(), "slot 0 is trimmed");

    // Reclaim should NOT free the chunk because the guard is still held.
    let freed = list.reclaim();
    assert_eq!(freed, 0, "chunk should not be freed while guard is held");

    // Drop the guard — now the chunk can be reclaimed.
    drop(guard);

    let freed = list.reclaim();
    assert_eq!(freed, 1, "chunk freed after guard dropped");
}

#[test]
fn reclaim_after_trim_with_no_guards() {
    let list = PxSlotList::<u64>::new();

    // Fill 3 chunks.
    for slot in 0..(SLOT_CHUNK_SIZE * 3) as u64 {
        let g = list.insert_if_empty(slot, slot);
        drop(g);
    }
    assert_eq!(list.len(), SLOT_CHUNK_SIZE * 3);

    // Trim past first 2 chunks.
    list.trim((SLOT_CHUNK_SIZE * 2) as u64);

    // Reclaim with no guards — should free 2 chunks.
    let freed = list.reclaim();
    assert_eq!(freed, 2, "two chunks freed (no guards held)");

    // Remaining slots in chunk 3 are still accessible.
    for slot in (SLOT_CHUNK_SIZE * 2) as u64..(SLOT_CHUNK_SIZE * 3) as u64 {
        let g = list.get(slot).expect("slot in third chunk should exist");
        assert_eq!(*g, slot);
    }
}

#[test]
fn multiple_guards_pin_one_chunk() {
    let list = PxSlotList::<u64>::new();

    // Insert into first chunk.
    for slot in 0..100u64 {
        let g = list.insert_if_empty(slot, slot);
        drop(g);
    }

    // Hold guards on two different slots in the same chunk.
    let g1 = list.get(10).expect("slot 10");
    let g2 = list.get(50).expect("slot 50");

    // Trim and try to reclaim.
    list.trim(SLOT_CHUNK_SIZE as u64);
    let freed = list.reclaim();
    assert_eq!(freed, 0, "chunk pinned by two guards");

    // Drop one guard — still pinned by the other.
    drop(g1);
    let freed = list.reclaim();
    assert_eq!(freed, 0, "chunk still pinned by one guard");

    // Drop the last guard — now reclaimable.
    drop(g2);
    let freed = list.reclaim();
    assert_eq!(freed, 1, "chunk freed after all guards dropped");
}

#[test]
fn reclaim_is_idempotent() {
    let list = PxSlotList::<u64>::new();

    for slot in 0..SLOT_CHUNK_SIZE as u64 {
        let g = list.insert_if_empty(slot, slot);
        drop(g);
    }

    list.trim(SLOT_CHUNK_SIZE as u64);
    let freed1 = list.reclaim();
    assert_eq!(freed1, 1, "first reclaim frees 1 chunk");

    // Second reclaim with nothing to free.
    let freed2 = list.reclaim();
    assert_eq!(freed2, 0, "second reclaim is no-op");
}

#[test]
fn trim_then_insert_above_trim_point() {
    let list = PxSlotList::<u64>::new();

    // Insert and trim first chunk.
    for slot in 0..SLOT_CHUNK_SIZE as u64 {
        let g = list.insert_if_empty(slot, slot);
        drop(g);
    }
    list.trim(SLOT_CHUNK_SIZE as u64);
    list.reclaim();

    // Insert into a new chunk above the trim point.
    let g = list.insert_if_empty(SLOT_CHUNK_SIZE as u64 + 5, 999);
    assert_eq!(*g, 999);
    drop(g);

    let g2 = list.get(SLOT_CHUNK_SIZE as u64 + 5).expect("new slot exists");
    assert_eq!(*g2, 999);
}

#[test]
fn progressive_trim_and_reclaim_across_many_chunks() {
    const N_CHUNKS: usize = 5;

    let list = PxSlotList::<u64>::new();

    // Fill N_CHUNKS chunks.
    for slot in 0..(SLOT_CHUNK_SIZE * N_CHUNKS) as u64 {
        let g = list.insert_if_empty(slot, slot);
        drop(g);
    }

    // Progressively trim and reclaim one chunk at a time.
    for i in 0..N_CHUNKS {
        let trim_point = (SLOT_CHUNK_SIZE * (i + 1)) as u64;
        list.trim(trim_point);
        let _freed = list.reclaim();
        // May free 0 or 1 depending on whether prior reclaim already cleaned up.
        // The key assertion is that remaining slots are still accessible.
        let remaining_start = trim_point;
        let remaining_end = (SLOT_CHUNK_SIZE * N_CHUNKS) as u64;
        for slot in remaining_start..remaining_end {
            let g = list.get(slot).expect("slot above trim_point should exist");
            assert_eq!(*g, slot);
        }
    }

    // All slots trimmed.
    for slot in 0..(SLOT_CHUNK_SIZE * N_CHUNKS) as u64 {
        assert!(list.get(slot).is_none(), "all slots should be trimmed");
    }
}
