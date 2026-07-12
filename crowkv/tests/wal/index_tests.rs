//! `SegmentIndex` unit tests: slot→location lookup, segment registration,
//! removal, and rebuild-from-scans.

use crowkv::wal::index::{SegmentIndex, SegmentMeta, SlotLocation};

fn loc(slot: u64, seg: u64, off: u64) -> (u64, SlotLocation) {
    (
        slot,
        SlotLocation {
            disk_idx: 0,
            segment_id: seg,
            file_offset: off,
        },
    )
}

#[allow(clippy::cast_possible_truncation)]
fn meta(seg: u64, min: u64, max: u64) -> SegmentMeta {
    SegmentMeta {
        segment_id: seg,
        disk_idx: 0,
        min_slot: min,
        max_slot: max,
        record_count: (max - min + 1) as u32,
    }
}

#[test]
fn insert_and_locate() {
    let mut idx = SegmentIndex::new();
    let (slot, location) = loc(5, 1, 100);
    idx.insert(slot, location);

    let found = idx.locate(5).expect("slot 5 should be indexed");
    assert_eq!(found.segment_id, 1);
    assert_eq!(found.file_offset, 100);
}

#[test]
fn locate_missing_returns_none() {
    let idx = SegmentIndex::new();
    assert!(idx.locate(42).is_none());
}

#[test]
fn insert_overwrites_prior_location() {
    let mut idx = SegmentIndex::new();
    let (_, l1) = loc(3, 1, 50);
    idx.insert(3, l1);
    let (_, l2) = loc(3, 2, 200);
    idx.insert(3, l2);

    let found = idx.locate(3).expect("slot 3 present");
    assert_eq!(found.segment_id, 2, "later insert overwrites");
    assert_eq!(found.file_offset, 200);
}

#[test]
fn register_and_iterate_segments() {
    let mut idx = SegmentIndex::new();
    idx.register_segment(meta(1, 1, 10));
    idx.register_segment(meta(2, 11, 20));

    let segs: Vec<_> = idx.segments().collect();
    assert_eq!(segs.len(), 2);
    assert_eq!(segs[0].segment_id, 1);
    assert_eq!(segs[1].segment_id, 2);
}

#[test]
fn remove_segment_drops_slot_entries() {
    let mut idx = SegmentIndex::new();
    idx.register_segment(meta(1, 1, 5));
    idx.register_segment(meta(2, 6, 10));

    // Insert slots in both segments.
    for s in 1..=5u64 {
        idx.insert(s, loc(s, 1, s * 100).1);
    }
    for s in 6..=10u64 {
        idx.insert(s, loc(s, 2, s * 100).1);
    }
    assert_eq!(idx.slot_count(), 10);

    // Remove segment 1 — its 5 slots should be gone.
    idx.remove_segment(1);
    assert_eq!(idx.slot_count(), 5, "segment 1 slots removed");
    assert!(idx.locate(1).is_none());
    assert!(idx.locate(5).is_none());
    assert!(idx.locate(6).is_some(), "segment 2 slots remain");
    assert!(idx.locate(10).is_some());
}

#[test]
fn remove_nonexistent_segment_is_noop() {
    let mut idx = SegmentIndex::new();
    idx.register_segment(meta(1, 1, 5));
    idx.insert(1, loc(1, 1, 0).1);

    idx.remove_segment(99); // no such segment
    assert_eq!(idx.slot_count(), 1, "no-op");
    assert!(idx.locate(1).is_some());
}

#[test]
fn rebuild_from_scans_clears_and_repopulates() {
    let mut idx = SegmentIndex::new();
    idx.register_segment(meta(1, 1, 3));
    idx.insert(1, loc(1, 1, 0).1);
    idx.insert(2, loc(2, 1, 100).1);

    // Rebuild with completely different data.
    let scans = vec![
        (meta(10, 1, 2), vec![(1u64, 0u64), (2u64, 64u64)]),
        (meta(20, 3, 5), vec![(3u64, 0u64), (4u64, 64u64), (5u64, 128u64)]),
    ];
    idx.rebuild_from_scans(scans);

    assert_eq!(idx.slot_count(), 5, "rebuilt with 5 slots");
    let segs: Vec<_> = idx.segments().collect();
    assert_eq!(segs.len(), 2, "two segments registered");

    // Old data is gone.
    let found = idx.locate(1).expect("slot 1 in new segment 10");
    assert_eq!(found.segment_id, 10);
    let found = idx.locate(5).expect("slot 5 in new segment 20");
    assert_eq!(found.segment_id, 20);
}

#[test]
fn slot_count_tracks_inserts_and_removals() {
    let mut idx = SegmentIndex::new();
    assert_eq!(idx.slot_count(), 0);

    idx.register_segment(meta(1, 1, 10));
    idx.insert(1, loc(1, 1, 0).1);
    idx.insert(2, loc(2, 1, 100).1);
    assert_eq!(idx.slot_count(), 2);

    // Overwrite doesn't increase count.
    idx.insert(1, loc(1, 1, 200).1);
    assert_eq!(idx.slot_count(), 2);

    idx.remove_segment(1);
    assert_eq!(idx.slot_count(), 0);
}
