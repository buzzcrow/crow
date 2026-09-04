// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Segment index: maps slot → (`disk_idx`, `segment_id`, `file_offset`).
//!
//! In-memory lookup; persisted as a small auxiliary file per group but
//! rebuildable from segment headers/footers if missing or stale.

use std::collections::BTreeMap;

use crate::paxos::roles::SlotIndex;

/// Location of a record within the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotLocation {
    pub disk_idx: usize,
    pub segment_id: u64,
    pub file_offset: u64,
}

/// In-memory index mapping slot → latest known location.
///
/// This is a rebuildable cache used for lookup and GC metadata. Multiple WAL
/// records may exist for the same slot; inserting a later location overwrites
/// the cache entry while replay still scans every durable record.
#[derive(Clone, Default, Debug)]
pub struct SegmentIndex {
    map: BTreeMap<SlotIndex, SlotLocation>,
    /// All known `segment_ids` per disk, with their slot ranges.
    segments: BTreeMap<u64, SegmentMeta>,
}

/// Per-segment metadata used by GC and index rebuild.
#[derive(Clone, Debug)]
pub struct SegmentMeta {
    pub segment_id: u64,
    pub disk_idx: usize,
    pub min_slot: SlotIndex,
    pub max_slot: SlotIndex,
    pub record_count: u32,
}

impl SegmentIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a slot→location cache mapping. Overwrites if already present.
    pub fn insert(&mut self, slot: SlotIndex, loc: SlotLocation) {
        self.map.insert(slot, loc);
    }

    /// Look up the location of a slot.
    #[must_use]
    pub fn locate(&self, slot: SlotIndex) -> Option<&SlotLocation> {
        self.map.get(&slot)
    }

    /// Register a segment's metadata (used by GC).
    pub fn register_segment(&mut self, meta: SegmentMeta) {
        self.segments.insert(meta.segment_id, meta);
    }

    /// Remove a segment and all its slot entries.
    pub fn remove_segment(&mut self, segment_id: u64) {
        if let Some(meta) = self.segments.remove(&segment_id) {
            self.map.retain(|_slot, loc| loc.segment_id != meta.segment_id);
        }
    }

    /// Iterate all segment metadata.
    pub fn segments(&self) -> impl Iterator<Item = &SegmentMeta> {
        self.segments.values()
    }

    /// All slots in the index.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.map.len()
    }

    /// Rebuild the index from a set of segment scans.
    pub fn rebuild_from_scans(&mut self, scans: Vec<(SegmentMeta, Vec<(SlotIndex, u64)>)>) {
        self.map.clear();
        self.segments.clear();
        for (meta, slots) in scans {
            let disk_idx = meta.disk_idx;
            let seg_id = meta.segment_id;
            for (slot, file_offset) in slots {
                self.map.insert(
                    slot,
                    SlotLocation {
                        disk_idx,
                        segment_id: seg_id,
                        file_offset,
                    },
                );
            }
            self.segments.insert(seg_id, meta);
        }
    }
}

/// Pipeline-partitioned live WAL index.
///
/// Each writer mutates only its own shard. Cross-pipeline consumers take a
/// bounded snapshot one shard at a time and never hold an index lock over I/O.
#[derive(Debug)]
pub struct ShardedSegmentIndex {
    shards: Vec<parking_lot::Mutex<SegmentIndex>>,
    group_id: u64,
}

impl ShardedSegmentIndex {
    /// Create one independent shard per WAL pipeline.
    ///
    /// # Panics
    ///
    /// Panics when `pipeline_count` is zero.
    #[must_use]
    pub fn new(pipeline_count: usize, group_id: u64) -> Self {
        assert!(pipeline_count > 0, "WAL requires at least one pipeline");
        Self {
            shards: (0..pipeline_count)
                .map(|_| parking_lot::Mutex::new(SegmentIndex::new()))
                .collect(),
            group_id,
        }
    }

    fn shard_for_slot(&self, slot: SlotIndex) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        let hash = slot.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ self.group_id;
        usize::try_from(hash % self.shards.len() as u64).expect("pipeline count exceeds usize")
    }

    pub fn insert(&self, pipeline_idx: usize, slot: SlotIndex, loc: SlotLocation) {
        debug_assert_eq!(pipeline_idx, loc.disk_idx);
        self.shards[pipeline_idx].lock().insert(slot, loc);
    }

    #[must_use]
    pub fn locate(&self, slot: SlotIndex) -> Option<SlotLocation> {
        self.shards[self.shard_for_slot(slot)]
            .lock()
            .locate(slot)
            .copied()
    }

    pub fn register_segment(&self, pipeline_idx: usize, meta: SegmentMeta) {
        debug_assert_eq!(pipeline_idx, meta.disk_idx);
        self.shards[pipeline_idx].lock().register_segment(meta);
    }

    pub fn remove_segment(&self, pipeline_idx: usize, segment_id: u64) {
        self.shards[pipeline_idx].lock().remove_segment(segment_id);
    }

    #[must_use]
    pub fn segments_snapshot(&self) -> Vec<SegmentMeta> {
        self.shards
            .iter()
            .flat_map(|shard| shard.lock().segments().cloned().collect::<Vec<_>>())
            .collect()
    }

    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().slot_count()).sum()
    }

    /// Return a point-in-time merged view, acquiring one shard at a time.
    #[must_use]
    pub fn snapshot(&self) -> SegmentIndex {
        let mut snapshot = SegmentIndex::new();
        for shard in &self.shards {
            let shard = shard.lock();
            snapshot
                .map
                .extend(shard.map.iter().map(|(slot, loc)| (*slot, *loc)));
            snapshot
                .segments
                .extend(shard.segments.iter().map(|(id, meta)| (*id, meta.clone())));
        }
        snapshot
    }

    /// Compatibility alias for callers that previously held the global index.
    #[must_use]
    pub fn lock(&self) -> SegmentIndex {
        self.snapshot()
    }
}
