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
#[derive(Default, Debug)]
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
