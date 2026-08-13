// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Zone bitmap-scan allocator — per-zone allocation state + Phase 1
//! (sync) allocate/free via per-bit CAS on the usage bitmap.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use crow_common::metrics::Counter;
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::{ZoneAllocationState, ZoneValue};
use crow_protocol::{DiskGroupId, UsageBitmap, ZoneValueExt};

/// Zone health — zones inherit the disk's `HwStatus`; no separate
/// zone-level CAS state machine (§9). Updated by the sync loop and
/// health probe (R76), not the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdbZoneHealth {
    Healthy,
    Missing,
    Bad,
}

/// A successfully allocated range within a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatedRange {
    /// Offset within the zone in units.
    pub unit_offset: u64,
    /// How many units this allocation spans.
    pub unit_count: u32,
}

/// Per-zone allocation state. One `Zone` per zone on a disk.
///
/// The in-memory `usage_bits` bitmap is a performance cache; the
/// durable state is the set of `BusyBlockKey`/`FreeBlockKey`/`ZoneValue`
/// records on the bound data group (§4.8). Allocate/free preserve
/// crash-safety invariants so R73 can recover from any crash point.
pub struct DdbZone {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub disk_group_id: DiskGroupId,
    pub zone_state: RwLock<DdbZoneHealth>,
    /// Total block units, word-aligned (multiple of 64).
    pub unit_capacity: u32,
    pub usage_bits: UsageBitmap,
    /// Rotating cursor over 64-bit words (word index).
    pub last_pos_64: AtomicU64,
    /// Count of set bits (allocated units).
    pub used_count: AtomicU32,
    /// Last compacted snapshot slot (R73).
    pub snapshot_slot: AtomicU64,
    /// Compaction backlog gauge — incremented on free, decremented by
    /// R73 compaction.
    pub uncompacted_free_record_count: AtomicU32,
    /// Per-zone CAS retry counter (§11: `zone.allocate.retry.cms.bit.count`).
    /// Incremented on each failed `cas_bit` in the allocate path.
    pub cas_retry_count: AtomicU64,
    /// Optional metrics handle for CAS retry counter. When `Some`,
    /// the `allocate` path increments this counter on each CAS retry.
    pub metrics_cas_retry: Option<std::sync::Arc<Counter>>,
}

impl DdbZone {
    /// Create a new empty zone with the given capacity.
    #[must_use]
    pub fn new(disk_id: DiskId, zone_index: u32, disk_group_id: DiskGroupId, unit_capacity: u32) -> Self {
        Self {
            disk_id,
            zone_index,
            disk_group_id,
            zone_state: RwLock::new(DdbZoneHealth::Healthy),
            unit_capacity,
            usage_bits: UsageBitmap::new(unit_capacity),
            last_pos_64: AtomicU64::new(0),
            used_count: AtomicU32::new(0),
            snapshot_slot: AtomicU64::new(0),
            uncompacted_free_record_count: AtomicU32::new(0),
            cas_retry_count: AtomicU64::new(0),
            metrics_cas_retry: None,
        }
    }

    /// Attach a metrics counter for CAS retries.
    #[must_use]
    pub fn with_cas_retry_metric(mut self, counter: std::sync::Arc<Counter>) -> Self {
        self.metrics_cas_retry = Some(counter);
        self
    }

    /// Whether this zone can accept allocations: `Healthy` and has
    /// free units.
    #[must_use]
    pub fn allocatable(&self) -> bool {
        *self.zone_state.read().unwrap() == DdbZoneHealth::Healthy
            && self.used_count.load(Ordering::Acquire) < self.unit_capacity
    }

    /// Derived allocation state for reporting only (§9). No CAS state
    /// machine — allocation concurrency is handled by per-bit CAS.
    #[must_use]
    pub fn derived_alloc_state(&self) -> ZoneAllocationState {
        let used = self.used_count.load(Ordering::Acquire);
        if used == 0 {
            ZoneAllocationState::ZoneAllocActive
        } else if used < self.unit_capacity {
            ZoneAllocationState::ZoneAllocAvailable
        } else {
            ZoneAllocationState::ZoneAllocFull
        }
    }

    /// Set the zone health (called by sync loop / health probe).
    pub fn set_health(&self, health: DdbZoneHealth) {
        *self.zone_state.write().unwrap() = health;
    }

    // ── R74 space-metrics accessors ───────────────────────────────
    // CROW reuses freed space immediately via bitmap scan (no
    // append-only `allocate_pos`, §3.4/§8), so `free = capacity -
    // busy`. These read the live atomics; `UsageBitmap::count_set()`
    // is reserved for the recalc verifier (§5) which needs an
    // independent popcount.

    /// Busy block units (live `used_count`).
    #[must_use]
    pub fn busy_blocks(&self) -> u32 {
        self.used_count.load(Ordering::Acquire)
    }

    /// Free block units = `unit_capacity - used_count`.
    #[must_use]
    pub fn free_blocks(&self) -> u32 {
        self.unit_capacity.saturating_sub(self.busy_blocks())
    }

    /// Busy bytes = `busy_blocks * unit_size_bytes`.
    #[must_use]
    pub fn busy_bytes(&self, unit_size_bytes: u32) -> u64 {
        u64::from(self.busy_blocks()) * u64::from(unit_size_bytes)
    }

    /// Free bytes = `free_blocks * unit_size_bytes`.
    #[must_use]
    pub fn free_bytes(&self, unit_size_bytes: u32) -> u64 {
        u64::from(self.free_blocks()) * u64::from(unit_size_bytes)
    }

    /// Capacity bytes = `unit_capacity * unit_size_bytes`.
    #[must_use]
    pub fn capacity_bytes(&self, unit_size_bytes: u32) -> u64 {
        u64::from(self.unit_capacity) * u64::from(unit_size_bytes)
    }

    /// Usage ratio = `used_count / unit_capacity` as f64 (0.0 when
    /// capacity is 0).
    #[must_use]
    pub fn usage_ratio(&self) -> f64 {
        let used = f64::from(self.busy_blocks());
        let cap = f64::from(self.unit_capacity);
        if cap == 0.0 {
            0.0
        } else {
            used / cap
        }
    }

    /// Phase 1 (sync) allocate — scan the usage bitmap from
    /// `last_pos_64` (rotating), find `unit_count` consecutive zero
    /// bits, CAS-set each. On CAS failure, retry the same word
    /// (bounded by `cas_retry_limit`); on exhaustion, fall through to
    /// the next word. Returns `None` if the zone is full or unhealthy.
    ///
    /// The bitmap is a performance cache; the durable `BusyBlockValue`
    /// is persisted in Phase 2 (§4.5). If diskdb crashes between Phase
    /// 1 and Phase 2, the bit is set in memory but no record exists —
    /// R73's full scan rebuilds the bitmap from records (the bit is
    /// clear, so the block is correctly free). This is a self-
    /// correcting ghost allocation (§4.8).
    pub fn allocate(&self, unit_count: u32, cas_retry_limit: u32) -> Option<AllocatedRange> {
        if !self.allocatable() || unit_count == 0 {
            return None;
        }
        let cap = self.unit_capacity;
        if unit_count > cap {
            return None;
        }
        let word_count = self.usage_bits.word_count();
        if word_count == 0 {
            return None;
        }

        #[allow(clippy::cast_possible_truncation)]
        let start_word = (self.last_pos_64.load(Ordering::Relaxed) % word_count as u64) as usize;
        let mut total_retries = 0u32;

        for i in 0..word_count {
            let word_idx = (start_word + i) % word_count;
            if let Some(offset) =
                self.claim_in_word(word_idx, unit_count, cas_retry_limit, &mut total_retries)
            {
                self.last_pos_64.store(word_idx as u64, Ordering::Relaxed);
                self.used_count.fetch_add(unit_count, Ordering::AcqRel);
                return Some(AllocatedRange {
                    unit_offset: offset,
                    unit_count,
                });
            }
        }
        None
    }

    /// Try to claim `unit_count` consecutive zero bits starting in
    /// `word_idx`. Returns the unit offset on success, `None` if the
    /// word is full or retries are exhausted.
    fn claim_in_word(
        &self,
        word_idx: usize,
        unit_count: u32,
        cas_retry_limit: u32,
        total_retries: &mut u32,
    ) -> Option<u64> {
        let word = self.usage_bits.load_word(word_idx);
        if word == u64::MAX {
            return None; // word full
        }

        // Find the first zero bit in this word.
        #[allow(clippy::cast_possible_truncation)]
        let base_bit = word_idx as u32 * 64;
        let first_zero = word.trailing_ones();

        // Try each zero bit in the word as a starting position.
        let mut bit_scan = word;
        let mut bit_pos = first_zero;
        loop {
            if bit_pos >= 64 {
                break; // no more zero bits in this word
            }
            let start_bit = base_bit + bit_pos;
            if start_bit + unit_count > self.unit_capacity {
                break; // past capacity
            }

            if self.try_claim_range(start_bit, unit_count, cas_retry_limit, total_retries) {
                return Some(u64::from(start_bit));
            }

            // Move to the next zero bit in this word.
            bit_scan |= 1u64 << bit_pos; // mark this bit as tried
            if bit_scan == u64::MAX {
                break;
            }
            bit_pos = bit_scan.trailing_ones();
        }
        None
    }

    /// Try to CAS-set `unit_count` consecutive bits starting at
    /// `start_bit`. On any CAS failure, clear the bits already set and
    /// return false. Each failed CAS increments the retry counter.
    fn try_claim_range(
        &self,
        start_bit: u32,
        unit_count: u32,
        cas_retry_limit: u32,
        total_retries: &mut u32,
    ) -> bool {
        // First, verify all bits in the range are clear (fast path
        // check without CAS).
        for i in 0..unit_count {
            let bit = start_bit + i;
            let word_idx = bit as usize / 64;
            let mask = 1u64 << (bit % 64);
            if self.usage_bits.load_word(word_idx) & mask != 0 {
                return false; // bit already set, range not free
            }
        }

        // CAS-set each bit in the range.
        let mut set_bits: Vec<u32> = Vec::with_capacity(unit_count as usize);
        for i in 0..unit_count {
            let bit = start_bit + i;
            if self.usage_bits.cas_bit(bit, true) {
                set_bits.push(bit);
            } else {
                // CAS failed — could be contention or someone set it.
                *total_retries += 1;
                self.cas_retry_count.fetch_add(1, Ordering::Relaxed);
                if let Some(ref metric) = self.metrics_cas_retry {
                    metric.inc();
                }
                // Roll back bits we already set.
                for &rb in &set_bits {
                    let _ = self.usage_bits.cas_bit(rb, false);
                }
                // Retry the same bit up to the retry limit.
                if *total_retries < cas_retry_limit {
                    // Re-check if the bit is still clear (contention, not
                    // a lost allocation).
                    let word_idx = bit as usize / 64;
                    let mask = 1u64 << (bit % 64);
                    if self.usage_bits.load_word(word_idx) & mask == 0 {
                        // Bit still clear — retry from start.
                        set_bits.clear();
                        return self.try_claim_range(start_bit, unit_count, cas_retry_limit, total_retries);
                    }
                }
                return false;
            }
        }
        true
    }

    /// Clear `unit_count` bits starting at `unit_offset` via CAS.
    /// Returns `false` if any bit was already clear (double-free
    /// detection). Decrements `used_count` on success.
    ///
    /// The bitmap clear happens before the durable `FreeBlockValue`
    /// persist (§4.6). If diskdb crashes after the clear but before
    /// the persist, the bit is clear in memory but the `BusyBlockKey`
    /// still exists on disk — R73's full scan re-sets the bit (§4.8).
    pub fn free(&self, unit_offset: u64, unit_count: u32) -> bool {
        if unit_count == 0 {
            return false;
        }
        #[allow(clippy::cast_possible_truncation)]
        let start = unit_offset as u32;
        if start + unit_count > self.unit_capacity {
            return false;
        }

        // Verify all bits are set first (fast path).
        for i in 0..unit_count {
            let bit = start + i;
            let word_idx = bit as usize / 64;
            let mask = 1u64 << (bit % 64);
            if self.usage_bits.load_word(word_idx) & mask == 0 {
                return false; // double-free: bit already clear
            }
        }

        // CAS-clear each bit.
        for i in 0..unit_count {
            let bit = start + i;
            if !self.usage_bits.cas_bit(bit, false) {
                // Bit was already clear (race with another free) —
                // re-set the bits we already cleared and fail.
                for j in 0..i {
                    let rb = start + j;
                    let _ = self.usage_bits.cas_bit(rb, true);
                }
                return false;
            }
        }

        self.used_count.fetch_sub(unit_count, Ordering::AcqRel);
        self.uncompacted_free_record_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Snapshot current `usage_bits` + `snapshot_slot` + `crc32` into a
    /// `ZoneValue` for compaction (R73 strategy 3). The bitmap is
    /// serialized via `UsageBitmap::snapshot`; the CRC is computed via
    /// `ZoneValueExt::compute_checksum`.
    #[must_use]
    pub fn to_zone_value(&self) -> ZoneValue {
        let mut zv = ZoneValue {
            usage_bitmap: self.usage_bits.snapshot(),
            snapshot_slot: self.snapshot_slot.load(Ordering::Acquire),
            crc32: 0,
        };
        zv.compute_checksum();
        zv
    }

    /// Rebuild a `Zone` from a `ZoneValue` snapshot (R73 recovery when
    /// no records exist after the snapshot). Deserializes `usage_bitmap`
    /// into a `UsageBitmap`, computes `used_count` = popcount, sets
    /// `snapshot_slot`. Verifies CRC via `ZoneValueExt::verify_checksum`
    /// before use; returns `None` on CRC failure (caller falls back to
    /// strategy 1).
    #[must_use]
    pub fn from_zone_value(
        disk_id: DiskId,
        zone_index: u32,
        disk_group_id: DiskGroupId,
        unit_capacity: u32,
        value: &ZoneValue,
    ) -> Option<Self> {
        if !value.verify_checksum() {
            return None;
        }
        let usage_bits = UsageBitmap::restore(&value.usage_bitmap);
        let used_count = usage_bits.count_set();
        Some(Self {
            disk_id,
            zone_index,
            disk_group_id,
            zone_state: RwLock::new(DdbZoneHealth::Healthy),
            unit_capacity,
            usage_bits,
            last_pos_64: AtomicU64::new(0),
            used_count: AtomicU32::new(u32::try_from(used_count).unwrap_or(u32::MAX)),
            snapshot_slot: AtomicU64::new(value.snapshot_slot),
            uncompacted_free_record_count: AtomicU32::new(0),
            cas_retry_count: AtomicU64::new(0),
            metrics_cas_retry: None,
        })
    }
}

// ── R74 usage structs ───────────────────────────────────────────

/// Per-zone brief usage — counts only (no bitmap bytes). The full
/// `usage_bitmap` is returned only by the specific-zone query path
/// (R74 §4) via `UsageBitmap::snapshot`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneUsage {
    pub zone_index: u32,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub busy_block_count: u32,
    pub free_block_count: u32,
    pub alloc_state: ZoneAllocationState,
    pub zone_state: DdbZoneHealth,
}

impl ZoneUsage {
    /// Build a brief `ZoneUsage` from a live `DdbZone`.
    #[must_use]
    pub fn from_zone(zone: &DdbZone, unit_size_bytes: u32) -> Self {
        Self {
            zone_index: zone.zone_index,
            capacity_bytes: zone.capacity_bytes(unit_size_bytes),
            busy_bytes: zone.busy_bytes(unit_size_bytes),
            free_bytes: zone.free_bytes(unit_size_bytes),
            busy_block_count: zone.busy_blocks(),
            free_block_count: zone.free_blocks(),
            alloc_state: zone.derived_alloc_state(),
            zone_state: *zone.zone_state.read().unwrap(),
        }
    }
}

#[cfg(test)]
mod accessor_tests {
    use super::*;

    fn make_zone(cap: u32) -> DdbZone {
        DdbZone::new(DiskId { high: 0, low: 1 }, 0, 100, cap)
    }

    #[test]
    fn busy_free_blocks_track_allocations_and_frees() {
        let z = make_zone(128);
        assert_eq!(z.busy_blocks(), 0);
        assert_eq!(z.free_blocks(), 128);
        let r = z.allocate(5, 100).expect("alloc 5");
        assert_eq!(z.busy_blocks(), 5);
        assert_eq!(z.free_blocks(), 123);
        // Free 2 of the 5 units — freed space is reusable immediately.
        assert!(z.free(r.unit_offset, 2));
        assert_eq!(z.busy_blocks(), 3);
        assert_eq!(z.free_blocks(), 125);
    }

    #[test]
    fn bytes_accessors_scale_by_unit_size() {
        let z = make_zone(128);
        let _ = z.allocate(5, 100);
        assert_eq!(z.busy_bytes(1), 5);
        assert_eq!(z.free_bytes(1), 123);
        assert_eq!(z.capacity_bytes(1), 128);
        assert_eq!(z.busy_bytes(1024), 5 * 1024);
        assert_eq!(z.capacity_bytes(1024), 128 * 1024);
    }

    #[test]
    fn usage_ratio_tracks_used_over_capacity() {
        let z = make_zone(128);
        let r = z.allocate(5, 100).expect("alloc 5");
        assert!((z.usage_ratio() - 5.0 / 128.0).abs() < f64::EPSILON);
        assert!(z.free(r.unit_offset, 2));
        assert!((z.usage_ratio() - 3.0 / 128.0).abs() < f64::EPSILON);
    }

    #[test]
    fn usage_ratio_zero_capacity_is_zero() {
        let z = DdbZone::new(DiskId { high: 0, low: 1 }, 0, 100, 0);
        assert!(z.usage_ratio() <= f64::EPSILON);
    }

    #[test]
    fn zone_usage_from_zone_is_brief() {
        let z = make_zone(128);
        let _ = z.allocate(5, 100);
        let u = ZoneUsage::from_zone(&z, 1024);
        assert_eq!(u.zone_index, 0);
        assert_eq!(u.busy_block_count, 5);
        assert_eq!(u.free_block_count, 123);
        assert_eq!(u.capacity_bytes, 128 * 1024);
        assert_eq!(u.busy_bytes, 5 * 1024);
        assert_eq!(u.alloc_state, ZoneAllocationState::ZoneAllocAvailable);
        assert_eq!(u.zone_state, DdbZoneHealth::Healthy);
    }
}
