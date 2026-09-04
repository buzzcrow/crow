// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Zone bitmap-scan allocator — per-zone allocation state + Phase 1
//! (sync) allocate/free via per-bit CAS on the usage bitmap.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::RwLock;

use crowdb_common::metrics::Counter;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{ZoneAllocationState, ZoneValue};
use crowdb_protocol::{DiskGroupId, UsageBitmap, ZoneValueExt};

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

/// Result of `compact_zone_inner` — the in-memory compaction step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactResult {
    /// The new `compact_ts` (monotonically advanced).
    pub new_compact_ts: u64,
    /// Count of incarnation-matched free records that were cleared.
    pub new_free_count: u32,
    /// Compatibility counter; incarnation filtering happens upstream.
    pub stale_free_count: u32,
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
    pub(crate) zone_state: AtomicU8,
    /// Total block units, word-aligned (multiple of 64).
    pub unit_capacity: u32,
    pub usage_bits: UsageBitmap,
    /// Rotating cursor over 64-bit words (word index).
    pub last_pos_64: AtomicU64,
    /// Count of set bits (allocated units).
    pub used_count: AtomicU32,
    /// Last compacted snapshot slot (R73).
    pub snapshot_slot: AtomicU64,
    /// Diagnostic maximum free timestamp observed by compaction.
    pub compact_ts: AtomicU64,
    /// Highest KV commit slot completely examined by compaction.
    pub compact_slot: AtomicU64,
    /// `true` when the zone has been compacted and its bitmap is
    /// accurate (eligible for rotation into the active set). Set by
    /// compaction and recovery; cleared when published into the active
    /// set.
    pub compacted_ready: AtomicBool,
    /// Zone-level lock for non-allocate operations (compaction, scanner,
    /// health checks). Not held across `.await` (I9). Allocate uses
    /// per-bit CAS (lock-free) and does not acquire this lock.
    pub zone_lock: RwLock<()>,
    /// Compaction backlog gauge — incremented on free, decremented by
    /// compaction.
    pub uncompacted_free_record_count: AtomicU32,
    /// Per-zone CAS retry counter (§11: `zone.allocate.retry.cms.bit.count`).
    /// Incremented on each failed `cas_bit` in the allocate path.
    pub cas_retry_count: AtomicU64,
    /// Optional metrics handle for CAS retry counter. When `Some`,
    /// the `allocate` path increments this counter on each CAS retry.
    pub metrics_cas_retry: Option<std::sync::Arc<Counter>>,
    /// Compaction-in-progress guard — `try`-set at `compact_zone`
    /// entry, cleared on exit (RAII). Prevents concurrent compaction
    /// of the same zone from double-freeing (HIGH-5).
    pub compacting: AtomicBool,
}

impl DdbZone {
    /// Create a new empty zone with the given capacity.
    #[must_use]
    pub fn new(disk_id: DiskId, zone_index: u32, disk_group_id: DiskGroupId, unit_capacity: u32) -> Self {
        Self {
            disk_id,
            zone_index,
            disk_group_id,
            zone_state: AtomicU8::new(DdbZoneHealth::Healthy as u8),
            unit_capacity,
            usage_bits: UsageBitmap::new(unit_capacity),
            last_pos_64: AtomicU64::new(0),
            used_count: AtomicU32::new(0),
            snapshot_slot: AtomicU64::new(0),
            compact_ts: AtomicU64::new(0),
            compact_slot: AtomicU64::new(0),
            compacted_ready: AtomicBool::new(false),
            zone_lock: RwLock::new(()),
            uncompacted_free_record_count: AtomicU32::new(0),
            cas_retry_count: AtomicU64::new(0),
            metrics_cas_retry: None,
            compacting: AtomicBool::new(false),
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
        self.health() == DdbZoneHealth::Healthy
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

    /// Set the zone health. Test-only: production code no longer marks
    /// individual zones — the disk-level `HwStatus` is the sole
    /// gatekeeper for the allocate path (R76). Retained for tests that
    /// exercise the `allocatable()` health check directly.
    #[cfg(feature = "test-util")]
    pub fn set_health(&self, health: DdbZoneHealth) {
        self.zone_state.store(health as u8, Ordering::Release);
    }

    pub fn health(&self) -> DdbZoneHealth {
        match self.zone_state.load(Ordering::Acquire) {
            0 => DdbZoneHealth::Healthy,
            1 => DdbZoneHealth::Missing,
            _ => DdbZoneHealth::Bad,
        }
    }

    // ── R74 space-metrics accessors ───────────────────────────────
    // CROWDB reuses freed space immediately via bitmap scan (no
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
    /// This is the **allocate-only** bitmap clear (I8), used only by
    /// the allocate Phase 2 failure path to undo Phase 1's bitmap
    /// claim. It is **never** called by the free path — the free path
    /// is persist-only (the bitmap is not touched on free; compaction
    /// is the sole bit-clearer for freed blocks).
    pub fn rollback_allocate(&self, unit_offset: u64, unit_count: u32) -> bool {
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
                return false; // bit already clear
            }
        }

        // CAS-clear each bit.
        for i in 0..unit_count {
            let bit = start + i;
            if !self.usage_bits.cas_bit(bit, false) {
                // Bit was already clear — re-set the bits we already
                // cleared and fail.
                for j in 0..i {
                    let rb = start + j;
                    let _ = self.usage_bits.cas_bit(rb, true);
                }
                return false;
            }
        }

        self.used_count.fetch_sub(unit_count, Ordering::AcqRel);
        true
    }

    /// Record a persist-only free in memory: increment
    /// `uncompacted_free_record_count`. No bitmap mutation, no
    /// `used_count` decrement — the bitmap is a conservative over-
    /// estimate (I1). Compaction is the sole bit-clearer (I3).
    pub fn record_free(&self) {
        self.uncompacted_free_record_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Clear free records already matched to their busy incarnation,
    /// recompute `used_count`, and advance the diagnostic free-time
    /// watermark monotonically. The zone lock is held
    /// only for the in-memory bitmap mutation (I9).
    ///
    /// The caller reads the free records from KV (no lock), calls this
    /// method, then writes the new `ZoneValue` + deletes all free
    /// records in one atomic `batch_write` (I6). Returns the partition
    /// counts so the caller knows how many records it will delete.
    pub fn compact_zone_inner(&self, free_records: &[crate::model::records::FreeRecord]) -> CompactResult {
        // Acquire zone lock for the in-memory bitmap mutation.
        let _guard = self.zone_lock.write().unwrap();

        // The caller has matched these facts against current busy incarnations.
        for free in free_records {
            #[allow(clippy::cast_possible_truncation)]
            let offset = free.key.unit_offset as u32;
            let _ = self.usage_bits.range_clear(offset, free.value.unit_count);
        }

        // Recompute used_count = popcount of the merged bitmap.
        let popcount = self.usage_bits.count_set();
        self.used_count
            .store(u32::try_from(popcount).unwrap_or(u32::MAX), Ordering::Release);

        let max_free_ts = free_records
            .iter()
            .map(|record| record.value.free_ts)
            .max()
            .unwrap_or(0);
        let current_compact_ts = self
            .compact_ts
            .fetch_max(max_free_ts, Ordering::AcqRel)
            .max(max_free_ts);

        #[allow(clippy::cast_possible_truncation)]
        CompactResult {
            new_compact_ts: current_compact_ts,
            new_free_count: free_records.len() as u32,
            stale_free_count: 0,
        }
    }

    /// Build the durable zone snapshot produced by `free_records` without
    /// changing the live bitmap or watermarks.
    #[must_use]
    pub(crate) fn prepare_compaction(
        &self,
        free_records: &[crate::model::records::FreeRecord],
        scan_cutoff: u64,
    ) -> ZoneValue {
        let usage_bits = UsageBitmap::restore(&self.usage_bits.snapshot());
        for free in free_records {
            #[allow(clippy::cast_possible_truncation)]
            let offset = free.key.unit_offset as u32;
            let _ = usage_bits.range_clear(offset, free.value.unit_count);
        }
        let max_free_ts = free_records
            .iter()
            .map(|record| record.value.free_ts)
            .max()
            .unwrap_or(0);
        let mut value = ZoneValue {
            usage_bitmap: usage_bits.snapshot(),
            snapshot_slot: scan_cutoff,
            crc32: 0,
            compact_ts: self.compact_ts.load(Ordering::Acquire).max(max_free_ts),
            compact_slot: scan_cutoff,
        };
        value.compute_checksum();
        value
    }

    /// Expose prospective snapshot construction to integration tests.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn prepare_compaction_for_tests(
        &self,
        free_records: &[crate::model::records::FreeRecord],
        scan_cutoff: u64,
    ) -> ZoneValue {
        self.prepare_compaction(free_records, scan_cutoff)
    }

    /// Mark the zone as compacted and ready for rotation.
    pub fn mark_compacted_ready(&self) {
        self.compacted_ready.store(true, Ordering::Release);
    }

    /// Mark the zone as not ready (published into the active set — will
    /// need re-compaction after being allocated from and freed).
    pub fn clear_compacted_ready(&self) {
        self.compacted_ready.store(false, Ordering::Release);
    }

    /// Scanner stub — replay journal and compare bitmap under the zone
    /// lock. Full implementation is R75; this establishes the lock
    /// discipline.
    pub fn scan_zone_inner(&self) {
        let _guard = self.zone_lock.read().unwrap();
        // R75: compare in-memory bitmap with record-derived bitmap.
    }

    /// Health-check stub — verify zone records and CRC under the zone
    /// lock. Full implementation is R76.
    pub fn health_check_zone_inner(&self) {
        let _guard = self.zone_lock.read().unwrap();
        // R76: verify CRC + snapshot integrity.
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
            compact_ts: self.compact_ts.load(Ordering::Acquire),
            compact_slot: self.compact_slot.load(Ordering::Acquire),
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
            zone_state: AtomicU8::new(DdbZoneHealth::Healthy as u8),
            unit_capacity,
            usage_bits,
            last_pos_64: AtomicU64::new(0),
            used_count: AtomicU32::new(u32::try_from(used_count).unwrap_or(u32::MAX)),
            snapshot_slot: AtomicU64::new(value.snapshot_slot),
            compact_ts: AtomicU64::new(value.compact_ts),
            compact_slot: AtomicU64::new(value.compact_slot),
            compacted_ready: AtomicBool::new(true),
            zone_lock: RwLock::new(()),
            uncompacted_free_record_count: AtomicU32::new(0),
            cas_retry_count: AtomicU64::new(0),
            metrics_cas_retry: None,
            compacting: AtomicBool::new(false),
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
            zone_state: zone.health(),
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
        // Rollback 2 of the 5 units (allocate-only bitmap clear).
        assert!(z.rollback_allocate(r.unit_offset, 2));
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
        assert!(z.rollback_allocate(r.unit_offset, 2));
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

    // ── Compaction watermark tests ─────────────────────────────────

    use crate::model::records::FreeRecord;
    use crowdb_protocol::diskdb::rpc::FreeBlockValue;
    use crowdb_protocol::key::FreeBlockKey;

    fn make_free_record(offset: u64, count: u32, freed_ts: u64) -> FreeRecord {
        FreeRecord {
            key: FreeBlockKey {
                disk_id: DiskId { high: 0, low: 1 },
                zone_index: 0,
                unit_offset: offset,
                allocation_ts: freed_ts,
            },
            value: FreeBlockValue {
                unit_count: count,
                previous_owner: None,
                pre_allocation_ts: freed_ts,
                free_ts: freed_ts,
            },
            commit_slot: freed_ts,
        }
    }

    #[test]
    fn compact_zone_inner_matched_record_is_cleared() {
        let z = make_zone(128);
        let r = z.allocate(4, 100).expect("alloc 4");
        // Set compact_ts to 100 — a free record with freed_ts=50 is stale.
        z.compact_ts.store(100, Ordering::Release);
        let free_records = vec![make_free_record(r.unit_offset, 4, 50)];
        let result = z.compact_zone_inner(&free_records);
        assert_eq!(result.stale_free_count, 0);
        assert_eq!(result.new_free_count, 1);
        assert_eq!(z.used_count.load(Ordering::Acquire), 0);
        assert_eq!(z.compact_ts.load(Ordering::Acquire), 100);
    }

    #[test]
    fn compact_zone_inner_new_records_cleared() {
        // Free records with freed_ts > compact_ts are new — their bits
        // ARE range_cleared, used_count recomputed, compact_ts advanced.
        let z = make_zone(128);
        let r = z.allocate(4, 100).expect("alloc 4");
        z.compact_ts.store(50, Ordering::Release);
        let free_records = vec![make_free_record(r.unit_offset, 4, 100)];
        let result = z.compact_zone_inner(&free_records);
        assert_eq!(result.stale_free_count, 0);
        assert_eq!(result.new_free_count, 1);
        // Bitmap cleared — used_count back to 0.
        assert_eq!(z.used_count.load(Ordering::Acquire), 0);
        // compact_ts advanced to max(50, 100) = 100.
        assert_eq!(z.compact_ts.load(Ordering::Acquire), 100);
    }

    #[test]
    fn compact_zone_inner_compact_ts_monotonic() {
        // compact_ts must not regress even if all free records are stale.
        let z = make_zone(128);
        z.compact_ts.store(200, Ordering::Release);
        let free_records = vec![make_free_record(0, 4, 50)];
        let result = z.compact_zone_inner(&free_records);
        // compact_ts stays at 200 (max(200, 50) = 200, but 50 is stale
        // so max_new_freed_ts = 0, new_compact_ts = max(200, 0) = 200).
        assert_eq!(result.new_compact_ts, 200);
        assert_eq!(z.compact_ts.load(Ordering::Acquire), 200);
    }

    #[test]
    fn compact_zone_inner_mixed_stale_and_new() {
        let z = make_zone(128);
        let r1 = z.allocate(4, 100).expect("alloc 4 at 0");
        let r2 = z.allocate(4, 100).expect("alloc 4 at 4");
        z.compact_ts.store(50, Ordering::Release);
        // The caller has incarnation-matched both records.
        let free_records = vec![
            make_free_record(r1.unit_offset, 4, 30),
            make_free_record(r2.unit_offset, 4, 100),
        ];
        let result = z.compact_zone_inner(&free_records);
        assert_eq!(result.stale_free_count, 0);
        assert_eq!(result.new_free_count, 2);
        assert_eq!(z.used_count.load(Ordering::Acquire), 0);
        // compact_ts = max(50, 100) = 100.
        assert_eq!(z.compact_ts.load(Ordering::Acquire), 100);
    }

    #[test]
    fn rollback_allocate_does_not_increment_backlog() {
        let z = make_zone(128);
        let r = z.allocate(4, 100).expect("alloc 4");
        let backlog_before = z.uncompacted_free_record_count.load(Ordering::Acquire);
        assert!(z.rollback_allocate(r.unit_offset, 4));
        let backlog_after = z.uncompacted_free_record_count.load(Ordering::Acquire);
        assert_eq!(
            backlog_after, backlog_before,
            "rollback must not increment backlog"
        );
    }

    #[test]
    fn record_free_increments_backlog() {
        let z = make_zone(128);
        let backlog_before = z.uncompacted_free_record_count.load(Ordering::Acquire);
        z.record_free();
        let backlog_after = z.uncompacted_free_record_count.load(Ordering::Acquire);
        assert_eq!(backlog_after, backlog_before + 1);
    }

    #[test]
    fn compacted_ready_lifecycle() {
        let z = make_zone(128);
        // new → false
        assert!(!z.compacted_ready.load(Ordering::Acquire));
        // mark_compacted_ready → true
        z.mark_compacted_ready();
        assert!(z.compacted_ready.load(Ordering::Acquire));
        // clear_compacted_ready → false
        z.clear_compacted_ready();
        assert!(!z.compacted_ready.load(Ordering::Acquire));
    }
}
