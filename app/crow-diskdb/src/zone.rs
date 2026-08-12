// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Zone bitmap-scan allocator — per-zone allocation state + Phase 1
//! (sync) allocate/free via per-bit CAS on the usage bitmap.
//!
//! See `doc/design/diskdb/design-crow-diskdb.md` §8 (allocation
//! algorithm) and `doc/working/design-diskdb-server.md` §4.1.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;

use crow_common::metrics::Counter;
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::ZoneAllocationState;
use crow_protocol::{DiskGroupId, UsageBitmap};

/// Zone health — zones inherit the disk's `HwStatus`; no separate
/// zone-level CAS state machine (§9). Updated by the sync loop and
/// health probe (R76), not the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneHealth {
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
pub struct Zone {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub disk_group_id: DiskGroupId,
    pub zone_state: RwLock<ZoneHealth>,
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

impl Zone {
    /// Create a new empty zone with the given capacity.
    #[must_use]
    pub fn new(disk_id: DiskId, zone_index: u32, disk_group_id: DiskGroupId, unit_capacity: u32) -> Self {
        Self {
            disk_id,
            zone_index,
            disk_group_id,
            zone_state: RwLock::new(ZoneHealth::Healthy),
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
        *self.zone_state.read().unwrap() == ZoneHealth::Healthy
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
    pub fn set_health(&self, health: ZoneHealth) {
        *self.zone_state.write().unwrap() = health;
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
}
