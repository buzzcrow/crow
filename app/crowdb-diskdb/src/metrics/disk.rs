// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-disk hot-path counters (R74 §3). Lock-free atomic event counters
//! — period (swapped to 0 by the reporting loop each tick) + total
//! (monotonic, never reset). The capacity/busy/free gauges are derived
//! from the bitmap on the reporting tick (§11), so this struct holds
//! only event counters, not capacity atomics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-zone period snapshot returned by `swap_periods`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeriodSnapshot {
    pub allocate_count: u64,
    pub allocate_bytes: u64,
    pub free_count: u64,
    pub free_bytes: u64,
}

/// Lock-free per-disk event counters. `record_allocate`/`record_free`
/// are called on the hot path after the Phase 1 bitmap CAS succeeds
/// (exactly once per durable-bound allocation/free). The reporting loop
/// calls `swap_periods` each tick and flushes the deltas into the
/// crowdb-common `allocate_total`/`free_total` counters.
#[derive(Debug, Default)]
pub struct DiskMetrics {
    period_allocate_count: AtomicU64,
    period_allocate_bytes: AtomicU64,
    period_free_count: AtomicU64,
    period_free_bytes: AtomicU64,
    total_allocate_count: AtomicU64,
    total_allocate_bytes: AtomicU64,
    total_free_count: AtomicU64,
    total_free_bytes: AtomicU64,
}

impl DiskMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump period + total allocate count/bytes. `count` is the number
    /// of allocation events (incremented by 1); `bytes` is
    /// `unit_count * unit_size_bytes`. Called after the Phase 1 bitmap
    /// CAS succeeds.
    pub fn record_allocate(&self, unit_count: u32, unit_size_bytes: u32) {
        let bytes = u64::from(unit_count) * u64::from(unit_size_bytes);
        self.period_allocate_count.fetch_add(1, Ordering::Relaxed);
        self.period_allocate_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_allocate_count.fetch_add(1, Ordering::Relaxed);
        self.total_allocate_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Bump period + total free count/bytes. `count` is the number of
    /// free events (incremented by 1); `bytes` is
    /// `unit_count * unit_size_bytes`. Called after the Phase 1 bitmap
    /// clear succeeds.
    pub fn record_free(&self, unit_count: u32, unit_size_bytes: u32) {
        let bytes = u64::from(unit_count) * u64::from(unit_size_bytes);
        self.period_free_count.fetch_add(1, Ordering::Relaxed);
        self.period_free_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_free_count.fetch_add(1, Ordering::Relaxed);
        self.total_free_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Atomically swap period counters to 0 and return the deltas.
    /// Totals are kept inline (incremented in `record_*`) for
    /// crash-safe monotonicity — the reporting loop does not add
    /// period deltas to totals.
    pub fn swap_periods(&self) -> PeriodSnapshot {
        PeriodSnapshot {
            allocate_count: self.period_allocate_count.swap(0, Ordering::Relaxed),
            allocate_bytes: self.period_allocate_bytes.swap(0, Ordering::Relaxed),
            free_count: self.period_free_count.swap(0, Ordering::Relaxed),
            free_bytes: self.period_free_bytes.swap(0, Ordering::Relaxed),
        }
    }

    /// Current total allocate count (monotonic).
    #[must_use]
    pub fn total_allocate_count(&self) -> u64 {
        self.total_allocate_count.load(Ordering::Relaxed)
    }

    /// Current total free count (monotonic).
    #[must_use]
    pub fn total_free_count(&self) -> u64 {
        self.total_free_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_swap_periods_accumulate_totals() {
        let m = DiskMetrics::new();
        m.record_allocate(4, 1024);
        m.record_allocate(4, 1024);
        m.record_allocate(4, 1024);
        let snap = m.swap_periods();
        assert_eq!(snap.allocate_count, 3);
        assert_eq!(snap.allocate_bytes, 3 * 4 * 1024);
        assert_eq!(snap.free_count, 0);
        assert_eq!(m.total_allocate_count(), 3);

        m.record_allocate(4, 1024);
        let snap = m.swap_periods();
        assert_eq!(snap.allocate_count, 1);
        assert_eq!(m.total_allocate_count(), 4);
    }

    #[test]
    fn record_free_tracks_periods_and_totals() {
        let m = DiskMetrics::new();
        m.record_free(2, 512);
        m.record_free(2, 512);
        let snap = m.swap_periods();
        assert_eq!(snap.free_count, 2);
        assert_eq!(snap.free_bytes, 2 * 2 * 512);
        assert_eq!(m.total_free_count(), 2);

        let snap = m.swap_periods();
        assert_eq!(snap.free_count, 0);
        assert_eq!(m.total_free_count(), 2);
    }

    #[test]
    fn swap_periods_zeroes_period_only() {
        let m = DiskMetrics::new();
        m.record_allocate(8, 100);
        let _ = m.swap_periods();
        let snap = m.swap_periods();
        assert_eq!(snap.allocate_count, 0);
        assert_eq!(m.total_allocate_count(), 1);
    }
}
