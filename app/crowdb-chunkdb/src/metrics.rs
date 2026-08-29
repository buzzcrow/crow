// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lifecycle lock + cache metrics for chunkdb observability.
//! Hot-path counters are `AtomicU64` with `Relaxed` ordering;
//! latency histograms are `Mutex<PreciseHistogram>` (rare contention).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crowdb_common::metrics::PreciseHistogram;
use serde::{Deserialize, Serialize};

/// Snapshot of [`LifecycleMetrics`] at a point in time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleMetricsSnapshot {
    pub lock_timeout_count: u64,
    pub lock_busy_count: u64,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
    pub cache_size: u64,
    pub reap_idle_count: u64,
    pub reap_idle_entries_removed: u64,
    /// Number of cache entries invalidated (one increment per chunk
    /// removed, by both `invalidate_chunk` and `invalidate_range`).
    pub invalidate_count: u64,
    pub lock_wait_count: u64,
    pub lock_wait_p50_us: u64,
    pub lock_wait_p99_us: u64,
    pub lock_wait_max_us: u64,
    pub lock_hold_count: u64,
    pub lock_hold_p50_us: u64,
    pub lock_hold_p99_us: u64,
    pub lock_hold_max_us: u64,
}

/// Latency histograms behind a `Mutex` — `PreciseHistogram` requires
/// `&mut self` for `record()`.
#[derive(Debug)]
struct LatencyHistograms {
    lock_wait: PreciseHistogram,
    lock_hold: PreciseHistogram,
}

impl Default for LatencyHistograms {
    fn default() -> Self {
        Self {
            lock_wait: PreciseHistogram::new(3),
            lock_hold: PreciseHistogram::new(3),
        }
    }
}

/// Metrics for the per-chunk lifecycle lock + payload cache.
#[derive(Debug, Default)]
pub struct LifecycleMetrics {
    lock_timeout_count: AtomicU64,
    lock_busy_count: AtomicU64,
    cache_hit_count: AtomicU64,
    cache_miss_count: AtomicU64,
    reap_idle_count: AtomicU64,
    reap_idle_entries_removed: AtomicU64,
    invalidate_count: AtomicU64,
    lat: Mutex<LatencyHistograms>,
}

impl LifecycleMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_lock_timeout(&self) {
        self.lock_timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lock_busy(&self) {
        self.lock_busy_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_hit(&self) {
        self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cache_miss(&self) {
        self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reap_idle(&self, entries_removed: u64) {
        self.reap_idle_count.fetch_add(1, Ordering::Relaxed);
        self.reap_idle_entries_removed
            .fetch_add(entries_removed, Ordering::Relaxed);
    }

    pub(crate) fn record_invalidate(&self) {
        self.invalidate_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_lock_wait(&self, dur_us: u64) {
        if let Ok(mut g) = self.lat.lock() {
            g.lock_wait.record(dur_us.max(1));
        }
    }

    pub(crate) fn record_lock_hold(&self, dur_us: u64) {
        if let Ok(mut g) = self.lat.lock() {
            g.lock_hold.record(dur_us.max(1));
        }
    }

    /// Snapshot all counters + histogram percentiles. `cache_size` is
    /// passed in by the caller (read from `quick_cache::Cache::entry_count()`).
    #[must_use]
    pub fn snapshot(&self, cache_size: u64) -> LifecycleMetricsSnapshot {
        let lat = self.lat.lock().map_or_else(
            |_| LatencyHistograms::default(),
            |g| LatencyHistograms {
                lock_wait: g.lock_wait.clone(),
                lock_hold: g.lock_hold.clone(),
            },
        );
        LifecycleMetricsSnapshot {
            lock_timeout_count: self.lock_timeout_count.load(Ordering::Relaxed),
            lock_busy_count: self.lock_busy_count.load(Ordering::Relaxed),
            cache_hit_count: self.cache_hit_count.load(Ordering::Relaxed),
            cache_miss_count: self.cache_miss_count.load(Ordering::Relaxed),
            cache_size,
            reap_idle_count: self.reap_idle_count.load(Ordering::Relaxed),
            reap_idle_entries_removed: self.reap_idle_entries_removed.load(Ordering::Relaxed),
            invalidate_count: self.invalidate_count.load(Ordering::Relaxed),
            lock_wait_count: lat.lock_wait.len(),
            lock_wait_p50_us: lat.lock_wait.value_at_quantile(0.50),
            lock_wait_p99_us: lat.lock_wait.value_at_quantile(0.99),
            lock_wait_max_us: lat.lock_wait.max(),
            lock_hold_count: lat.lock_hold.len(),
            lock_hold_p50_us: lat.lock_hold.value_at_quantile(0.50),
            lock_hold_p99_us: lat.lock_hold.value_at_quantile(0.99),
            lock_hold_max_us: lat.lock_hold.max(),
        }
    }
}
