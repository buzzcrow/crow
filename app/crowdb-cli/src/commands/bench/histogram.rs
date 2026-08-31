// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free fixed-bucket latency histogram for bench reporting.
//!
//! 1µs-resolution linear buckets over `[0, LINEAR_US)` give exact
//! microsecond percentiles for the common RPC/KV latency range; a
//! single overflow bucket catches tail outliers `>= LINEAR_US`.
//! `record_us` is a `Relaxed` `fetch_add` — safe to call from multiple
//! tokio tasks or C++ I/O worker threads (coroutine `on_response`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of linear 1µs buckets. 65 536 → exact µs resolution up to
/// ~65ms (covers every reference regression latency). 512 KB per
/// histogram (one per bench run).
const LINEAR_US: usize = 65_536;
const NUM_BUCKETS: usize = LINEAR_US + 1; // + overflow bucket

#[derive(Debug)]
pub struct BenchHistogram {
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl BenchHistogram {
    #[must_use]
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(NUM_BUCKETS);
        for _ in 0..NUM_BUCKETS {
            buckets.push(AtomicU64::new(0));
        }
        Self {
            buckets,
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }

    /// Record one latency observation in microseconds.
    pub fn record_us(&self, us: u64) {
        let idx = match usize::try_from(us) {
            Ok(v) if v < LINEAR_US => v,
            _ => LINEAR_US,
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Snapshot without resetting.
    #[must_use]
    pub fn snapshot(&self) -> BenchHistSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum_us = self.sum_us.load(Ordering::Relaxed);
        let avg = sum_us.checked_div(count).unwrap_or(0);
        let p50 = self.percentile(count, 50);
        let p99 = self.percentile(count, 99);
        let p999 = self.percentile(count, 999);
        BenchHistSnapshot { avg, p50, p99, p999 }
    }

    fn percentile(&self, count: u64, per_mille: u64) -> u64 {
        if count == 0 {
            return 0;
        }
        // target = count * per_mille / 1000 (per_mille: 50, 99, 999).
        let target = count.saturating_mul(per_mille) / 1000;
        let mut cumulative = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            cumulative += b.load(Ordering::Relaxed);
            if cumulative >= target {
                return if i < LINEAR_US { i as u64 } else { LINEAR_US as u64 };
            }
        }
        LINEAR_US as u64
    }
}

impl Default for BenchHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BenchHistSnapshot {
    pub avg: u64,
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
}
