// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricName;

/// Latency bucket boundaries in nanoseconds. 13 buckets: 12 explicit
/// thresholds + infinity. An observation is placed in the first bucket
/// whose upper bound is >= the value.
const BUCKET_BOUNDS_NS: [u64; 12] = [
    1_000,         // 1µs
    10_000,        // 10µs
    100_000,       // 100µs
    500_000,       // 500µs
    1_000_000,     // 1ms
    5_000_000,     // 5ms
    10_000_000,    // 10ms
    50_000_000,    // 50ms
    100_000_000,   // 100ms
    500_000_000,   // 500ms
    1_000_000_000, // 1s
    u64::MAX,      // infinity (catch-all)
];

const NUM_BUCKETS: usize = BUCKET_BOUNDS_NS.len(); // 12

/// Fixed-bucket latency histogram with count and `total_count`.
///
/// Each `observe(ns)` does a binary search on `BUCKET_BOUNDS_NS` to find
/// the bucket, then `fetch_add(1)` on that bucket and on `count`/`total`.
/// No allocation, no locks.
#[allow(dead_code)]
#[derive(Debug)]
pub struct LatencyHistogram {
    name: MetricName,
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    total_count: AtomicU64,
}

#[allow(dead_code)]
impl LatencyHistogram {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
        }
    }

    /// Record a latency observation in nanoseconds.
    pub fn observe(&self, ns: u64) {
        let idx = bucket_index(ns);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(ns, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot and reset window state. Returns count, p50, p99, max (all in
    /// nanoseconds), and `total_count`.
    pub fn flush(&self) -> HistogramSnapshot {
        let mut bucket_counts = [0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            bucket_counts[i] = b.swap(0, Ordering::Relaxed);
        }
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);

        let p50 = percentile(&bucket_counts, count, 50);
        let p99 = percentile(&bucket_counts, count, 99);
        let max = max_latency(&bucket_counts);
        let avg = sum.checked_div(count).unwrap_or(0);

        HistogramSnapshot {
            count,
            avg,
            p50,
            p99,
            max,
            total_count,
        }
    }

    /// Current values without resetting.
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut bucket_counts = [0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            bucket_counts[i] = b.load(Ordering::Relaxed);
        }
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);

        let p50 = percentile(&bucket_counts, count, 50);
        let p99 = percentile(&bucket_counts, count, 99);
        let max = max_latency(&bucket_counts);
        let avg = sum.checked_div(count).unwrap_or(0);

        HistogramSnapshot {
            count,
            avg,
            p50,
            p99,
            max,
            total_count,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Binary search for the bucket index whose upper bound >= ns.
#[allow(dead_code)]
fn bucket_index(ns: u64) -> usize {
    BUCKET_BOUNDS_NS
        .partition_point(|&bound| bound < ns)
        .min(NUM_BUCKETS - 1)
}

/// Compute the p-th percentile from bucket counts. Returns the bucket
/// boundary (in ns) that contains the p-th percentile value.
#[allow(dead_code)]
fn percentile(bucket_counts: &[u64; NUM_BUCKETS], count: u64, p: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let target = count * p / 100;
    let mut cumulative = 0u64;
    for (i, &bc) in bucket_counts.iter().enumerate() {
        cumulative += bc;
        if cumulative >= target {
            return BUCKET_BOUNDS_NS[i];
        }
    }
    BUCKET_BOUNDS_NS[NUM_BUCKETS - 1]
}

/// Find the highest non-empty bucket's upper bound.
#[allow(dead_code)]
fn max_latency(bucket_counts: &[u64; NUM_BUCKETS]) -> u64 {
    for (i, &bc) in bucket_counts.iter().enumerate().rev() {
        if bc > 0 {
            return BUCKET_BOUNDS_NS[i];
        }
    }
    0
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub avg: u64,
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
    pub total_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_p50_p99_with_known_distribution() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        // 100 observations at ~500µs (500_000 ns) → bucket index 3 (bound 500_000)
        for _ in 0..100 {
            h.observe(500_000);
        }
        let s = h.flush();
        assert_eq!(s.count, 100);
        assert_eq!(s.total_count, 100);
        // p50 and p99 should be in the 500µs bucket
        assert_eq!(s.p50, 500_000);
        assert_eq!(s.p99, 500_000);
        assert_eq!(s.max, 500_000);
    }

    #[test]
    fn histogram_mixed_distribution() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        // 80 fast (1µs), 20 slow (10ms)
        for _ in 0..80 {
            h.observe(1_000);
        }
        for _ in 0..20 {
            h.observe(10_000_000);
        }
        let s = h.flush();
        assert_eq!(s.count, 100);
        // p50 should fall in the 1µs bucket (first 80 are at 1µs)
        assert_eq!(s.p50, 1_000);
        // p99 should fall in the 10ms bucket (cumulative at 1µs = 80, target = 99)
        assert_eq!(s.p99, 10_000_000);
        assert_eq!(s.max, 10_000_000);
    }

    #[test]
    fn histogram_window_resets_after_flush() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        h.observe(1_000);
        h.observe(2_000);
        let s1 = h.flush();
        assert_eq!(s1.count, 2);
        assert_eq!(s1.total_count, 2);

        let s2 = h.flush();
        assert_eq!(s2.count, 0);
        assert_eq!(s2.p50, 0);
        assert_eq!(s2.total_count, 2); // total accumulates
    }

    #[test]
    fn histogram_snapshot_does_not_reset() {
        let h = LatencyHistogram::new(MetricName::Static("test.lh"));
        h.observe(100_000);
        let s = h.snapshot();
        assert_eq!(s.count, 1);
        let s2 = h.snapshot();
        assert_eq!(s2.count, 1);
    }

    #[test]
    fn bucket_index_correctness() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1_000), 0);
        assert_eq!(bucket_index(1_001), 1);
        assert_eq!(bucket_index(10_000), 1);
        assert_eq!(bucket_index(10_001), 2);
        assert_eq!(bucket_index(u64::MAX), 11);
    }
}
