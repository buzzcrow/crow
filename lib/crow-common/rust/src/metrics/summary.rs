// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricName;

/// Lightweight latency summary: tracks count, sum, max, and `total_count`.
///
/// `observe(ns)` does `fetch_add` on `count`/`sum`/`total_count` and a
/// `compare_exchange` loop on max. No min (rarely useful operationally).
/// No allocation, no locks.
#[allow(dead_code)]
#[derive(Debug)]
pub struct LatencySummary {
    name: MetricName,
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
    total_count: AtomicU64,
}

#[allow(dead_code)]
impl LatencySummary {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
        }
    }

    /// Record a latency observation in nanoseconds.
    pub fn observe(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(ns, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);

        // CAS loop to update max
        let mut current_max = self.max.load(Ordering::Relaxed);
        while ns > current_max {
            match self
                .max
                .compare_exchange_weak(current_max, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Snapshot and reset window state. Returns count, avg (ns), max (ns),
    /// and `total_count`.
    pub fn flush(&self) -> SummarySnapshot {
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let max = self.max.swap(0, Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);
        let avg = sum.checked_div(count).unwrap_or(0);
        SummarySnapshot {
            count,
            avg,
            max,
            total_count,
        }
    }

    /// Current values without resetting.
    pub fn snapshot(&self) -> SummarySnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let max = self.max.load(Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);
        let avg = sum.checked_div(count).unwrap_or(0);
        SummarySnapshot {
            count,
            avg,
            max,
            total_count,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SummarySnapshot {
    pub count: u64,
    pub avg: u64,
    pub max: u64,
    pub total_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_avg_and_max() {
        let s = LatencySummary::new(MetricName::Static("test.l"));
        s.observe(100);
        s.observe(200);
        s.observe(300);
        let snap = s.flush();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.avg, 200); // (100+200+300)/3
        assert_eq!(snap.max, 300);
        assert_eq!(snap.total_count, 3);
    }

    #[test]
    fn summary_max_resets_after_flush() {
        let s = LatencySummary::new(MetricName::Static("test.l"));
        s.observe(500);
        let s1 = s.flush();
        assert_eq!(s1.max, 500);

        let s2 = s.flush();
        assert_eq!(s2.count, 0);
        assert_eq!(s2.avg, 0);
        assert_eq!(s2.max, 0);
        assert_eq!(s2.total_count, 1); // total accumulates
    }

    #[test]
    fn summary_snapshot_does_not_reset() {
        let s = LatencySummary::new(MetricName::Static("test.l"));
        s.observe(42);
        let snap = s.snapshot();
        assert_eq!(snap.count, 1);
        assert_eq!(snap.avg, 42);
        let snap2 = s.snapshot();
        assert_eq!(snap2.count, 1);
    }

    #[test]
    fn summary_concurrent_max_update() {
        let s = LatencySummary::new(MetricName::Static("test.l"));
        s.observe(100);
        s.observe(50); // should not update max
        s.observe(200);
        let snap = s.flush();
        assert_eq!(snap.max, 200);
    }
}
