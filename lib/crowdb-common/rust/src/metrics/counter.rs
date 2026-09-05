// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricName;

/// Monotonic counter with window delta and cumulative total.
///
/// `window` is reset to 0 on each flush; `total` accumulates forever.
/// Both are `AtomicU64` updated via `fetch_add(1, Relaxed)` — no locks,
/// no allocation.
#[allow(dead_code)]
#[derive(Debug)]
pub struct Counter {
    name: MetricName,
    window: AtomicU64,
    total: AtomicU64,
}

#[allow(dead_code)]
impl Counter {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            window: AtomicU64::new(0),
            total: AtomicU64::new(0),
        }
    }

    /// Increment by 1.
    pub fn inc(&self) {
        self.window.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment by `n`.
    pub fn inc_by(&self, n: u64) {
        self.window.fetch_add(n, Ordering::Relaxed);
        self.total.fetch_add(n, Ordering::Relaxed);
    }

    /// Snapshot the window delta and total, then reset window to 0.
    pub fn flush(&self) -> CounterSnapshot {
        let count = self.window.swap(0, Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);
        CounterSnapshot { count, total }
    }

    /// Current window + total without resetting.
    pub fn snapshot(&self) -> CounterSnapshot {
        let count = self.window.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);
        CounterSnapshot { count, total }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CounterSnapshot {
    pub count: u64,
    pub total: u64,
}

/// Gauge: current state, can go up or down. Holds a plain count.
#[allow(dead_code)]
#[derive(Debug)]
pub struct Gauge {
    name: MetricName,
    value: AtomicU64,
}

#[allow(dead_code)]
impl Gauge {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            value: AtomicU64::new(0),
        }
    }

    /// Set the current value.
    pub fn set(&self, v: u64) {
        self.value.store(v, Ordering::Relaxed);
    }

    /// Increment the gauge atomically.
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the gauge atomically without wrapping below zero.
    pub fn dec(&self) {
        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            });
    }

    /// Read the current value.
    pub fn snapshot(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_window_reset_and_total_accumulate() {
        let c = Counter::new(MetricName::Static("test.c"));
        c.inc();
        c.inc_by(9);
        let s1 = c.flush();
        assert_eq!(s1.count, 10);
        assert_eq!(s1.total, 10);

        // No new increments — window should be 0, total unchanged.
        let s2 = c.flush();
        assert_eq!(s2.count, 0);
        assert_eq!(s2.total, 10);

        // More increments — window has delta, total accumulates.
        c.inc_by(5);
        let s3 = c.flush();
        assert_eq!(s3.count, 5);
        assert_eq!(s3.total, 15);
    }

    #[test]
    fn counter_snapshot_does_not_reset() {
        let c = Counter::new(MetricName::Static("test.c"));
        c.inc_by(7);
        let s = c.snapshot();
        assert_eq!(s.count, 7);
        assert_eq!(s.total, 7);
        // Window not reset.
        let s2 = c.snapshot();
        assert_eq!(s2.count, 7);
    }

    #[test]
    fn gauge_reports_last_value() {
        let g = Gauge::new(MetricName::Static("test.g"));
        g.set(42);
        assert_eq!(g.snapshot(), 42);
        g.set(0);
        assert_eq!(g.snapshot(), 0);
        g.set(100);
        assert_eq!(g.snapshot(), 100);
    }
}
