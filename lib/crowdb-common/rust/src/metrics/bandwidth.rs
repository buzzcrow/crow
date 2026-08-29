// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricName;

/// Bandwidth metric: tracks count, byte sum (for avg size), and total bytes.
///
/// `observe(bytes)` increments count and adds to both `sum` (window) and
/// `total_bytes` (cumulative). On flush, `sum` and `count` are reset;
/// `total_bytes` accumulates.
#[allow(dead_code)]
#[derive(Debug)]
pub struct Bandwidth {
    name: MetricName,
    count: AtomicU64,
    sum: AtomicU64,
    total_bytes: AtomicU64,
}

#[allow(dead_code)]
impl Bandwidth {
    #[must_use]
    pub fn new(name: MetricName) -> Self {
        Self {
            name,
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        }
    }

    /// Record one observation of `bytes` size.
    pub fn observe(&self, bytes: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(bytes, Ordering::Relaxed);
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Snapshot window values and reset them. Returns count, `avg_size` (bytes),
    /// and rate (bytes/sec) computed from `window_secs`.
    pub fn flush(&self, window_secs: f64) -> BandwidthSnapshot {
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let avg_size = sum.checked_div(count).unwrap_or(0);
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rate = if window_secs > 0.0 {
            (sum as f64 / window_secs) as u64
        } else {
            0
        };
        BandwidthSnapshot {
            count,
            avg_size,
            rate,
            total_bytes,
        }
    }

    /// Current values without resetting.
    pub fn snapshot(&self, window_secs: f64) -> BandwidthSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let avg_size = sum.checked_div(count).unwrap_or(0);
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rate = if window_secs > 0.0 {
            (sum as f64 / window_secs) as u64
        } else {
            0
        };
        BandwidthSnapshot {
            count,
            avg_size,
            rate,
            total_bytes,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BandwidthSnapshot {
    pub count: u64,
    pub avg_size: u64,
    pub rate: u64,
    pub total_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bandwidth_basic_flush() {
        let bw = Bandwidth::new(MetricName::Static("test.bw"));
        for _ in 0..10 {
            bw.observe(100);
        }
        let s = bw.flush(0.5);
        assert_eq!(s.count, 10);
        assert_eq!(s.avg_size, 100);
        assert_eq!(s.rate, 2000); // 1000 bytes / 0.5s = 2000 bytes/s
        assert_eq!(s.total_bytes, 1000);
    }

    #[test]
    fn bandwidth_window_resets_after_flush() {
        let bw = Bandwidth::new(MetricName::Static("test.bw"));
        bw.observe(50);
        bw.observe(150);
        let s1 = bw.flush(1.0);
        assert_eq!(s1.count, 2);
        assert_eq!(s1.avg_size, 100);
        assert_eq!(s1.total_bytes, 200);

        let s2 = bw.flush(1.0);
        assert_eq!(s2.count, 0);
        assert_eq!(s2.avg_size, 0);
        assert_eq!(s2.rate, 0);
        assert_eq!(s2.total_bytes, 200); // total accumulates
    }

    #[test]
    fn bandwidth_snapshot_does_not_reset() {
        let bw = Bandwidth::new(MetricName::Static("test.bw"));
        bw.observe(200);
        let s = bw.snapshot(1.0);
        assert_eq!(s.count, 1);
        assert_eq!(s.avg_size, 200);
        let s2 = bw.snapshot(1.0);
        assert_eq!(s2.count, 1);
    }
}
