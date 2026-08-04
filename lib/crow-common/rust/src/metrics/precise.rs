// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! High-precision latency histogram for benchmark reporting.
//!
//! [`PreciseHistogram`] uses an HDR-style logarithmic bucket scheme that
//! delivers ≥3 significant digits of percentile precision — the same
//! precision `hdrhistogram::Histogram<u64>` provides at `sig_digits=3` —
//! without an external dependency. It is the precise counterpart to the
//! fixed-bucket [`super::LatencyHistogram`], which stays the zero-alloc,
//! lock-free server hot-path option.
//!
//! All mutating methods take `&mut self`: every call site has exclusive
//! access (the client library guards its window histograms with a
//! `Mutex`, the bench `OpStats` is per-worker owned, and the cumulative
//! histograms are owned by the flusher task). A lock-free HDR impl would
//! add atomic-bucket-array and CAS-resize complexity for no benefit here.

/// Number of linear sub-buckets per power-of-2 magnitude. For 3
/// significant digits: `2^ceil(log2(10^3)) = 2^10 = 1024`. Relative
/// error within a sub-bucket is `1/1024 ≈ 0.098%`.
const SUB_BUCKET_COUNT: usize = 1024;
const SUB_BUCKET_SHIFT: u32 = 10; // log2(SUB_BUCKET_COUNT)

/// Highest trackable value (microseconds). `2^32 - 1 ≈ 71 minutes` — far
/// beyond any realistic bench latency — so `auto(true)` is a no-op: the
/// pre-allocated range already covers everything. Values above this clamp
/// to the top sub-bucket.
const MAX_TRACKABLE: u64 = u32::MAX as u64;

/// Number of power-of-2 magnitudes needed to cover `[0, 2^32)`.
/// Bucket 0 = `[0, 1024)` (linear, width 1); bucket `b>=1` =
/// `[1024<<(b-1), 1024<<b)` (width `1<<(b-1)`). The highest set bit of
/// `2^32-1` is position 31, so the largest bucket index is
/// `31 - (SUB_BUCKET_SHIFT - 1) = 22`; `NUM_BUCKETS = 23`.
const NUM_BUCKETS: usize = 23;

/// HDR-style precise latency histogram. Records values in microseconds;
/// reports percentiles at ≥3 significant digits.
#[derive(Debug, Clone)]
pub struct PreciseHistogram {
    counts: Vec<u64>,
    count: u64,
    sum: u64,
    min_value: u64,
    max_value: u64,
}

impl PreciseHistogram {
    /// Create a histogram with `sig_digits` significant digits of
    /// precision. Only `3` is currently supported (the precision the bench
    /// and client library require); other values panic to keep the bucket
    /// layout compile-time fixed.
    ///
    /// # Panics
    /// Panics if `sig_digits != 3`.
    #[must_use]
    pub fn new(sig_digits: u8) -> Self {
        assert!(
            sig_digits == 3,
            "PreciseHistogram currently supports only 3 significant digits"
        );
        Self {
            counts: vec![0; NUM_BUCKETS * SUB_BUCKET_COUNT],
            count: 0,
            sum: 0,
            min_value: 0,
            max_value: 0,
        }
    }

    /// No-op retained for API compatibility with `hdrhistogram`'s
    /// `auto(true)`. The pre-allocated range (`2^32 µs`) already covers
    /// every realistic bench latency, so auto-resize is unnecessary.
    pub fn auto(&mut self, _enabled: bool) {}

    /// Record a latency observation in microseconds. Values are clamped to
    /// `[1, MAX_TRACKABLE]`; a zero value is floored to 1 (matching the
    /// bench/client convention of `lat_us.max(1)`).
    pub fn record(&mut self, mut value: u64) {
        if value == 0 {
            value = 1;
        }
        if value > MAX_TRACKABLE {
            value = MAX_TRACKABLE;
        }
        let (b, s) = index_of(value);
        self.counts[b * SUB_BUCKET_COUNT + s] += 1;
        self.count += 1;
        self.sum += value;
        if self.min_value == 0 || value < self.min_value {
            self.min_value = value;
        }
        if value > self.max_value {
            self.max_value = value;
        }
    }

    /// Merge another histogram's counts into this one. `min`/`max`/`sum`
    /// are combined so queries on the merged histogram match a histogram
    /// recorded with the union of both value sets.
    pub fn add(&mut self, other: &Self) {
        for (dst, &src) in self.counts.iter_mut().zip(other.counts.iter()) {
            *dst += src;
        }
        self.count += other.count;
        self.sum += other.sum;
        if other.count > 0 {
            if self.min_value == 0 || other.min_value < self.min_value {
                self.min_value = other.min_value;
            }
            if other.max_value > self.max_value {
                self.max_value = other.max_value;
            }
        }
    }

    /// Clear all observations.
    pub fn reset(&mut self) {
        self.counts.fill(0);
        self.count = 0;
        self.sum = 0;
        self.min_value = 0;
        self.max_value = 0;
    }

    /// `true` if no observations have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Total number of observations.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Minimum recorded value (exact, not bucketed). `0` if empty.
    #[must_use]
    pub fn min(&self) -> u64 {
        self.min_value
    }

    /// Maximum recorded value (exact, not bucketed). `0` if empty.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.max_value
    }

    /// Arithmetic mean of recorded values. `0.0` if empty.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }

    /// Value at quantile `q` (in `[0.0, 1.0]`). Returns the lower bound of
    /// the sub-bucket containing the `q`-th percentile, accurate to within
    /// one sub-bucket width (≤0.1% at 3 significant digits). Returns `0`
    /// if empty.
    #[must_use]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn value_at_quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let q = q.clamp(0.0, 1.0);
        let target = (q * self.count as f64).ceil() as u64; // 1-indexed rank
        let target = target.max(1).min(self.count);
        let mut cumulative: u64 = 0;
        for b in 0..NUM_BUCKETS {
            let base = b * SUB_BUCKET_COUNT;
            for s in 0..SUB_BUCKET_COUNT {
                cumulative += self.counts[base + s];
                if cumulative >= target {
                    return value_for_index(b, s);
                }
            }
        }
        self.max_value
    }
}

impl Default for PreciseHistogram {
    fn default() -> Self {
        Self::new(3)
    }
}

/// Compute the `(bucket, sub_bucket)` index for `value` (assumed in
/// `[1, MAX_TRACKABLE]`).
#[allow(clippy::cast_possible_truncation)]
fn index_of(value: u64) -> (usize, usize) {
    debug_assert!((1..=MAX_TRACKABLE).contains(&value));
    if value < SUB_BUCKET_COUNT as u64 {
        return (0, value as usize);
    }
    let v = value as u32;
    let pow2 = v.ilog2(); // floor(log2(v))
    let bucket = (pow2 - (SUB_BUCKET_SHIFT - 1)) as usize;
    let base = (SUB_BUCKET_COUNT as u64) << (bucket - 1);
    let sub = ((value - base) >> (bucket - 1)) as usize;
    (bucket, sub)
}

/// Lower bound of the sub-bucket at `(bucket, sub)`.
fn value_for_index(bucket: usize, sub: usize) -> u64 {
    if bucket == 0 {
        return sub as u64;
    }
    let base = (SUB_BUCKET_COUNT as u64) << (bucket - 1);
    base + ((sub as u64) << (bucket - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::float_cmp)]
    fn empty_histogram_queries_return_zero() {
        let h = PreciseHistogram::new(3);
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.mean(), 0.0);
        assert_eq!(h.value_at_quantile(0.5), 0);
    }

    #[test]
    fn known_distribution_p50_p99() {
        let mut h = PreciseHistogram::new(3);
        for _ in 0..80 {
            h.record(1); // 1µs
        }
        for _ in 0..20 {
            h.record(10_000); // 10ms
        }
        assert_eq!(h.len(), 100);
        // p50: 50th value is in the 1µs group.
        assert_eq!(h.value_at_quantile(0.50), 1);
        // p99: 99th value is in the 10ms group.
        assert_eq!(h.value_at_quantile(0.99), 10_000);
        assert_eq!(h.max(), 10_000);
        assert_eq!(h.min(), 1);
    }

    #[test]
    #[allow(clippy::cast_possible_wrap)]
    fn percentile_precision_three_sig_digits() {
        // Uniform distribution over [1, 1_000_000]: true p50 = 500_000,
        // p90 = 900_000, p99 = 990_000. With 1024 sub-buckets per
        // magnitude, the sub-bucket width near 1e6 is 1024µs, so the
        // reported value must be within ~0.1% of the true quantile.
        let mut h = PreciseHistogram::new(3);
        for v in 1..=1_000_000u64 {
            h.record(v);
        }
        let p50 = h.value_at_quantile(0.50);
        let p90 = h.value_at_quantile(0.90);
        let p99 = h.value_at_quantile(0.99);
        let within = |got: u64, want: u64| (got as i64 - want as i64).unsigned_abs() <= want / 1000;
        assert!(within(p50, 500_000), "p50={p50}");
        assert!(within(p90, 900_000), "p90={p90}");
        assert!(within(p99, 990_000), "p99={p99}");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn add_and_clone_merge_correctness() {
        let mut a = PreciseHistogram::new(3);
        for v in [1u64, 2, 3, 1_000] {
            a.record(v);
        }
        let mut b = PreciseHistogram::new(3);
        for v in [5u64, 10_000, 10_000] {
            b.record(v);
        }
        let mut merged = a.clone();
        merged.add(&b);

        let mut single = PreciseHistogram::new(3);
        for v in [1u64, 2, 3, 1_000, 5, 10_000, 10_000] {
            single.record(v);
        }
        assert_eq!(merged.len(), single.len());
        assert_eq!(merged.min(), single.min());
        assert_eq!(merged.max(), single.max());
        assert_eq!(merged.mean(), single.mean());
        assert_eq!(merged.value_at_quantile(0.50), single.value_at_quantile(0.50));
        assert_eq!(merged.value_at_quantile(0.99), single.value_at_quantile(0.99));
    }

    #[test]
    fn record_clamps_above_max_to_top_bucket() {
        let mut h = PreciseHistogram::new(3);
        h.record(u64::MAX / 2); // far above MAX_TRACKABLE
        assert_eq!(h.len(), 1);
        assert_eq!(h.max(), MAX_TRACKABLE);
    }

    #[test]
    fn record_floors_zero_to_one() {
        let mut h = PreciseHistogram::new(3);
        h.record(0);
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 1);
    }

    #[test]
    fn reset_clears_all() {
        let mut h = PreciseHistogram::new(3);
        h.record(5);
        h.record(100);
        h.reset();
        assert!(h.is_empty());
        assert_eq!(h.value_at_quantile(0.5), 0);
    }

    #[test]
    fn index_of_boundaries() {
        assert_eq!(index_of(1), (0, 1));
        assert_eq!(index_of(1023), (0, 1023));
        assert_eq!(index_of(1024), (1, 0));
        assert_eq!(index_of(1025), (1, 1));
        assert_eq!(index_of(2047), (1, 1023));
        assert_eq!(index_of(2048), (2, 0));
        assert_eq!(index_of(4096), (3, 0));
    }

    #[test]
    fn value_for_index_round_trips_lower_bound() {
        // The lower bound of the sub-bucket that a value lands in must
        // not exceed the value.
        for &v in &[1u64, 1023, 1024, 1025, 2047, 2048, 2050, 4096, 1_000_000] {
            let (b, s) = index_of(v);
            let lb = value_for_index(b, s);
            assert!(lb <= v, "v={v} lb={lb}");
        }
    }
}
