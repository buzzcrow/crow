// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! JSON-serializable bench report.
//!
//! Key work: percentile extraction from `hdrhistogram::Histogram<u64>`,
//! a lossless round-trippable struct, helpers to read/write the report
//! file under `~/.crowkv/bench/<run-id>.json`.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use super::workload::{OpKind, WorkloadKind};

/// Latency percentiles (microseconds). Stored as `u64` because the
/// histograms are recorded in microsecond units.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct Percentiles {
    pub min_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

impl Percentiles {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            min_us: 0,
            p50_us: 0,
            p90_us: 0,
            p99_us: 0,
            p999_us: 0,
            max_us: 0,
        }
    }
}

/// Extract a fixed set of percentiles from an HDR histogram.
#[must_use]
pub fn percentiles_from_histogram(h: &Histogram<u64>) -> Percentiles {
    if h.is_empty() {
        return Percentiles::empty();
    }
    Percentiles {
        min_us: h.min(),
        p50_us: h.value_at_quantile(0.50),
        p90_us: h.value_at_quantile(0.90),
        p99_us: h.value_at_quantile(0.99),
        p999_us: h.value_at_quantile(0.999),
        max_us: h.max(),
    }
}

/// Per-op-kind stats block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpReport {
    pub ops: u64,
    pub errors: u64,
    pub not_found: u64,
    pub latency_us: Percentiles,
}

/// Top-level bench result. Stored as JSON under
/// `~/.crowkv/bench/<run-id>.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchReport {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    /// Wall-clock duration of the **measurement** window only —
    /// `total run wall time - warmup_ms`. Older reports without an
    /// explicit warmup field had `warmup_ms == 0` so this stays equal
    /// to the actual run length, preserving back-compat.
    pub duration_ms: u64,
    /// Wall-clock duration of the warmup window. `0` when warmup was
    /// not configured (i.e. all ops counted toward the report).
    /// Defaults to `0` on read-back of historical JSON files via
    /// serde's default-fill, so existing reports keep deserializing.
    #[serde(default)]
    pub warmup_ms: u64,
    pub workload: WorkloadKind,
    pub connections: u32,
    pub threads: u32,
    pub key_space: u64,
    pub value_size: usize,
    pub target_endpoint: String,
    pub store_id: u64,
    pub group_id: u64,
    pub total_ops: u64,
    pub total_errors: u64,
    pub error_rate: f64,
    pub by_op: BTreeMap<String, OpReport>,
}

impl BenchReport {
    /// Default report location: `~/.crowkv/bench/`.
    #[must_use]
    pub fn default_dir() -> Option<PathBuf> {
        dirs::home_dir().map(|h| h.join(".crowkv").join("bench"))
    }

    /// Write the report as pretty JSON to `<dir>/<run-id>.json`. The
    /// directory is created if missing.
    ///
    /// # Errors
    /// I/O or serialization errors.
    pub fn write_to(&self, dir: &Path) -> io::Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!("{}.json", self.run_id));
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(&path, json)?;
        Ok(path)
    }

    /// Read a previously-written report from disk.
    ///
    /// # Errors
    /// I/O or JSON-parse errors.
    pub fn read_from(path: &Path) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }

    /// Pretty multi-line summary suitable for the `bench report` CLI.
    #[must_use]
    pub fn human_summary(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "run_id          : {}\nworkload        : {:?}\nduration        : {} ms (measurement)\nwarmup          : {} ms (discarded)\nconnections     : {}\nthreads         : {}\nkey_space       : {}\nvalue_size      : {} B\ntarget          : {} (store={} group={})\ntotal_ops       : {}\ntotal_errors    : {}\nerror_rate      : {:.4}",
            self.run_id,
            self.workload,
            self.duration_ms,
            self.warmup_ms,
            self.connections,
            self.threads,
            self.key_space,
            self.value_size,
            self.target_endpoint,
            self.store_id,
            self.group_id,
            self.total_ops,
            self.total_errors,
            self.error_rate,
        );
        for (kind, op) in &self.by_op {
            let p = &op.latency_us;
            let _ = writeln!(
                out,
                "{kind:>8}: ops={ops} err={err} nf={nf}  p50={p50}us p99={p99}us p999={p999}us max={max}us",
                kind = kind,
                ops = op.ops,
                err = op.errors,
                nf = op.not_found,
                p50 = p.p50_us,
                p99 = p.p99_us,
                p999 = p.p999_us,
                max = p.max_us,
            );
        }
        out
    }
}

/// Builder for accumulating one op kind worth of stats.
#[derive(Debug)]
pub struct OpStats {
    pub ops: u64,
    pub errors: u64,
    pub not_found: u64,
    pub histogram: Histogram<u64>,
}

impl OpStats {
    /// Build an empty `OpStats` with an auto-resizing HDR histogram at
    /// 3 significant digits. Auto-resize means the upper bound grows on
    /// demand instead of saturating, so pathological tail latencies are
    /// reported faithfully at the cost of a few extra allocations.
    ///
    /// # Panics
    /// Panics if `Histogram::new` rejects the precision (it shouldn't
    /// with `3`).
    #[must_use]
    pub fn new() -> Self {
        let mut histogram = Histogram::<u64>::new(3).expect("hdr histogram precision");
        histogram.auto(true);
        Self {
            ops: 0,
            errors: 0,
            not_found: 0,
            histogram,
        }
    }

    pub fn record(&mut self, latency_us: u64, ok: bool, not_found: bool) {
        self.ops += 1;
        if !ok {
            self.errors += 1;
        }
        if not_found {
            self.not_found += 1;
        }
        // Floor at 1us; auto-resize handles the upper bound.
        let v = latency_us.max(1);
        let _ = self.histogram.record(v);
    }

    /// Merge another `OpStats` into this one (used by the runner to reduce
    /// per-worker stats).
    ///
    /// # Panics
    /// Panics if the underlying HDR histogram addition fails (it shouldn't
    /// with equal bounds).
    pub fn merge(&mut self, other: &Self) {
        self.ops += other.ops;
        self.errors += other.errors;
        self.not_found += other.not_found;
        self.histogram.add(&other.histogram).expect("histogram add");
    }

    #[must_use]
    pub fn into_report(self) -> OpReport {
        OpReport {
            ops: self.ops,
            errors: self.errors,
            not_found: self.not_found,
            latency_us: percentiles_from_histogram(&self.histogram),
        }
    }
}

impl Default for OpStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper for the runner: build the per-op map keyed by `OpKind::label`.
#[must_use]
pub fn per_op_map(stats: BTreeMap<OpKind, OpStats>) -> BTreeMap<String, OpReport> {
    stats
        .into_iter()
        .map(|(k, v)| (k.label().to_string(), v.into_report()))
        .collect()
}
