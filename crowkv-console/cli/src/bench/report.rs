// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Bench report: Markdown-writable, JSON-serializable struct.
//!
//! Key work: percentile extraction from `hdrhistogram::Histogram<u64>`,
//! a lossless round-trippable struct, helpers to read/write the report
//! file under `bench-runs/<datetime>/report.md` (Markdown) or
//! `bench-runs/<datetime>/report.json` (JSON, for `--json` mode).
//!
//! The `bench-runs/` directory is always at the **project root**
//! (found by walking up from CWD for `pixi.toml`), never inside a
//! crate directory. Each run gets its own datetime-stamped subdirectory
//! (`YYYY-MM-DD_HHMMSS`) so runs never collide.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use super::workload::{OpKind, WorkloadKind};

/// Walk up from CWD to find the project root (the directory containing
/// `pixi.toml`). Falls back to CWD if not found.
#[must_use]
fn project_root() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut dir: &Path = &cwd;
    loop {
        if dir.join("pixi.toml").exists() {
            return dir.to_path_buf();
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return cwd,
        }
    }
}

/// Top-level bench-runs directory: `<project_root>/bench-runs/`.
#[must_use]
fn bench_runs_root() -> PathBuf {
    project_root().join("bench-runs")
}

/// Generate a datetime-stamped run directory name: `YYYY-MM-DD_HHMMSS`.
#[must_use]
fn timestamp_dir_name(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d_%H%M%S").to_string()
}

/// Latency percentiles (microseconds). Stored as `u64` because the
/// histograms are recorded in microsecond units.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct Percentiles {
    pub min_us: u64,
    /// Arithmetic mean latency. `#[serde(default)]` so historical reports
    /// written before this field existed still deserialize (defaulting to
    /// 0 on read-back).
    #[serde(default)]
    pub avg_us: u64,
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
            avg_us: 0,
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
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn percentiles_from_histogram(h: &Histogram<u64>) -> Percentiles {
    if h.is_empty() {
        return Percentiles::empty();
    }
    Percentiles {
        min_us: h.min(),
        avg_us: h.mean() as u64,
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
    /// Server-side metrics aggregated across all deployed nodes (R10
    /// benchmark framework). `#[serde(default)]` so historical reports
    /// written before this field existed still deserialize.
    #[serde(default)]
    pub server_metrics: ServerMetrics,
}

impl BenchReport {
    /// Create a new run directory under `<project_root>/bench-runs/<datetime>/`
    /// and return its path. The directory is created on disk.
    ///
    /// # Errors
    /// I/O errors from `create_dir_all`.
    pub fn create_run_dir(at: DateTime<Utc>) -> io::Result<PathBuf> {
        let dir = bench_runs_root().join(timestamp_dir_name(at));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    /// Scan `bench-runs/*/` for a subdirectory whose name contains `tag`
    /// (case-insensitive). Returns the path to the first match, or `None`.
    /// Used by `bench report` and `bench compare` to locate runs by
    /// partial datetime or run-id.
    #[must_use]
    pub fn find_run_dir(tag: &str) -> Option<PathBuf> {
        let root = bench_runs_root();
        let Ok(entries) = fs::read_dir(&root) else {
            return None;
        };
        let tag_lower = tag.to_ascii_lowercase();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.to_ascii_lowercase().contains(&tag_lower) {
                return Some(entry.path());
            }
        }
        None
    }

    /// Write the report as Markdown to `<run_dir>/report.md`.
    ///
    /// # Errors
    /// I/O or serialization errors.
    pub fn write_to(&self, run_dir: &Path) -> io::Result<PathBuf> {
        let path = run_dir.join("report.md");
        fs::write(&path, self.to_markdown())?;
        Ok(path)
    }

    /// Write the report as pretty JSON to `<run_dir>/report.json`.
    /// Used by the `--json` CLI flag.
    ///
    /// # Errors
    /// I/O or serialization errors.
    #[expect(dead_code, reason = "used by --json flag in future CLI expansion")]
    pub fn write_json_to(&self, run_dir: &Path) -> io::Result<PathBuf> {
        let path = run_dir.join("report.json");
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(&path, json)?;
        Ok(path)
    }

    /// Read a previously-written Markdown report from disk.
    ///
    /// # Errors
    /// I/O or parse errors.
    pub fn read_from(path: &Path) -> io::Result<Self> {
        let content = fs::read_to_string(path)?;
        parse_markdown_report(&content)
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
                "{kind:>8}: ops={ops} err={err} nf={nf}  avg={avg}us p50={p50}us p99={p99}us p999={p999}us max={max}us",
                kind = kind,
                ops = op.ops,
                err = op.errors,
                nf = op.not_found,
                avg = p.avg_us,
                p50 = p.p50_us,
                p99 = p.p99_us,
                p999 = p.p999_us,
                max = p.max_us,
            );
        }
        let sm = &self.server_metrics;
        let _ = writeln!(
            out,
            "server_metrics  : wal_append={wal_append} kv_put={kv_put} kv_get={kv_get}\nsystem          : cpu_user={cpu_user}us cpu_sys={cpu_sys}us rss={rss}KB tcp_retrans={tcp_retrans} tcp_lost={tcp_lost}",
            wal_append = sm.wal_append_count,
            kv_put = sm.kv_put_count,
            kv_get = sm.kv_get_count,
            cpu_user = sm.system.cpu_user_us,
            cpu_sys = sm.system.cpu_sys_us,
            rss = sm.system.rss_kb,
            tcp_retrans = sm.system.tcp_retransmits,
            tcp_lost = sm.system.tcp_lost,
        );
        out
    }

    /// Render the report as a human-readable Markdown document with tables.
    #[must_use]
    pub fn to_markdown(&self) -> String {
        use std::fmt::Write;
        let mut md = String::new();

        let _ = writeln!(md, "# Benchmark Report: {}", self.run_id);
        let _ = writeln!(md);
        let _ = writeln!(
            md,
            "- **Started**: {}",
            self.started_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(
            md,
            "- **Finished**: {}",
            self.finished_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(md, "- **Duration**: {} ms (measurement)", self.duration_ms);
        let _ = writeln!(md, "- **Warmup**: {} ms (discarded)", self.warmup_ms);
        let _ = writeln!(md, "- **Workload**: {:?}", self.workload);
        let _ = writeln!(
            md,
            "- **Target**: `{}` (store={}, group={})",
            self.target_endpoint, self.store_id, self.group_id
        );
        let _ = writeln!(md);

        let _ = writeln!(md, "## Configuration");
        let _ = writeln!(md);
        let _ = writeln!(md, "| Parameter | Value |");
        let _ = writeln!(md, "|---|---|");
        let _ = writeln!(md, "| connections | {} |", self.connections);
        let _ = writeln!(md, "| threads | {} |", self.threads);
        let _ = writeln!(md, "| key_space | {} |", self.key_space);
        let _ = writeln!(md, "| value_size | {} B |", self.value_size);
        let _ = writeln!(md);

        #[expect(clippy::cast_precision_loss, reason = "display-only throughput")]
        let secs = (self.duration_ms as f64) / 1000.0;
        #[expect(clippy::cast_precision_loss, reason = "display-only throughput")]
        let qps = if secs > 0.0 {
            self.total_ops as f64 / secs
        } else {
            0.0
        };
        let _ = writeln!(md, "## Summary");
        let _ = writeln!(md);
        let _ = writeln!(md, "| Metric | Value |");
        let _ = writeln!(md, "|---|---|");
        let _ = writeln!(md, "| total_ops | {} |", self.total_ops);
        let _ = writeln!(md, "| throughput | {qps:.1} ops/s |");
        let _ = writeln!(md, "| total_errors | {} |", self.total_errors);
        let _ = writeln!(md, "| error_rate | {:.4} |", self.error_rate);

        let _ = writeln!(md);

        let _ = writeln!(md, "## Per-Operation Latency");
        let _ = writeln!(md);
        let _ = writeln!(
            md,
            "| op | ops | errors | not_found | avg(us) | p50(us) | p90(us) | p99(us) | p999(us) | max(us) |"
        );
        let _ = writeln!(md, "|---|---|---|---|---|---|---|---|---|---|");
        for (kind, op) in &self.by_op {
            let p = &op.latency_us;
            let _ = writeln!(
                md,
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                kind,
                op.ops,
                op.errors,
                op.not_found,
                p.avg_us,
                p.p50_us,
                p.p90_us,
                p.p99_us,
                p.p999_us,
                p.max_us,
            );
        }
        let _ = writeln!(md);

        let sm = &self.server_metrics;
        let _ = writeln!(md, "## Server Metrics");
        let _ = writeln!(md);
        let _ = writeln!(md, "| Metric | Value |");
        let _ = writeln!(md, "|---|---|");
        let _ = writeln!(md, "| wal_append | {} |", sm.wal_append_count);
        let _ = writeln!(md, "| kv_put | {} |", sm.kv_put_count);
        let _ = writeln!(md, "| kv_get | {} |", sm.kv_get_count);
        let _ = writeln!(md);

        let _ = writeln!(md, "### System");
        let _ = writeln!(md);
        let _ = writeln!(md, "| Metric | Value |");
        let _ = writeln!(md, "|---|---|");
        let _ = writeln!(md, "| cpu_user | {} us |", sm.system.cpu_user_us);
        let _ = writeln!(md, "| cpu_sys | {} us |", sm.system.cpu_sys_us);
        let _ = writeln!(md, "| rss | {} KB |", sm.system.rss_kb);
        let _ = writeln!(md, "| tcp_retransmits | {} |", sm.system.tcp_retransmits);
        let _ = writeln!(md, "| tcp_lost | {} |", sm.system.tcp_lost);
        let _ = writeln!(md);

        md
    }
}

/// Server-side perf counters aggregated across all deployed nodes,
/// extracted from each node's `log/metrics.log` file after a bench run
/// (R10 benchmark framework). `#[serde(default)]` on `BenchReport`'s
/// field keeps historical reports without this data deserializable.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ServerMetrics {
    /// Total WAL records appended (from the `*.wal.append.l` latency
    /// summary's per-window counts, summed across the whole run).
    pub wal_append_count: u64,
    /// Total KV put ops (from the `*.kv.put.lh` latency histogram's
    /// per-window counts, summed across the whole run).
    pub kv_put_count: u64,
    /// Total KV get ops (from the `*.kv.get.lh` latency histogram's
    /// per-window counts, summed across the whole run).
    pub kv_get_count: u64,
    /// System resource usage (see `crowkv::metrics::system`).
    pub system: SystemMetrics,
}

/// System resource usage extracted from a node's `metrics.log` "misc"
/// section. CPU time and TCP counters are deltas summed across the run;
/// `rss_kb` is the peak (max) RSS observed during the run.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SystemMetrics {
    pub cpu_user_us: u64,
    pub cpu_sys_us: u64,
    pub rss_kb: u64,
    pub tcp_retransmits: u64,
    pub tcp_lost: u64,
}

/// Section a metrics-log line currently belongs to, tracked while
/// scanning `parse_metrics_log`'s input line by line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogSection {
    None,
    Counter,
    Histogram,
    Summary,
    Bandwidth,
    Gauge,
    Misc,
}

/// Parse a `crowkv-server` `log/metrics.log` file's full contents into a
/// [`ServerMetrics`] summary spanning every flush block in the file.
///
/// Counters/histograms/summaries print only the *window delta* count per
/// flush (see `crowkv::metrics::mod::flush_histograms`/`flush_summaries`),
/// so per-run totals for KV ops and WAL appends are the sum of the count
/// column across every block. `sys.*` CPU/TCP lines are deltas since the
/// previous flush (summed); `sys.rss_kb` is an absolute point-in-time
/// snapshot (tracked as the run's peak).
#[must_use]
pub fn parse_metrics_log(content: &str) -> ServerMetrics {
    let mut metrics = ServerMetrics::default();
    let mut section = LogSection::None;
    let mut peak_rss_kb = 0u64;

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("[metrics ") {
            section = LogSection::None;
            continue;
        }
        if line == "misc" {
            section = LogSection::Misc;
            continue;
        }
        if line.starts_with("name") {
            section = if line.contains("p50") {
                LogSection::Histogram
            } else if line.contains("avg_size") {
                LogSection::Bandwidth
            } else if line.contains("avg(us)") {
                LogSection::Summary
            } else if line.contains("total") {
                LogSection::Counter
            } else if line.contains("value") {
                LogSection::Gauge
            } else {
                LogSection::None
            };
            continue;
        }

        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        match section {
            LogSection::Histogram => {
                let name = fields[0];
                let count: u64 = fields[1].parse().unwrap_or(0);
                if name.contains(".kv.put.") {
                    metrics.kv_put_count += count;
                } else if name.contains(".kv.get.") {
                    metrics.kv_get_count += count;
                }
            }
            LogSection::Summary => {
                let name = fields[0];
                let count: u64 = fields[1].parse().unwrap_or(0);
                if name.contains(".wal.append.") {
                    metrics.wal_append_count += count;
                }
            }
            LogSection::Misc => {
                let value: u64 = fields[1].parse().unwrap_or(0);
                match fields[0] {
                    "sys.cpu_user_us" => metrics.system.cpu_user_us += value,
                    "sys.cpu_sys_us" => metrics.system.cpu_sys_us += value,
                    "sys.rss_kb" => peak_rss_kb = peak_rss_kb.max(value),
                    "sys.tcp_retrans" => metrics.system.tcp_retransmits += value,
                    "sys.tcp_lost" => metrics.system.tcp_lost += value,
                    _ => {}
                }
            }
            LogSection::Counter | LogSection::Bandwidth | LogSection::Gauge | LogSection::None => {}
        }
    }

    metrics.system.rss_kb = peak_rss_kb;
    metrics
}

/// Aggregate per-node [`ServerMetrics`] into a single cluster-wide summary:
/// counters (WAL append, KV put/get) and CPU time are summed across nodes;
/// RSS and TCP retransmit/lost counters take the max across nodes.
#[must_use]
pub fn aggregate_server_metrics(per_node: &[ServerMetrics]) -> ServerMetrics {
    let mut agg = ServerMetrics::default();
    for m in per_node {
        agg.wal_append_count += m.wal_append_count;
        agg.kv_put_count += m.kv_put_count;
        agg.kv_get_count += m.kv_get_count;
        agg.system.cpu_user_us += m.system.cpu_user_us;
        agg.system.cpu_sys_us += m.system.cpu_sys_us;
        agg.system.rss_kb = agg.system.rss_kb.max(m.system.rss_kb);
        agg.system.tcp_retransmits = agg.system.tcp_retransmits.max(m.system.tcp_retransmits);
        agg.system.tcp_lost = agg.system.tcp_lost.max(m.system.tcp_lost);
    }
    agg
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

/// Parse a Markdown report (as written by [`BenchReport::to_markdown`])
/// back into a [`BenchReport`]. This is a lightweight line-based parser
/// that extracts values from the table rows.
///
/// # Errors
/// Returns an `io::Error` if the report is missing the header or
/// required fields.
#[expect(
    clippy::too_many_lines,
    reason = "line-based parser, each field is a single line"
)]
fn parse_markdown_report(content: &str) -> io::Result<BenchReport> {
    let mut report = BenchReport {
        run_id: String::new(),
        started_at: chrono::Utc::now(),
        finished_at: chrono::Utc::now(),
        duration_ms: 0,
        warmup_ms: 0,
        workload: WorkloadKind::Mix,
        connections: 0,
        threads: 0,
        key_space: 0,
        value_size: 0,
        target_endpoint: String::new(),
        store_id: 0,
        group_id: 0,
        total_ops: 0,
        total_errors: 0,
        error_rate: 0.0,
        by_op: BTreeMap::new(),
        server_metrics: ServerMetrics::default(),
    };

    let parse_u = |s: &str| -> u64 { s.trim().parse().unwrap_or(0) };
    let parse_f = |s: &str| -> f64 { s.trim().parse().unwrap_or(0.0) };

    for line in content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("# Benchmark Report: ") {
            report.run_id = rest.trim().to_string();
        } else if let Some(rest) = line.strip_prefix("- **Started**: ") {
            if let Ok(dt) =
                chrono::DateTime::parse_from_str(&rest.replace(" UTC", " +0000"), "%Y-%m-%d %H:%M:%S %z")
            {
                report.started_at = dt.with_timezone(&Utc);
            }
        } else if let Some(rest) = line.strip_prefix("- **Finished**: ") {
            if let Ok(dt) =
                chrono::DateTime::parse_from_str(&rest.replace(" UTC", " +0000"), "%Y-%m-%d %H:%M:%S %z")
            {
                report.finished_at = dt.with_timezone(&Utc);
            }
        } else if let Some(rest) = line.strip_prefix("- **Duration**: ") {
            report.duration_ms = parse_u(rest.split_whitespace().next().unwrap_or("0"));
        } else if let Some(rest) = line.strip_prefix("- **Warmup**: ") {
            report.warmup_ms = parse_u(rest.split_whitespace().next().unwrap_or("0"));
        } else if let Some(rest) = line.strip_prefix("- **Workload**: ") {
            report.workload = match rest.trim() {
                "Read" => WorkloadKind::Read,
                "Write" => WorkloadKind::Write,
                "List" => WorkloadKind::List,
                _ => WorkloadKind::Mix,
            };
        } else if let Some(rest) = line.strip_prefix("- **Target**: ") {
            let rest = rest.trim_start_matches('`');
            let parts: Vec<&str> = rest.splitn(2, '(').collect();
            report.target_endpoint = parts[0].trim().to_string();
            if parts.len() > 1 {
                let inner = parts[1].trim_end_matches(')');
                for pair in inner.split(',') {
                    let kv: Vec<&str> = pair.split('=').collect();
                    if kv.len() == 2 {
                        let key = kv[0].trim();
                        let val = parse_u(kv[1]);
                        if key == "store" {
                            report.store_id = val;
                        } else if key == "group" {
                            report.group_id = val;
                        }
                    }
                }
            }
        } else if line.starts_with("| ")
            && !line.starts_with("|---")
            && !line.starts_with("| Parameter")
            && !line.starts_with("| Metric")
            && !line.starts_with("| op |")
        {
            let raw_cols: Vec<&str> = line.split('|').collect();
            let cols: Vec<String> = raw_cols
                .into_iter()
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if cols.len() < 2 {
                continue;
            }
            match cols[0].as_str() {
                "connections" => report.connections = u32::try_from(parse_u(&cols[1])).unwrap_or(0),
                "threads" => report.threads = u32::try_from(parse_u(&cols[1])).unwrap_or(0),
                "key_space" => report.key_space = parse_u(&cols[1]),
                "value_size" => {
                    report.value_size =
                        usize::try_from(parse_u(cols[1].split_whitespace().next().unwrap_or("0")))
                            .unwrap_or(0);
                }
                "total_ops" => report.total_ops = parse_u(&cols[1]),
                "total_errors" => report.total_errors = parse_u(&cols[1]),
                "error_rate" => report.error_rate = parse_f(&cols[1]),
                "wal_append" => report.server_metrics.wal_append_count = parse_u(&cols[1]),
                "kv_put" => report.server_metrics.kv_put_count = parse_u(&cols[1]),
                "kv_get" => report.server_metrics.kv_get_count = parse_u(&cols[1]),
                "cpu_user" => {
                    report.server_metrics.system.cpu_user_us =
                        parse_u(cols[1].split_whitespace().next().unwrap_or("0"));
                }
                "cpu_sys" => {
                    report.server_metrics.system.cpu_sys_us =
                        parse_u(cols[1].split_whitespace().next().unwrap_or("0"));
                }
                "rss" => {
                    report.server_metrics.system.rss_kb =
                        parse_u(cols[1].split_whitespace().next().unwrap_or("0"));
                }
                "tcp_retransmits" => {
                    report.server_metrics.system.tcp_retransmits = parse_u(&cols[1]);
                }
                "tcp_lost" => {
                    report.server_metrics.system.tcp_lost = parse_u(&cols[1]);
                }
                "read" | "write" if cols.len() >= 10 => {
                    let p = Percentiles {
                        min_us: 0,
                        avg_us: parse_u(&cols[4]),
                        p50_us: parse_u(&cols[5]),
                        p90_us: parse_u(&cols[6]),
                        p99_us: parse_u(&cols[7]),
                        p999_us: parse_u(&cols[8]),
                        max_us: parse_u(&cols[9]),
                    };
                    report.by_op.insert(
                        cols[0].clone(),
                        OpReport {
                            ops: parse_u(&cols[1]),
                            errors: parse_u(&cols[2]),
                            not_found: parse_u(&cols[3]),
                            latency_us: p,
                        },
                    );
                }
                _ => {}
            }
        }
    }

    if report.run_id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing '# Benchmark Report:' header",
        ));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R10: historical `Percentiles` JSON written before `avg_us` existed
    /// must still deserialize, defaulting the new field to 0.
    #[test]
    fn percentiles_deserializes_without_avg_us_field() {
        let old_json = r#"{"min_us":1,"p50_us":2,"p90_us":3,"p99_us":4,"p999_us":5,"max_us":6}"#;
        let p: Percentiles = serde_json::from_str(old_json).unwrap();
        assert_eq!(p.avg_us, 0);
        assert_eq!(p.min_us, 1);
        assert_eq!(p.max_us, 6);
    }

    #[test]
    fn percentiles_from_histogram_computes_mean() {
        let mut h = Histogram::<u64>::new(3).unwrap();
        h.record(10).unwrap();
        h.record(20).unwrap();
        h.record(30).unwrap();
        let p = percentiles_from_histogram(&h);
        assert_eq!(p.avg_us, 20);
        assert_eq!(p.min_us, 10);
        assert_eq!(p.max_us, 30);
    }

    #[test]
    fn percentiles_from_empty_histogram_is_empty() {
        let h = Histogram::<u64>::new(3).unwrap();
        let p = percentiles_from_histogram(&h);
        assert_eq!(p, Percentiles::empty());
    }

    /// R10: `parse_metrics_log` sums histogram/summary window-delta counts
    /// across multiple flush blocks, sums CPU/TCP misc deltas, and tracks
    /// peak RSS — mirroring two ticks of `MetricsRunner::flush`.
    #[test]
    fn parse_metrics_log_sums_across_flush_blocks() {
        let log = "\
[metrics 2026-07-17T00:00:00.000Z window=1s]
name             count  tps(/s)  avg(us)  p50(us)  p99(us)  max(us)
s.1.kv.put.lh       10        10       50       48       90      95
s.1.kv.get.lh        5         5       20       19       30      31
name             count  tps(/s)  avg(us)  max(us)
s.1.g.1.wal.append.l   10        10       12       20
misc
sys.cpu_user_us  1000
sys.cpu_sys_us   200
sys.rss_kb       51200
sys.tcp_retrans  0
sys.tcp_lost     0

[metrics 2026-07-17T00:00:01.000Z window=1s]
name             count  tps(/s)  avg(us)  p50(us)  p99(us)  max(us)
s.1.kv.put.lh       20        20       55       50       99      101
name             count  tps(/s)  avg(us)  max(us)
s.1.g.1.wal.append.l   20        20       13       22
misc
sys.cpu_user_us  1100
sys.cpu_sys_us   210
sys.rss_kb       61440
sys.tcp_retrans  2
sys.tcp_lost     1
";
        let m = parse_metrics_log(log);
        assert_eq!(m.kv_put_count, 30); // 10 + 20, kv.get absent in 2nd block
        assert_eq!(m.kv_get_count, 5);
        assert_eq!(m.wal_append_count, 30); // 10 + 20
        assert_eq!(m.system.cpu_user_us, 2100); // 1000 + 1100
        assert_eq!(m.system.cpu_sys_us, 410); // 200 + 210
        assert_eq!(m.system.rss_kb, 61440); // peak across blocks
        assert_eq!(m.system.tcp_retransmits, 2);
        assert_eq!(m.system.tcp_lost, 1);
    }

    #[test]
    fn parse_metrics_log_empty_content_is_default() {
        assert_eq!(parse_metrics_log(""), ServerMetrics::default());
    }

    #[test]
    fn aggregate_server_metrics_sums_counters_and_maxes_system() {
        let node_a = ServerMetrics {
            wal_append_count: 10,
            kv_put_count: 5,
            kv_get_count: 3,
            system: SystemMetrics {
                cpu_user_us: 100,
                cpu_sys_us: 20,
                rss_kb: 50_000,
                tcp_retransmits: 1,
                tcp_lost: 0,
            },
        };
        let node_b = ServerMetrics {
            wal_append_count: 15,
            kv_put_count: 7,
            kv_get_count: 4,
            system: SystemMetrics {
                cpu_user_us: 90,
                cpu_sys_us: 30,
                rss_kb: 60_000,
                tcp_retransmits: 3,
                tcp_lost: 2,
            },
        };
        let agg = aggregate_server_metrics(&[node_a, node_b]);
        assert_eq!(agg.wal_append_count, 25);
        assert_eq!(agg.kv_put_count, 12);
        assert_eq!(agg.kv_get_count, 7);
        assert_eq!(agg.system.cpu_user_us, 190);
        assert_eq!(agg.system.cpu_sys_us, 50);
        assert_eq!(agg.system.rss_kb, 60_000);
        assert_eq!(agg.system.tcp_retransmits, 3);
        assert_eq!(agg.system.tcp_lost, 2);
    }
}
