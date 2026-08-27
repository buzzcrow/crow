// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! JSON-serializable bench report.
//!
//! Key work: percentile extraction from `crow_common::metrics::PreciseHistogram`,
//! a lossless round-trippable struct, helpers to read/write the report
//! file under `bench-runs/<run-id>.json`.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use crow_common::metrics::{PreciseHistogram, SystemMetrics};
use serde::{Deserialize, Serialize};

use super::workload::{OpKind, WorkloadKind};
use crow_kv_client::ClientMetricsSnapshot;

/// Default for `BenchReport::target` when deserializing historical
/// reports that predate the field. Historical reports were all KV.
#[must_use]
fn default_target_kv() -> String {
    "kv".to_string()
}

/// Latency percentiles (microseconds). Stored as `u64` because the
/// histograms are recorded in microsecond units.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub(crate) struct Percentiles {
    pub(crate) min_us: u64,
    /// Arithmetic mean latency. `#[serde(default)]` so historical reports
    /// written before this field existed still deserialize (defaulting to
    /// 0 on read-back).
    #[serde(default)]
    pub(crate) avg_us: u64,
    pub(crate) p50_us: u64,
    pub(crate) p90_us: u64,
    pub(crate) p99_us: u64,
    pub(crate) p999_us: u64,
    pub(crate) max_us: u64,
}

impl Percentiles {
    #[must_use]
    pub(crate) fn empty() -> Self {
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

/// Extract a fixed set of percentiles from a precise histogram.
#[must_use]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(crate) fn percentiles_from_histogram(h: &PreciseHistogram) -> Percentiles {
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
pub(crate) struct OpReport {
    pub(crate) ops: u64,
    #[serde(default)]
    pub(crate) attempts: u64,
    pub(crate) errors: u64,
    pub(crate) no_leader: u64,
    pub(crate) not_found: u64,
    /// Reads where spot-check bytes didn't match the deterministic
    /// `byte_at(key_id, offset)` formula. Should be 0 in a correct
    /// system. `#[serde(default)]` so historical reports deserialize.
    #[serde(default)]
    pub(crate) correctness_errors: u64,
    pub(crate) latency_us: Percentiles,
}

/// Top-level bench result. Stored as JSON under
/// `bench-runs/<run-id>.json` with a human-readable `.txt` companion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(clippy::struct_field_names)]
pub(crate) struct BenchReport {
    pub(crate) run_id: String,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) finished_at: DateTime<Utc>,
    /// Wall-clock duration of the **measurement** window only —
    /// `total run wall time - warmup_ms`. Older reports without an
    /// explicit warmup field had `warmup_ms == 0` so this stays equal
    /// to the actual run length, preserving back-compat.
    pub(crate) duration_ms: u64,
    /// Wall-clock duration of the warmup window. `0` when warmup was
    /// not configured (i.e. all ops counted toward the report).
    /// Defaults to `0` on read-back of historical JSON files via
    /// serde's default-fill, so existing reports keep deserializing.
    #[serde(default)]
    pub(crate) warmup_ms: u64,
    pub(crate) workload: WorkloadKind,
    /// Target label: `kv`, `rpc`, etc. `#[serde(default)]` so
    /// historical reports without this field deserialize as `kv`.
    #[serde(default = "default_target_kv")]
    pub(crate) target: String,
    /// Storage mode label: `mem`, `file`, or `block`.
    #[serde(default)]
    pub(crate) mode: String,
    pub(crate) connections: u32,
    pub(crate) loader_num: u32,
    pub(crate) key_space: u64,
    pub(crate) value_size: usize,
    pub(crate) target_endpoint: String,
    pub(crate) store_id: u64,
    pub(crate) group_id: u64,
    pub(crate) total_ops: u64,
    #[serde(default)]
    pub(crate) total_attempts: u64,
    pub(crate) total_errors: u64,
    pub(crate) error_rate: f64,
    /// Total spot-check correctness errors across all op kinds (reads
    /// where returned bytes didn't match the deterministic formula).
    /// Should be 0 in a correct system. `#[serde(default)]` so
    /// historical reports deserialize.
    #[serde(default)]
    pub(crate) correctness_errors: u64,
    /// Wall-clock duration of the pre-population phase (0 when not
    /// run). `#[serde(default)]` for back-compat.
    #[serde(default)]
    pub(crate) pre_pop_ms: u64,
    /// Write failures during pre-population. `#[serde(default)]` for
    /// back-compat.
    #[serde(default)]
    pub(crate) pre_pop_errors: u64,
    pub(crate) by_op: BTreeMap<String, OpReport>,
    /// Server-side metrics aggregated across all deployed nodes.
    /// `#[serde(default)]` so historical reports written before this
    /// field existed still deserialize.
    #[serde(default)]
    pub(crate) server_metrics: ServerMetrics,
    /// Client-side metrics from `CrowkvClient`'s internal counters
    /// (per-op counts, leader-related retry events, topology refreshes).
    /// `#[serde(default)]` so historical reports written before this
    /// field existed still deserialize.
    #[serde(default)]
    pub(crate) client_metrics: ClientMetricsSnapshot,
    /// Client-side crow-rpc transport stats (end-of-run cumulative
    /// snapshot from the bench process's `CrowkvClient`).
    #[serde(default)]
    pub(crate) client_transport_stats: TransportStatsSnapshot,
}

impl BenchReport {
    /// Default report location: `bench-runs/` under the current working directory.
    #[must_use]
    pub(crate) fn default_dir() -> PathBuf {
        PathBuf::from("bench-runs")
    }

    /// Write the report as pretty JSON to `<dir>/report.json`.
    /// The directory is created if missing.
    ///
    /// # Errors
    /// I/O or serialization errors.
    pub(crate) fn write_to(&self, dir: &Path) -> io::Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let path = dir.join("report.json");
        let json = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        fs::write(&path, json)?;
        Ok(path)
    }

    /// Write a human-readable Markdown report to `<dir>/report.md`.
    /// The directory is created if missing.
    ///
    /// `node_ids` and `workspace_dir` provide cluster topology context
    /// that is not stored in the JSON struct itself.
    ///
    /// # Errors
    /// I/O errors.
    pub(crate) fn write_md_to(
        &self,
        dir: &Path,
        node_ids: &[u64],
        workspace_dir: &Path,
        endpoint_map: &std::collections::HashMap<String, String>,
    ) -> io::Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let path = dir.join("report.md");
        let md = self.markdown_report(node_ids, workspace_dir, endpoint_map);
        fs::write(&path, md)?;
        Ok(path)
    }

    /// Read a previously-written report from disk.
    ///
    /// # Errors
    /// I/O or JSON-parse errors.
    pub(crate) fn read_from(path: &Path) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }
}

/// Server-side perf counters aggregated across all deployed nodes,
/// extracted from each node's `log/metrics.log` file after a bench run.
/// `#[serde(default)]` on `BenchReport`'s field keeps historical reports
/// without this data deserializable.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct ServerMetrics {
    /// Total WAL records appended (from the `*.wal.append.l` latency
    /// summary's per-window counts, summed across the whole run).
    pub(crate) wal_append_count: u64,
    /// Total KV put ops (from the `*.kv.put.lh` latency histogram's
    /// per-window counts, summed across the whole run).
    pub(crate) kv_put_count: u64,
    /// Total KV get ops (from the `*.kv.get.lh` latency histogram's
    /// per-window counts, summed across the whole run).
    pub(crate) kv_get_count: u64,
    /// Total logical (payload) bytes written to the block device.
    /// From `*.wal.block.logical_bytes.c` counter, summed across the run.
    #[serde(default)]
    pub(crate) wal_logical_bytes: u64,
    /// Total physical bytes written to the block device after alignment
    /// and RMW widening. From `*.wal.block.physical_bytes.c` counter.
    #[serde(default)]
    pub(crate) wal_physical_bytes: u64,
    /// Number of read-modify-write operations triggered by partial-block
    /// writes. From `*.wal.block.rmw.c` counter.
    #[serde(default)]
    pub(crate) wal_rmw_count: u64,
    /// System resource usage (see `crow_kv::metrics::system`).
    pub(crate) system: SystemMetrics,
    /// Server-side crow-rpc transport stats (aggregated across nodes):
    /// syscall counts + frame aggregation, summed across the run.
    #[serde(default)]
    pub(crate) rpc: TransportStatsSnapshot,
    /// Inter-replica consensus RPC latency (leader → followers):
    /// `accept_quorum_rpc` avg (us), per-replica `rpc.l@2`/`@3` avg (us),
    /// follower `engine_apply` avg (us). Last-window snapshot from the
    /// leader's metrics log.
    #[serde(default)]
    pub(crate) replica: ReplicaMetrics,
    /// Total proposals that hit the inflight window slow path (window
    /// was full, had to queue). From `*.write.inflight_enqueued.c`
    /// counter, summed across the run. Zero means the window was never
    /// full — increasing `max_inflight` won't help.
    #[serde(default)]
    pub(crate) inflight_enqueued: u64,
    /// Avg wait time (us) for queued proposals (window-full events).
    /// From `*.write.inflight_wait.l` summary, max avg across windows.
    #[serde(default)]
    pub(crate) inflight_wait_avg_us: u64,
}

/// Inter-replica consensus RPC metrics from the leader's perspective.
/// Captured from the metrics-log flush windows (steady state). Latency
/// values are avg microseconds; tps values are round-trips per second.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct ReplicaMetrics {
    /// Leader → follower 2 RPC avg latency (us).
    #[serde(default)]
    pub(crate) r2: u64,
    /// Leader → follower 2 RPC tps — round-trips per second.
    #[serde(default)]
    pub(crate) r2_tps: u64,
    /// Leader → follower 3 RPC avg latency (us).
    #[serde(default)]
    pub(crate) r3: u64,
    /// Leader → follower 3 RPC tps — round-trips per second.
    #[serde(default)]
    pub(crate) r3_tps: u64,
}

/// crow-rpc transport stats: submit→writev queue-wait latency.
/// Used for both server-side (summed from metrics-log window deltas)
/// and client-side (end-of-run cumulative snapshot) reporting.
/// Legacy fields (`read_calls`, etc.) kept for deserialization of
/// historical reports — always zero in new reports.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TransportStatsSnapshot {
    #[serde(default)]
    pub read_calls: u64,
    #[serde(default)]
    pub writev_calls: u64,
    #[serde(default)]
    pub frames_sent: u64,
    #[serde(default)]
    pub frames_parsed: u64,
    #[serde(default)]
    pub read_bytes: u64,
    #[serde(default)]
    pub writev_bytes: u64,
    /// Cumulative count of submit→writev latency samples.
    #[serde(default)]
    pub submit_to_writev_count: u64,
    /// Cumulative average submit→writev queue wait (microseconds).
    #[serde(default)]
    pub submit_to_writev_avg_us: u64,
    /// Total `enqueue_send` rejections (queue full or connection closed).
    #[serde(default)]
    pub send_queue_rejects: u64,
}

/// Per-op outcome flags recorded into `OpStats`. Grouped as a struct
/// so `OpStats::record` stays under clippy's bool-parameter limit.
/// Construct at call sites with struct literal syntax, e.g.
/// `OpOutcome { ok: true, not_found: true, ..Default::default() }`.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_excessive_bools, reason = "independent per-op flags")]
pub(crate) struct OpOutcome {
    pub(crate) ok: bool,
    pub(crate) no_leader: bool,
    pub(crate) not_found: bool,
    pub(crate) correctness_error: bool,
}

/// Builder for accumulating one op kind worth of stats.
#[derive(Debug)]
pub(crate) struct OpStats {
    pub(crate) ops: u64,
    pub(crate) errors: u64,
    pub(crate) no_leader: u64,
    pub(crate) not_found: u64,
    pub(crate) correctness_errors: u64,
    pub(crate) histogram: PreciseHistogram,
}

impl OpStats {
    /// Build an empty `OpStats` with a `PreciseHistogram` at 3 significant
    /// digits. The histogram's pre-allocated range (`2^32 µs`) covers any
    /// realistic bench latency, so no auto-resize is needed.
    #[must_use]
    pub(crate) fn new() -> Self {
        let mut histogram = PreciseHistogram::new(3);
        // Retained for parity with the previous hdrhistogram-based API;
        // a no-op since the pre-allocated range already covers everything.
        histogram.auto(true);
        Self {
            ops: 0,
            errors: 0,
            no_leader: 0,
            not_found: 0,
            correctness_errors: 0,
            histogram,
        }
    }

    pub(crate) fn record(&mut self, latency_us: u64, outcome: OpOutcome) {
        self.ops += 1;
        if !outcome.ok {
            self.errors += 1;
        }
        if outcome.no_leader {
            self.no_leader += 1;
        }
        if outcome.not_found {
            self.not_found += 1;
        }
        if outcome.correctness_error {
            self.correctness_errors += 1;
        }
        // Floor at 1us; PreciseHistogram clamps the upper bound internally.
        let v = latency_us.max(1);
        self.histogram.record(v);
    }

    /// Merge another `OpStats` into this one (used by the runner to reduce
    /// per-worker stats).
    pub(crate) fn merge(&mut self, other: &Self) {
        self.ops += other.ops;
        self.errors += other.errors;
        self.no_leader += other.no_leader;
        self.not_found += other.not_found;
        self.correctness_errors += other.correctness_errors;
        self.histogram.add(&other.histogram);
    }

    #[must_use]
    pub(crate) fn into_report(self) -> OpReport {
        OpReport {
            ops: self.ops - self.errors,
            attempts: self.ops,
            errors: self.errors,
            no_leader: self.no_leader,
            not_found: self.not_found,
            correctness_errors: self.correctness_errors,
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
pub(crate) fn per_op_map(stats: BTreeMap<OpKind, OpStats>) -> BTreeMap<String, OpReport> {
    stats
        .into_iter()
        .map(|(k, v)| (k.label().to_string(), v.into_report()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Historical `Percentiles` JSON written before `avg_us` existed
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
        let mut h = PreciseHistogram::new(3);
        h.record(10);
        h.record(20);
        h.record(30);
        let p = percentiles_from_histogram(&h);
        assert_eq!(p.avg_us, 20);
        assert_eq!(p.min_us, 10);
        assert_eq!(p.max_us, 30);
    }

    #[test]
    fn percentiles_from_empty_histogram_is_empty() {
        let h = PreciseHistogram::new(3);
        let p = percentiles_from_histogram(&h);
        assert_eq!(p, Percentiles::empty());
    }
}
