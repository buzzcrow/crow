// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! JSON-serializable bench report.
//!
//! Key work: percentile extraction from `hdrhistogram::Histogram<u64>`,
//! a lossless round-trippable struct, helpers to read/write the report
//! file under `bench-runs/<run-id>.json`.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use super::workload::{OpKind, WorkloadKind};
use crowkv_client::ClientMetricsSnapshot;

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
    #[serde(default)]
    pub attempts: u64,
    pub errors: u64,
    pub no_leader: u64,
    pub not_found: u64,
    pub latency_us: Percentiles,
}

/// Top-level bench result. Stored as JSON under
/// `bench-runs/<run-id>.json` with a human-readable `.txt` companion.
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
    /// Storage mode label: `mem`, `file`, or `block`.
    #[serde(default)]
    pub mode: String,
    pub connections: u32,
    pub threads: u32,
    pub key_space: u64,
    pub value_size: usize,
    pub target_endpoint: String,
    pub store_id: u64,
    pub group_id: u64,
    pub total_ops: u64,
    #[serde(default)]
    pub total_attempts: u64,
    pub total_errors: u64,
    pub error_rate: f64,
    pub by_op: BTreeMap<String, OpReport>,
    /// Server-side metrics aggregated across all deployed nodes.
    /// `#[serde(default)]` so historical reports written before this
    /// field existed still deserialize.
    #[serde(default)]
    pub server_metrics: ServerMetrics,
    /// Client-side metrics from `CrowkvClient`'s internal counters
    /// (per-op counts, leader-related retry events, topology refreshes).
    /// `#[serde(default)]` so historical reports written before this
    /// field existed still deserialize.
    #[serde(default)]
    pub client_metrics: ClientMetricsSnapshot,
}

impl BenchReport {
    /// Default report location: `bench-runs/` under the current working directory.
    #[must_use]
    pub fn default_dir() -> PathBuf {
        PathBuf::from("bench-runs")
    }

    /// Write the report as pretty JSON to `<dir>/report.json`.
    /// The directory is created if missing.
    ///
    /// # Errors
    /// I/O or serialization errors.
    pub fn write_to(&self, dir: &Path) -> io::Result<PathBuf> {
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
    pub fn write_md_to(
        &self,
        dir: &Path,
        node_ids: &[String],
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
    pub fn read_from(path: &Path) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }

    /// Generate a comprehensive Markdown report covering test
    /// configuration, cluster topology, per-op results, and server-side
    /// metrics. Uses bullet lists with bold labels for readability.
    #[allow(
        clippy::too_many_lines,
        reason = "display formatter, splitting reduces readability"
    )]
    #[must_use]
    pub fn markdown_report(
        &self,
        node_ids: &[String],
        workspace_dir: &Path,
        endpoint_map: &std::collections::HashMap<String, String>,
    ) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        let _ = writeln!(out, "# CrowKV Benchmark Report");
        let _ = writeln!(out);

        // ── Run Info ──
        let _ = writeln!(out, "## Run Info");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **run_id:** {}", self.run_id);
        let _ = writeln!(
            out,
            "- **started_at:** {}",
            self.started_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(
            out,
            "- **finished_at:** {}",
            self.finished_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        let _ = writeln!(
            out,
            "- **duration:** {} ms (measurement window)",
            self.duration_ms
        );
        if self.warmup_ms > 0 {
            let _ = writeln!(out, "- **warmup:** {} ms (discarded)", self.warmup_ms);
        }
        let _ = writeln!(out);

        // ── Test Configuration ──
        let _ = writeln!(out, "## Test Configuration");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **workload:** {:?}", self.workload);
        let _ = writeln!(out, "- **storage_mode:** {}", self.mode);
        let _ = writeln!(out, "- **connections:** {} (gRPC channels)", self.connections);
        let _ = writeln!(out, "- **threads:** {} (worker tasks, closed-loop)", self.threads);
        let _ = writeln!(out, "- **key_space:** {} keys", self.key_space);
        let _ = writeln!(out, "- **value_size:** {} bytes", self.value_size);
        let _ = writeln!(out, "- **target_endpoint:** {}", self.target_endpoint);
        let _ = writeln!(
            out,
            "- **store_id / group_id:** {} / {}",
            self.store_id, self.group_id
        );
        let _ = writeln!(out);

        // ── Cluster Topology ──
        let _ = writeln!(out, "## Cluster Topology");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **nodes:** {} (3-replica Paxos group)", node_ids.len());
        for (i, nid) in node_ids.iter().enumerate() {
            let _ = writeln!(out, "  - node[{i}]: {nid}");
        }
        let _ = writeln!(out, "- **workspace:** {}", workspace_dir.display());
        let (wal_desc, kv_desc) = match self.mode.as_str() {
            "mem" => (
                "mem-block (in-memory, no disk I/O)",
                "crowtree + mem-block page store (in-memory, no disk I/O)",
            ),
            "file" => (
                "file (file-backed, no O_DIRECT)",
                "crowtree + file page store (file-backed, no O_DIRECT)",
            ),
            "block-device" => (
                "block-device (O_DIRECT, 4K aligned)",
                "crowtree + block page store (O_DIRECT, 4K aligned)",
            ),
            other => (other, other),
        };
        let _ = writeln!(out, "- **WAL backend:** {wal_desc}");
        let _ = writeln!(out, "- **KV engine backend:** {kv_desc}");
        let _ = writeln!(out);

        // ── Client-side Results ──
        let _ = writeln!(out, "## Client-side Results");
        let _ = writeln!(out);
        #[allow(
            clippy::cast_precision_loss,
            reason = "display-only QPS, precision loss irrelevant"
        )]
        let secs = self.duration_ms as f64 / 1000.0;
        #[allow(
            clippy::cast_precision_loss,
            reason = "display-only QPS, precision loss irrelevant"
        )]
        let qps = if secs > 0.0 {
            self.total_ops as f64 / secs
        } else {
            0.0
        };
        let _ = writeln!(out, "- **total_ops (success):** {}", self.total_ops);
        let _ = writeln!(out, "- **total_attempts:** {}", self.total_attempts);
        let _ = writeln!(out, "- **total_errors:** {}", self.total_errors);
        let _ = writeln!(out, "- **error_rate:** {:.4}", self.error_rate);
        let _ = writeln!(out, "- **throughput:** {qps:.1} ops/s (success only)");
        let _ = writeln!(out);

        // ── Per-Op Breakdown ──
        if !self.by_op.is_empty() {
            let _ = writeln!(out, "## Per-Op Breakdown");
            let _ = writeln!(out);
            for (kind, op) in &self.by_op {
                let p = &op.latency_us;
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "display-only QPS, precision loss irrelevant"
                )]
                let op_qps = if secs > 0.0 { op.ops as f64 / secs } else { 0.0 };
                let _ = writeln!(out, "### {kind}");
                let _ = writeln!(out);
                let _ = writeln!(out, "- **ops (success):** {} ({op_qps:.1} ops/s)", op.ops);
                let _ = writeln!(out, "- **attempts:** {}", op.attempts);
                let _ = writeln!(out, "- **errors:** {}", op.errors);
                if op.no_leader > 0 {
                    let _ = writeln!(out, "- **no_leader:** {}", op.no_leader);
                }
                if op.not_found > 0 {
                    let _ = writeln!(out, "- **not_found:** {}", op.not_found);
                }
                let _ = writeln!(
                    out,
                    "- **latency (us):** min={} avg={} p50={} p90={} p99={} p999={} max={}",
                    p.min_us, p.avg_us, p.p50_us, p.p90_us, p.p99_us, p.p999_us, p.max_us
                );
                let _ = writeln!(out);
            }
        }

        // ── Client-side Metrics ──
        let cm = &self.client_metrics;
        let _ = writeln!(out, "## Client-side Metrics");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **put_errors:** {}", cm.put_errors);
        let _ = writeln!(out, "- **get_errors:** {}", cm.get_errors);
        let _ = writeln!(out, "- **delete_errors:** {}", cm.delete_errors);
        let _ = writeln!(out, "- **scan_errors:** {}", cm.scan_errors);
        let _ = writeln!(out, "- **batch_write_errors:** {}", cm.batch_write_errors);
        let _ = writeln!(
            out,
            "- **not_leader_hint_followed:** {}",
            cm.not_leader_hint_followed
        );
        let _ = writeln!(out, "- **leader_query:** {}", cm.leader_query);
        let _ = writeln!(out, "- **unknown_leader_wait:** {}", cm.unknown_leader_wait);
        let _ = writeln!(out, "- **transport_error_retry:** {}", cm.transport_error_retry);
        let _ = writeln!(out, "- **retries_exhausted:** {}", cm.retries_exhausted);
        let _ = writeln!(out, "- **no_leader:** {}", cm.no_leader);
        let _ = writeln!(out, "- **topology_refresh:** {}", cm.topology_refresh);
        let _ = writeln!(out);

        // ── Leader Change Analysis ──
        let changes = &cm.leader_changes;
        if changes.is_empty() {
            let _ = writeln!(out, "## Leader Change Analysis");
            let _ = writeln!(out);
            let _ = writeln!(out, "- no leader changes detected");
            let _ = writeln!(out);
        } else {
            let total_recovery_ms: u64 = changes.iter().map(|c| c.recovery_ms).sum();
            let max_recovery_ms = changes.iter().map(|c| c.recovery_ms).max().unwrap_or(0);
            let min_recovery_ms = changes.iter().map(|c| c.recovery_ms).min().unwrap_or(0);
            let avg_recovery_ms = total_recovery_ms / changes.len() as u64;
            let _ = writeln!(out, "## Leader Change Analysis");
            let _ = writeln!(out);
            let _ = writeln!(out, "- **count:** {}", changes.len());
            let _ = writeln!(out, "- **total_recovery:** {total_recovery_ms} ms");
            let _ = writeln!(out, "- **avg_recovery:** {avg_recovery_ms} ms");
            let _ = writeln!(out, "- **min_recovery:** {min_recovery_ms} ms");
            let _ = writeln!(out, "- **max_recovery:** {max_recovery_ms} ms");
            let _ = writeln!(out);
            let _ = writeln!(out, "### Episodes");
            let _ = writeln!(out);
            let resolve = |ep: &str| -> String {
                let normalized = ep.strip_prefix("http://").unwrap_or(ep);
                endpoint_map
                    .get(ep)
                    .or_else(|| endpoint_map.get(normalized))
                    .cloned()
                    .unwrap_or_else(|| ep.to_string())
            };
            for (i, c) in changes.iter().enumerate() {
                let detected_at = chrono::DateTime::from_timestamp_millis(
                    i64::try_from(c.detected_at_ms).unwrap_or(i64::MAX),
                )
                .map_or_else(
                    || format!("{}ms", c.detected_at_ms),
                    |dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                );
                let _ = writeln!(
                    out,
                    "- **[{}]** `{} -> {}` trigger={} recovery={}ms detected_at={}",
                    i,
                    resolve(&c.old_endpoint),
                    resolve(&c.new_endpoint),
                    c.trigger,
                    c.recovery_ms,
                    detected_at,
                );
            }
            let _ = writeln!(out);
        }

        // ── Server-side Metrics ──
        let sm = &self.server_metrics;
        let _ = writeln!(out, "## Server-side Metrics (aggregated across nodes)");
        let _ = writeln!(out);
        let _ = writeln!(out, "- **wal_append_count:** {}", sm.wal_append_count);
        let _ = writeln!(out, "- **kv_put_count:** {}", sm.kv_put_count);
        let _ = writeln!(out, "- **kv_get_count:** {}", sm.kv_get_count);
        let _ = writeln!(out);
        let _ = writeln!(out, "### System Resources");
        let _ = writeln!(out);
        let sys = &sm.system;
        let _ = writeln!(out, "- **cpu_user:** {} us", sys.cpu_user_us);
        let _ = writeln!(out, "- **cpu_sys:** {} us", sys.cpu_sys_us);
        let _ = writeln!(out, "- **peak_rss:** {} KB", sys.rss_kb);
        let _ = writeln!(out, "- **tcp_retransmits:** {}", sys.tcp_retransmits);
        let _ = writeln!(out, "- **tcp_lost:** {}", sys.tcp_lost);
        let _ = writeln!(out);

        // ── Anomalies ──
        let mut anomalies: Vec<String> = Vec::new();
        if self.error_rate > 0.0 {
            anomalies.push(format!("non-zero client error rate: {:.4}", self.error_rate));
        }
        if sys.tcp_retransmits > 0 {
            anomalies.push(format!("TCP retransmits: {}", sys.tcp_retransmits));
        }
        if sys.tcp_lost > 0 {
            anomalies.push(format!("TCP lost segments: {}", sys.tcp_lost));
        }
        if !cm.leader_changes.is_empty() {
            let total: u64 = cm.leader_changes.iter().map(|c| c.recovery_ms).sum();
            anomalies.push(format!(
                "leader changes: {} (total recovery {}ms)",
                cm.leader_changes.len(),
                total,
            ));
        }
        let _ = writeln!(out, "## Anomalies");
        let _ = writeln!(out);
        if anomalies.is_empty() {
            let _ = writeln!(out, "- none");
        } else {
            for a in &anomalies {
                let _ = writeln!(out, "- {a}");
            }
        }
        let _ = writeln!(out);
        let _ = writeln!(out, "---");
        out
    }

    /// Pretty multi-line summary suitable for `bench compare` output.
    #[must_use]
    pub fn human_summary(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "run_id          : {}\nmode            : {}\nworkload        : {:?}\nduration        : {} ms (measurement)\nwarmup          : {} ms (discarded)\nconnections     : {}\nthreads         : {}\nkey_space       : {}\nvalue_size      : {} B\ntarget          : {} (store={} group={})\ntotal_ops       : {}\ntotal_errors    : {}\nerror_rate      : {:.4}",
            self.run_id,
            self.mode,
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
                "{kind:>8}: ops={ops} err={err} nl={nl} nf={nf}  avg={avg}us p50={p50}us p99={p99}us p999={p999}us max={max}us",
                kind = kind,
                ops = op.ops,
                err = op.errors,
                nl = op.no_leader,
                nf = op.not_found,
                avg = p.avg_us,
                p50 = p.p50_us,
                p99 = p.p99_us,
                p999 = p.p999_us,
                max = p.max_us,
            );
        }
        let sm = &self.server_metrics;
        let cm = &self.client_metrics;
        let _ = writeln!(
            out,
            "server_metrics  : wal_append={wal_append} kv_put={kv_put} kv_get={kv_get}\nsystem          : cpu_user={cpu_user}us cpu_sys={cpu_sys}us rss={rss}KB tcp_retrans={tcp_retrans} tcp_lost={tcp_lost}\nclient_metrics  : nl_hint={nl_hint} leader_query={leader_query} xport_err={xport_err} retries_exhausted={retries_exhausted} no_leader={no_leader} topo_refresh={topo_refresh}",
            wal_append = sm.wal_append_count,
            kv_put = sm.kv_put_count,
            kv_get = sm.kv_get_count,
            cpu_user = sm.system.cpu_user_us,
            cpu_sys = sm.system.cpu_sys_us,
            rss = sm.system.rss_kb,
            tcp_retrans = sm.system.tcp_retransmits,
            tcp_lost = sm.system.tcp_lost,
            nl_hint = cm.not_leader_hint_followed,
            leader_query = cm.leader_query,
            xport_err = cm.transport_error_retry,
            retries_exhausted = cm.retries_exhausted,
            no_leader = cm.no_leader,
            topo_refresh = cm.topology_refresh,
        );
        let lc = &cm.leader_changes;
        if lc.is_empty() {
            let _ = writeln!(out, "leader_changes   : none");
        } else {
            let total: u64 = lc.iter().map(|c| c.recovery_ms).sum();
            let max: u64 = lc.iter().map(|c| c.recovery_ms).max().unwrap_or(0);
            let _ = writeln!(
                out,
                "leader_changes   : count={} total_recovery={}ms max_recovery={}ms",
                lc.len(),
                total,
                max,
            );
        }
        out
    }
}

/// Server-side perf counters aggregated across all deployed nodes,
/// extracted from each node's `log/metrics.log` file after a bench run.
/// `#[serde(default)]` on `BenchReport`'s field keeps historical reports
/// without this data deserializable.
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
                if name.contains(".wal.") && name.contains(".append.") {
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
    pub no_leader: u64,
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
            no_leader: 0,
            not_found: 0,
            histogram,
        }
    }

    pub fn record(&mut self, latency_us: u64, ok: bool, no_leader: bool, not_found: bool) {
        self.ops += 1;
        if !ok {
            self.errors += 1;
        }
        if no_leader {
            self.no_leader += 1;
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
        self.no_leader += other.no_leader;
        self.not_found += other.not_found;
        self.histogram.add(&other.histogram).expect("histogram add");
    }

    #[must_use]
    pub fn into_report(self) -> OpReport {
        OpReport {
            ops: self.ops - self.errors,
            attempts: self.ops,
            errors: self.errors,
            no_leader: self.no_leader,
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

    /// `parse_metrics_log` sums histogram/summary window-delta counts
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
s.1.g.1.wal.file.append.l   10        10       12       20
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
s.1.g.1.wal.file.append.l   20        20       13       22
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
