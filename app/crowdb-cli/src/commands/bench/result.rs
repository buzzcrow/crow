// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `BenchResult` — the JSON shape emitted by every `bench` subcommand.
//!
//! The regression scripts parse this with `jq` from stdout. A
//! human-readable plain-text version is logged via `tracing::info!`
//! to the CLI's tracing log file. Optional sections
//! (`correctness_errors`, `client_transport_stats`, `server_metrics`)
//! are present only on the subcommands that produce them; `jq`'s `// 0`
//! fallback tolerates their absence.

use std::fmt;

use serde::Serialize;

use crowdb_common::metrics::HistogramSnapshot;

/// Top-level bench result. `by_op` carries one entry per op kind
/// actually exercised (`read` / `write` / `list`).
#[derive(Debug, Serialize)]
pub struct BenchResult {
    pub total_ops: u64,
    pub duration_ms: u64,
    pub total_errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correctness_errors: Option<u64>,
    pub by_op: BenchOps,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_transport_stats: Option<TransportStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_metrics: Option<ServerMetrics>,
}

#[derive(Debug, Default, Serialize)]
pub struct BenchOps {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read: Option<OpStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write: Option<OpStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list: Option<OpStats>,
}

impl BenchOps {
    /// Attach a read op section.
    #[allow(dead_code)]
    pub fn read(mut self, s: HistogramSnapshot) -> Self {
        self.read = Some(OpStats::from(s));
        self
    }
    /// Attach a write op section.
    pub fn write(mut self, s: HistogramSnapshot) -> Self {
        self.write = Some(OpStats::from(s));
        self
    }
    /// Attach a list (scan) op section.
    #[allow(dead_code)]
    pub fn list(mut self, s: HistogramSnapshot) -> Self {
        self.list = Some(OpStats::from(s));
        self
    }
}

#[derive(Debug, Serialize)]
pub struct OpStats {
    pub latency_us: LatencyUs,
}

impl From<HistogramSnapshot> for OpStats {
    fn from(s: HistogramSnapshot) -> Self {
        // LatencyHistogram records in ns; convert to µs for the JSON report.
        Self {
            latency_us: LatencyUs {
                avg: s.avg / 1000,
                p50: s.p50 / 1000,
                p99: s.p99 / 1000,
            },
        }
    }
}

/// Per-op latency percentiles. Field names are serialized as
/// `*_us` (the contract the regression scripts parse); the Rust names
/// drop the shared `_us` postfix to satisfy `clippy::struct_field_names`.
#[derive(Debug, Serialize)]
pub struct LatencyUs {
    #[serde(rename = "avg_us")]
    pub avg: u64,
    #[serde(rename = "p50_us")]
    pub p50: u64,
    #[serde(rename = "p99_us")]
    pub p99: u64,
}

/// crowdb-rpc transport stats (client-side or server-side). Fields not
/// exposed by the FFI for a given side are reported as 0.
#[derive(Debug, Default, Serialize)]
pub struct TransportStats {
    pub writev_calls: u64,
    pub frames_sent: u64,
    pub read_calls: u64,
    pub frames_parsed: u64,
    pub submit_to_writev_avg_us: u64,
}

/// Server-side metrics fetched from the KV server management API after
/// a write run. Aggregated across the cluster where noted.
#[derive(Debug, Default, Serialize)]
pub struct ServerMetrics {
    /// WAL append count summed across nodes.
    pub wal_append_count: u64,
    pub rpc: TransportStats,
    pub replica: ReplicaStats,
    pub inflight_enqueued: u64,
    pub inflight_wait_avg_us: u64,
    /// Server-side per-op RPC latency (averaged across nodes).
    pub rpc_latency: Option<ServerRpcLatency>,
}

/// Server-side per-request-type RPC latency, averaged across nodes.
#[derive(Debug, Default, Serialize)]
pub struct ServerRpcLatency {
    pub put_avg_us: u64,
    pub put_p50_us: u64,
    pub put_p99_us: u64,
    pub get_avg_us: u64,
    pub get_p50_us: u64,
    pub get_p99_us: u64,
    pub scan_avg_us: u64,
    pub scan_p50_us: u64,
    pub scan_p99_us: u64,
    pub delete_avg_us: u64,
    pub delete_p50_us: u64,
    pub delete_p99_us: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct ReplicaStats {
    /// Round-trip latency (µs) to replica 2.
    pub r2: u64,
    /// Round-trips/s to replica 2.
    pub r2_tps: u64,
    /// Round-trip latency (µs) to replica 3.
    pub r3: u64,
    /// Round-trips/s to replica 3.
    pub r3_tps: u64,
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ops_s = (self.total_ops * 1000).checked_div(self.duration_ms).unwrap_or(0);
        writeln!(f, "=== bench report ===")?;
        writeln!(
            f,
            "  ops: {}  duration: {}ms  ops/s: {}  errors: {}",
            self.total_ops, self.duration_ms, ops_s, self.total_errors,
        )?;
        if let Some(ce) = self.correctness_errors {
            writeln!(f, "  correctness_errors: {ce}")?;
        }
        if let Some(s) = &self.by_op.read {
            writeln!(
                f,
                "  read:  avg={}us  p50={}us  p99={}us",
                s.latency_us.avg, s.latency_us.p50, s.latency_us.p99,
            )?;
        }
        if let Some(s) = &self.by_op.write {
            writeln!(
                f,
                "  write: avg={}us  p50={}us  p99={}us",
                s.latency_us.avg, s.latency_us.p50, s.latency_us.p99,
            )?;
        }
        if let Some(s) = &self.by_op.list {
            writeln!(
                f,
                "  list:  avg={}us  p50={}us  p99={}us",
                s.latency_us.avg, s.latency_us.p50, s.latency_us.p99,
            )?;
        }
        if let Some(cts) = &self.client_transport_stats {
            writeln!(
                f,
                "  client_transport: writev={} frames_sent={} read={} frames_parsed={} s2w_avg={}us",
                cts.writev_calls,
                cts.frames_sent,
                cts.read_calls,
                cts.frames_parsed,
                cts.submit_to_writev_avg_us,
            )?;
        }
        if let Some(sm) = &self.server_metrics {
            let wal_per_node = sm.wal_append_count / 3;
            writeln!(
                f,
                "  server: wal_append={} ({} /node)  inflight_enq={}  inflight_wait_avg={}us",
                sm.wal_append_count, wal_per_node, sm.inflight_enqueued, sm.inflight_wait_avg_us,
            )?;
            writeln!(
                f,
                "  server_rpc: writev={} frames_sent={} read={} frames_parsed={} s2w_avg={}us",
                sm.rpc.writev_calls,
                sm.rpc.frames_sent,
                sm.rpc.read_calls,
                sm.rpc.frames_parsed,
                sm.rpc.submit_to_writev_avg_us,
            )?;
            writeln!(
                f,
                "  replica: r2={}us/{}tps  r3={}us/{}tps",
                sm.replica.r2, sm.replica.r2_tps, sm.replica.r3, sm.replica.r3_tps,
            )?;
            if let Some(rl) = &sm.rpc_latency {
                writeln!(f, "  server_rpc_latency (avg/p50/p99 us):",)?;
                writeln!(
                    f,
                    "    put:    {}/{}/{}",
                    rl.put_avg_us, rl.put_p50_us, rl.put_p99_us,
                )?;
                writeln!(
                    f,
                    "    get:    {}/{}/{}",
                    rl.get_avg_us, rl.get_p50_us, rl.get_p99_us,
                )?;
                writeln!(
                    f,
                    "    scan:   {}/{}/{}",
                    rl.scan_avg_us, rl.scan_p50_us, rl.scan_p99_us,
                )?;
                writeln!(
                    f,
                    "    delete: {}/{}/{}",
                    rl.delete_avg_us, rl.delete_p50_us, rl.delete_p99_us,
                )?;
            }
        }
        write!(f, "=== end report ===")
    }
}
