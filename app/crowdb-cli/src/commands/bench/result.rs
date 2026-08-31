// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `BenchResult` — the JSON shape emitted by every `bench` subcommand.
//!
//! The regression scripts parse this with `jq`. Optional sections
//! (`correctness_errors`, `client_transport_stats`, `server_metrics`)
//! are present only on the subcommands that produce them; `jq`'s `// 0`
//! fallback tolerates their absence.

use serde::Serialize;

use super::histogram::BenchHistSnapshot;

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
    pub fn read(mut self, s: BenchHistSnapshot) -> Self {
        self.read = Some(OpStats::from(s));
        self
    }
    /// Attach a write op section.
    pub fn write(mut self, s: BenchHistSnapshot) -> Self {
        self.write = Some(OpStats::from(s));
        self
    }
    /// Attach a list (scan) op section.
    #[allow(dead_code)]
    pub fn list(mut self, s: BenchHistSnapshot) -> Self {
        self.list = Some(OpStats::from(s));
        self
    }
}

#[derive(Debug, Serialize)]
pub struct OpStats {
    pub latency_us: LatencyUs,
}

impl From<BenchHistSnapshot> for OpStats {
    fn from(s: BenchHistSnapshot) -> Self {
        Self {
            latency_us: LatencyUs {
                avg: s.avg,
                p50: s.p50,
                p99: s.p99,
                p999: s.p999,
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
    #[serde(rename = "p999_us")]
    pub p999: u64,
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

/// Result of `bench kv clean` — wipe user data + wait for re-election.
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct CleanResult {
    pub new_leader: String,
    pub wiped_nodes: u64,
}
