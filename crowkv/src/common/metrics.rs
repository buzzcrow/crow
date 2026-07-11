//! Lightweight per-layer counters for the cluster hierarchy.
//!
//! V1: cheap atomic counters intended to be consumed by the topology endpoint
//! and pushed into a time-series DB later. No histograms in code — emitting raw
//! counts/last-rtt is enough for an external scraper to derive p95/p99 over time.
//!
//! ## Why hand-rolled and not the `metrics` crate
//!
//! - `metrics` is great for emit-only integrations (Prometheus / `OTel`) but
//!   each call goes through a global recorder; we want sub-microsecond
//!   `Relaxed` increments on the hot path.
//! - We also need to *read back* the counters from the management API. The
//!   `metrics` crate hides the storage; `AtomicU64` is direct.
//! - When we need rich aggregation (histograms, exemplars), expose the same
//!   `MetricsSnapshot` via a `metrics` recorder; do not change call sites.
//!
//! ## Counters
//!
//! - `rpc_count`: successful RPCs.
//! - `err_count`: failed RPCs (transport / quorum / paxos rejection).
//! - `last_rtt_ms`: most recent successful round-trip latency in ms; 0 if no
//!   successful RPC has been recorded.
//!
//! Future fields (deferred to V2): `bytes_in`, `bytes_out`, `tps_window`.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct LayerMetrics {
    rpc_count: AtomicU64,
    err_count: AtomicU64,
    last_rtt_ms: AtomicU64,
}

impl LayerMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful RPC and its observed latency (rounded to ms).
    pub fn record_ok(&self, rtt_ms: u64) {
        self.rpc_count.fetch_add(1, Ordering::Relaxed);
        self.last_rtt_ms.store(rtt_ms, Ordering::Relaxed);
    }

    /// Record a failed RPC.
    pub fn record_err(&self) {
        self.err_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Take an atomic snapshot for reporting.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            rpc_count: self.rpc_count.load(Ordering::Relaxed),
            err_count: self.err_count.load(Ordering::Relaxed),
            last_rtt_ms: self.last_rtt_ms.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time read of `LayerMetrics`. Pure data; trivially serializable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub rpc_count: u64,
    pub err_count: u64,
    pub last_rtt_ms: u64,
}
