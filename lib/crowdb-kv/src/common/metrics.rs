// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

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
//! - `last_rtt_ns`: most recent successful round-trip latency in ns; 0 if no
//!   successful RPC has been recorded.
//!
//! Future fields (deferred to V2): `bytes_in`, `bytes_out`, `tps_window`.

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use crowdb_protocol::mgmt::MetricsSnapshot;

#[derive(Debug, Default)]
pub(crate) struct LayerMetrics {
    rpc_count: AtomicU64,
    err_count: AtomicU64,
    last_rtt_ns: AtomicU64,
}

impl LayerMetrics {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record a successful RPC and its observed latency (nanoseconds).
    pub(crate) fn record_ok(&self, rtt_ns: u64) {
        self.rpc_count.fetch_add(1, Ordering::Relaxed);
        self.last_rtt_ns.store(rtt_ns, Ordering::Relaxed);
    }

    /// Record a failed RPC.
    pub(crate) fn record_err(&self) {
        self.err_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Take an atomic snapshot for reporting.
    #[must_use]
    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            rpc_count: self.rpc_count.load(Ordering::Relaxed),
            err_count: self.err_count.load(Ordering::Relaxed),
            last_rtt_ms: self.last_rtt_ns.load(Ordering::Relaxed) / 1_000_000,
        }
    }
}

/// Per-`PxLocalReplica` leader-election counters.
///
/// Counters are cheap `Relaxed` increments on the election hot path; the
/// election driver and step-down sequence call into the bump helpers,
/// then operators read [`ElectionMetricsSnapshot`] via the management
/// API / health endpoint. Only monotonic counters live here — derived
/// gauges (`current_term`, `last_heartbeat_age_ms`, `lease_remaining_ms`,
/// `bulk_phase1_in_flight_slots`) are computed in
/// `PxLocalReplica::election_metrics_snapshot()` at read time so we
/// never have to keep an `AtomicU64` in sync with the canonical mutex-
/// guarded state.
#[derive(Debug, Default)]
pub(crate) struct ElectionMetrics {
    election_count: AtomicU64,
    step_downs_higher_term: AtomicU64,
    step_downs_lease_unrenewable: AtomicU64,
    step_downs_admin: AtomicU64,
}

impl ElectionMetrics {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// One election attempt started (Candidate transition).
    pub(crate) fn record_election(&self) {
        self.election_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Leader stepped down because it observed `term > current_term`.
    pub(crate) fn record_step_down_higher_term(&self) {
        self.step_downs_higher_term.fetch_add(1, Ordering::Relaxed);
    }

    /// Leader stepped down because the lease became unrenewable.
    pub(crate) fn record_step_down_lease_unrenewable(&self) {
        self.step_downs_lease_unrenewable.fetch_add(1, Ordering::Relaxed);
    }

    /// Leader stepped down because of an admin `StepDown` RPC.
    pub(crate) fn record_step_down_admin(&self) {
        self.step_downs_admin.fetch_add(1, Ordering::Relaxed);
    }

    /// Read counters (monotonic). Derived gauges are filled in by the
    /// `PxLocalReplica::election_metrics_snapshot()` wrapper.
    #[must_use]
    pub(crate) fn counters(&self) -> ElectionMetricsCounters {
        ElectionMetricsCounters {
            election_count: self.election_count.load(Ordering::Relaxed),
            step_downs_higher_term: self.step_downs_higher_term.load(Ordering::Relaxed),
            step_downs_lease_unrenewable: self.step_downs_lease_unrenewable.load(Ordering::Relaxed),
            step_downs_admin: self.step_downs_admin.load(Ordering::Relaxed),
        }
    }
}

/// Monotonic-counter half of [`ElectionMetricsSnapshot`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ElectionMetricsCounters {
    pub(crate) election_count: u64,
    pub(crate) step_downs_higher_term: u64,
    pub(crate) step_downs_lease_unrenewable: u64,
    pub(crate) step_downs_admin: u64,
}

/// Point-in-time election + lease state on one replica. Combines the
/// monotonic counters from [`ElectionMetrics`] with snapshots of the
/// mutex-guarded election / lease state computed at read time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ElectionMetricsSnapshot {
    pub election_count: u64,
    pub current_term: u64,
    /// Milliseconds since the most recent accepted heartbeat (followers)
    /// or the most recent quorum-renewing heartbeat (leaders). `None`
    /// before the first heartbeat has been observed.
    pub last_heartbeat_age_ms: Option<u64>,
    /// Remaining lease window in milliseconds for the leader. `None`
    /// when the lease has expired or this replica is not the leader.
    pub lease_remaining_ms: Option<u64>,
    /// Number of slots currently being repaired by bulk Phase 1.
    pub bulk_phase1_in_flight_slots: u64,
    pub step_downs_higher_term: u64,
    pub step_downs_lease_unrenewable: u64,
    pub step_downs_admin: u64,
}
