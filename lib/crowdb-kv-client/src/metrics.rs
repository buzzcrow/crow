// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lightweight atomic counters and latency tracking for client-side
//! observability. Hot-path counters are `AtomicU64` with `Relaxed`
//! ordering — no locks, no allocation. Leader-change tracking uses a
//! `Mutex` because leader changes are rare events (not hot path).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use crowdb_common::metrics::PreciseHistogram;

/// One recorded leader-change episode: from when the client first
/// detected the old leader was wrong to when it confirmed a new leader.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct LeaderChangeEpisode {
    /// Unix epoch milliseconds when the leader change was first detected.
    pub detected_at_ms: u64,
    /// Recovery duration in milliseconds (detection → new leader confirmed).
    pub recovery_ms: u64,
    /// Previous leader endpoint.
    pub old_endpoint: String,
    /// New leader endpoint.
    pub new_endpoint: String,
    /// What triggered detection: `not_leader_hint`, `unknown_leader`,
    /// or `transport_error`.
    pub trigger: String,
}

/// Internal tracker for in-progress leader-change episodes. Protected
/// by a `Mutex` — leader changes are rare, so contention is negligible.
#[derive(Debug, Default)]
struct LeaderChangeTracker {
    /// Completed episodes, ready for the next snapshot.
    episodes: Vec<LeaderChangeEpisode>,
    /// `(store_id, group_id)` → `(start_instant, old_endpoint)` for
    /// an episode in progress. First worker to hit a leader error
    /// opens the episode; the worker that confirms the new leader
    /// closes it.
    pending: HashMap<(u64, u64), (Instant, String)>,
}

impl LeaderChangeTracker {
    fn on_leader_error(&mut self, store_id: u64, group_id: u64, current_endpoint: &str) {
        self.pending
            .entry((store_id, group_id))
            .or_insert_with(|| (Instant::now(), current_endpoint.to_string()));
    }

    fn on_leader_resolved(
        &mut self,
        store_id: u64,
        group_id: u64,
        new_endpoint: &str,
        trigger: &str,
        now_ms: u64,
    ) {
        if let Some((start, old_endpoint)) = self.pending.remove(&(store_id, group_id)) {
            if new_endpoint != old_endpoint {
                self.episodes.push(LeaderChangeEpisode {
                    detected_at_ms: now_ms
                        .saturating_sub(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)),
                    recovery_ms: u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                    old_endpoint,
                    new_endpoint: new_endpoint.to_string(),
                    trigger: trigger.to_string(),
                });
            }
        }
    }

    fn snapshot(&mut self) -> Vec<LeaderChangeEpisode> {
        std::mem::take(&mut self.episodes)
    }
}

/// Snapshot of [`ClientMetrics`] at a point in time. All values are
/// cumulative totals since client creation.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientMetricsSnapshot {
    pub put_errors: u64,
    pub get_errors: u64,
    pub delete_errors: u64,
    pub scan_errors: u64,
    pub batch_write_errors: u64,
    pub not_leader_hint_followed: u64,
    pub leader_query: u64,
    pub unknown_leader_wait: u64,
    pub transport_error_retry: u64,
    pub retries_exhausted: u64,
    pub no_leader: u64,
    pub topology_refresh: u64,
    /// `MinSlot` reads whose first endpoint was picked by a distributed
    /// selector (`AnyReplica`, `LeastConnections`, or `Latency`) — a
    /// non-leader replica, or the leader as one of the pool. Lets an
    /// operator confirm distribution is actually happening. `Leader`
    /// policy never increments this.
    #[serde(default)]
    pub read_endpoint_distributed: u64,
    /// `MinSlot` reads that were distributed to a follower but fell
    /// back to the leader because the follower had not applied
    /// `min_slot` (server returned `NotLeader`). Pairs with the
    /// server-side `read.minslot_fallback.c` to confirm the fallback
    /// rate stays low. Fires for every distributed policy, not just
    /// `AnyReplica`.
    #[serde(default)]
    pub read_endpoint_fallback: u64,
    /// Recorded leader-change episodes during the client's lifetime.
    #[serde(default)]
    pub leader_changes: Vec<LeaderChangeEpisode>,
}

/// Per-op-kind window latency histograms. Drained by `drain_window`
/// for periodic flushing; the caller is responsible for accumulating
/// drained snapshots into cumulative histograms if desired.
#[derive(Debug)]
struct WindowLatency {
    put: PreciseHistogram,
    get: PreciseHistogram,
    delete: PreciseHistogram,
    scan: PreciseHistogram,
    batch_write: PreciseHistogram,
}

impl Default for WindowLatency {
    fn default() -> Self {
        let mk = || PreciseHistogram::new(3);
        Self {
            put: mk(),
            get: mk(),
            delete: mk(),
            scan: mk(),
            batch_write: mk(),
        }
    }
}

/// Metrics embedded in [`crate::CrowdbKvClient`]. Hot-path error counters
/// are lock-free atomics; per-op latency histograms are `Mutex<Histogram>`;
/// leader-change tracking uses a `Mutex` (rare event, not hot path).
#[derive(Debug, Default)]
pub struct ClientMetrics {
    put_errors: AtomicU64,
    get_errors: AtomicU64,
    delete_errors: AtomicU64,
    scan_errors: AtomicU64,
    batch_write_errors: AtomicU64,
    not_leader_hint_followed: AtomicU64,
    leader_query: AtomicU64,
    unknown_leader_wait: AtomicU64,
    transport_error_retry: AtomicU64,
    retries_exhausted: AtomicU64,
    no_leader: AtomicU64,
    topology_refresh: AtomicU64,
    read_endpoint_distributed: AtomicU64,
    read_endpoint_fallback: AtomicU64,
    leader_changes: Mutex<LeaderChangeTracker>,
    window_lat: Mutex<WindowLatency>,
}

impl ClientMetrics {
    pub(crate) fn record_put_latency(&self, lat_us: u64) {
        if let Ok(mut g) = self.window_lat.lock() {
            g.put.record(lat_us.max(1));
        }
    }

    pub(crate) fn record_get_latency(&self, lat_us: u64) {
        if let Ok(mut g) = self.window_lat.lock() {
            g.get.record(lat_us.max(1));
        }
    }

    pub(crate) fn record_delete_latency(&self, lat_us: u64) {
        if let Ok(mut g) = self.window_lat.lock() {
            g.delete.record(lat_us.max(1));
        }
    }

    pub(crate) fn record_scan_latency(&self, lat_us: u64) {
        if let Ok(mut g) = self.window_lat.lock() {
            g.scan.record(lat_us.max(1));
        }
    }

    pub(crate) fn record_batch_write_latency(&self, lat_us: u64) {
        if let Ok(mut g) = self.window_lat.lock() {
            g.batch_write.record(lat_us.max(1));
        }
    }

    pub(crate) fn record_put_error(&self) {
        self.put_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_get_error(&self) {
        self.get_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_delete_error(&self) {
        self.delete_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_scan_error(&self) {
        self.scan_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_batch_write_error(&self) {
        self.batch_write_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_not_leader_hint(&self) {
        self.not_leader_hint_followed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_leader_query(&self) {
        self.leader_query.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_unknown_leader_wait(&self) {
        self.unknown_leader_wait.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_transport_error(&self) {
        self.transport_error_retry.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retries_exhausted(&self) {
        self.retries_exhausted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_no_leader(&self) {
        self.no_leader.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_topology_refresh(&self) {
        self.topology_refresh.fetch_add(1, Ordering::Relaxed);
    }

    /// A `MinSlot` read was routed by a distributed selector
    /// (`AnyReplica`, `LeastConnections`, or `Latency`) to a replica
    /// chosen from the topology cache's replica list.
    pub(crate) fn record_read_endpoint_distributed(&self) {
        self.read_endpoint_distributed.fetch_add(1, Ordering::Relaxed);
    }

    /// A distributed `MinSlot` read fell back to the leader after the
    /// chosen replica returned `NotLeader` (had not applied `min_slot`).
    pub(crate) fn record_read_endpoint_fallback(&self) {
        self.read_endpoint_fallback.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the start of a leader-change episode. Called when any
    /// worker encounters a leader-related error (`NotLeaderHint`,
    /// unknown leader, transport error to the current leader).
    /// First caller wins; subsequent callers for the same group
    /// are ignored until the episode is resolved.
    pub(crate) fn on_leader_error(&self, store_id: u64, group_id: u64, current_endpoint: &str) {
        if let Ok(mut tracker) = self.leader_changes.lock() {
            tracker.on_leader_error(store_id, group_id, current_endpoint);
        }
    }

    /// Record the resolution of a leader-change episode. Called when
    /// the client confirms a new leader endpoint. If the new endpoint
    /// differs from the old one, a `LeaderChangeEpisode` is recorded.
    pub(crate) fn on_leader_resolved(&self, store_id: u64, group_id: u64, new_endpoint: &str, trigger: &str) {
        if let Ok(mut tracker) = self.leader_changes.lock() {
            tracker.on_leader_resolved(store_id, group_id, new_endpoint, trigger, now_epoch_ms());
        }
    }

    /// Read all counters as a snapshot. Values are cumulative totals.
    #[must_use]
    pub fn snapshot(&self) -> ClientMetricsSnapshot {
        let leader_changes = self
            .leader_changes
            .lock()
            .map(|mut t| t.snapshot())
            .unwrap_or_default();
        ClientMetricsSnapshot {
            put_errors: self.put_errors.load(Ordering::Relaxed),
            get_errors: self.get_errors.load(Ordering::Relaxed),
            delete_errors: self.delete_errors.load(Ordering::Relaxed),
            scan_errors: self.scan_errors.load(Ordering::Relaxed),
            batch_write_errors: self.batch_write_errors.load(Ordering::Relaxed),
            not_leader_hint_followed: self.not_leader_hint_followed.load(Ordering::Relaxed),
            leader_query: self.leader_query.load(Ordering::Relaxed),
            unknown_leader_wait: self.unknown_leader_wait.load(Ordering::Relaxed),
            transport_error_retry: self.transport_error_retry.load(Ordering::Relaxed),
            retries_exhausted: self.retries_exhausted.load(Ordering::Relaxed),
            no_leader: self.no_leader.load(Ordering::Relaxed),
            topology_refresh: self.topology_refresh.load(Ordering::Relaxed),
            read_endpoint_distributed: self.read_endpoint_distributed.load(Ordering::Relaxed),
            read_endpoint_fallback: self.read_endpoint_fallback.load(Ordering::Relaxed),
            leader_changes,
        }
    }

    /// Drain per-op-kind window latency histograms, returning one
    /// `PreciseHistogram` per op kind. The caller is expected to accumulate
    /// these into cumulative histograms for run-wide percentiles.
    #[must_use]
    pub fn drain_window(&self) -> WindowLatencySnapshot {
        self.window_lat.lock().map_or_else(
            |_| WindowLatencySnapshot::default(),
            |mut g| {
                let mk = || PreciseHistogram::new(3);
                WindowLatencySnapshot {
                    put: std::mem::replace(&mut g.put, mk()),
                    get: std::mem::replace(&mut g.get, mk()),
                    delete: std::mem::replace(&mut g.delete, mk()),
                    scan: std::mem::replace(&mut g.scan, mk()),
                    batch_write: std::mem::replace(&mut g.batch_write, mk()),
                }
            },
        )
    }

    /// Flush per-op-kind window latency histograms to `writer` in the
    /// same column-aligned format as the server `[rust-metrics]` log.
    /// Takes a pre-drained `WindowLatencySnapshot` so the caller can
    /// also use it for cumulative accumulation.
    #[allow(
        clippy::uninlined_format_args,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn flush_latencies<W: std::fmt::Write>(
        &self,
        writer: &mut W,
        snap: &WindowLatencySnapshot,
        window_secs: f64,
    ) {
        let width = 24usize;
        let count_w = 7usize;
        let tps_w = 7usize;

        let entries: [(&str, &PreciseHistogram); 5] = [
            ("client.put.lh", &snap.put),
            ("client.get.lh", &snap.get),
            ("client.delete.lh", &snap.delete),
            ("client.scan.lh", &snap.scan),
            ("client.batch_write.lh", &snap.batch_write),
        ];
        let active: Vec<(&str, &PreciseHistogram)> =
            entries.iter().filter(|(_, h)| !h.is_empty()).copied().collect();
        if active.is_empty() {
            return;
        }
        let _ = writeln!(
            writer,
            "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}",
            "",
            "count",
            "tps(/s)",
            "avg(us)",
            "p50(us)",
            "p99(us)",
            "max(us)",
            width = width,
            count_w = count_w,
            tps_w = tps_w,
        );
        for (name, h) in &active {
            let name_w = name.len().max(width);
            let count = h.len();
            let tps = tps_calc(count, window_secs);
            let avg = h.mean() as u64;
            let p50 = h.value_at_quantile(0.50);
            let p99 = h.value_at_quantile(0.99);
            let max = h.max();
            let _ = writeln!(
                writer,
                "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}",
                name,
                count,
                tps,
                avg,
                p50,
                p99,
                max,
                name_w = name_w,
                count_w = count_w,
                tps_w = tps_w,
            );
        }
    }
}

/// Snapshot of drained window latency histograms, one per op kind.
#[derive(Debug)]
pub struct WindowLatencySnapshot {
    pub put: PreciseHistogram,
    pub get: PreciseHistogram,
    pub delete: PreciseHistogram,
    pub scan: PreciseHistogram,
    pub batch_write: PreciseHistogram,
}

impl Default for WindowLatencySnapshot {
    fn default() -> Self {
        let mk = || PreciseHistogram::new(3);
        Self {
            put: mk(),
            get: mk(),
            delete: mk(),
            scan: mk(),
            batch_write: mk(),
        }
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn tps_calc(count: u64, window_secs: f64) -> u64 {
    (count as f64 / window_secs) as u64
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}
