// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Lightweight atomic counters and latency tracking for client-side
//! observability. Hot-path counters are `AtomicU64` with `Relaxed`
//! ordering — no locks, no allocation. Leader-change tracking uses a
//! `Mutex` because leader changes are rare events (not hot path).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

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
    pub put_ops: u64,
    pub put_errors: u64,
    pub get_ops: u64,
    pub get_errors: u64,
    pub delete_ops: u64,
    pub delete_errors: u64,
    pub scan_ops: u64,
    pub scan_errors: u64,
    pub batch_write_ops: u64,
    pub batch_write_errors: u64,
    pub not_leader_hint_followed: u64,
    pub leader_query: u64,
    pub unknown_leader_wait: u64,
    pub transport_error_retry: u64,
    pub retries_exhausted: u64,
    pub no_leader: u64,
    pub topology_refresh: u64,
    /// Recorded leader-change episodes during the client's lifetime.
    #[serde(default)]
    pub leader_changes: Vec<LeaderChangeEpisode>,
}

/// Metrics counters embedded in [`crate::CrowkvClient`]. Hot-path
/// counters are lock-free atomics; leader-change tracking uses a
/// `Mutex` (rare event, not hot path).
#[derive(Debug, Default)]
pub struct ClientMetrics {
    put_ops: AtomicU64,
    put_errors: AtomicU64,
    get_ops: AtomicU64,
    get_errors: AtomicU64,
    delete_ops: AtomicU64,
    delete_errors: AtomicU64,
    scan_ops: AtomicU64,
    scan_errors: AtomicU64,
    batch_write_ops: AtomicU64,
    batch_write_errors: AtomicU64,
    not_leader_hint_followed: AtomicU64,
    leader_query: AtomicU64,
    unknown_leader_wait: AtomicU64,
    transport_error_retry: AtomicU64,
    retries_exhausted: AtomicU64,
    no_leader: AtomicU64,
    topology_refresh: AtomicU64,
    leader_changes: Mutex<LeaderChangeTracker>,
}

impl ClientMetrics {
    pub(crate) fn record_put(&self, ok: bool) {
        self.put_ops.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.put_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_get(&self, ok: bool) {
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.get_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_delete(&self, ok: bool) {
        self.delete_ops.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.delete_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_scan(&self, ok: bool) {
        self.scan_ops.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.scan_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_batch_write(&self, ok: bool) {
        self.batch_write_ops.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.batch_write_errors.fetch_add(1, Ordering::Relaxed);
        }
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
            put_ops: self.put_ops.load(Ordering::Relaxed),
            put_errors: self.put_errors.load(Ordering::Relaxed),
            get_ops: self.get_ops.load(Ordering::Relaxed),
            get_errors: self.get_errors.load(Ordering::Relaxed),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            delete_errors: self.delete_errors.load(Ordering::Relaxed),
            scan_ops: self.scan_ops.load(Ordering::Relaxed),
            scan_errors: self.scan_errors.load(Ordering::Relaxed),
            batch_write_ops: self.batch_write_ops.load(Ordering::Relaxed),
            batch_write_errors: self.batch_write_errors.load(Ordering::Relaxed),
            not_leader_hint_followed: self.not_leader_hint_followed.load(Ordering::Relaxed),
            leader_query: self.leader_query.load(Ordering::Relaxed),
            unknown_leader_wait: self.unknown_leader_wait.load(Ordering::Relaxed),
            transport_error_retry: self.transport_error_retry.load(Ordering::Relaxed),
            retries_exhausted: self.retries_exhausted.load(Ordering::Relaxed),
            no_leader: self.no_leader.load(Ordering::Relaxed),
            topology_refresh: self.topology_refresh.load(Ordering::Relaxed),
            leader_changes,
        }
    }
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}
