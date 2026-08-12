// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Retry logic, topology cache refresh, and `NotLeaderHint` handling
//! for [`CrowkvClient`].

use std::time::Duration;

use crow_kv::rpc::{KvErrorCode, KvResponse};
use tracing::warn;

use crate::client::CrowkvClient;
use crate::error::{Error, Result};

impl CrowkvClient {
    /// If `resp` carries a `NotLeaderHint`, follow it immediately (uncounted
    /// retry — forward progress toward the real leader) and update the
    /// topology cache. Returns `None` if `resp` did not indicate not-leader
    /// (caller should treat it as a normal application error).
    pub(crate) fn follow_not_leader(
        &self,
        store_id: u64,
        group_id: u64,
        resp: &KvResponse,
    ) -> Option<String> {
        if resp.not_leader_hint.is_empty() {
            return None;
        }
        self.topology
            .set_leader(store_id, group_id, resp.not_leader_hint.clone());
        Some(resp.not_leader_hint.clone())
    }

    /// A `not leader` failure with an empty hint (the responding replica
    /// doesn't know who its leader is either -- typically mid-election,
    /// e.g. right after a restart; a real hint would have already been
    /// handled by [`Self::follow_not_leader`] before this is checked).
    /// Checks the structured `error_code` first, falling back to the
    /// string for old servers that don't set the code (default 0 =
    /// `KvErrorNone`).
    pub(crate) fn is_unknown_leader(error_code: i32, error: &str) -> bool {
        error_code == KvErrorCode::KvErrorNotLeader as i32 || error == "not leader"
    }

    /// After an [`Self::is_unknown_leader`] failure, give the election a
    /// chance to converge and pick up whatever leader the cache learns in
    /// the meantime, instead of busy-retrying the same non-answering
    /// replica ("100ms-then-retry"). Logs refresh failures instead of
    /// silently swallowing them; the caller's `count_other` surfaces
    /// `RetriesExhausted` if the endpoint stays stale.
    pub(crate) async fn wait_and_refresh_leader(
        &self,
        store_id: u64,
        group_id: u64,
        endpoint: &str,
    ) -> String {
        self.metrics.record_unknown_leader_wait();
        self.metrics.record_leader_query();
        self.metrics.record_topology_refresh();
        if let Err(e) = self.topology.refresh().await {
            warn!(error = %e, "topology refresh failed in wait_and_refresh_leader");
        }
        tokio::time::sleep(self.retry.unknown_leader_wait).await;
        self.topology
            .leader(store_id, group_id)
            .unwrap_or_else(|| endpoint.to_string())
    }

    /// Transport-level failure (connect/timeout/unavailable): best-effort
    /// topology refresh (covers "leader moved and we don't know where"),
    /// exponential backoff, then return the (possibly updated) endpoint to
    /// retry against. Logs refresh failures instead of silently
    /// swallowing them; the caller's `count_other` surfaces
    /// `RetriesExhausted` if the endpoint stays stale.
    pub(crate) async fn handle_transport_err(
        &self,
        store_id: u64,
        group_id: u64,
        current: &str,
        backoff: &mut Duration,
    ) -> String {
        self.metrics.record_topology_refresh();
        if let Err(e) = self.topology.refresh().await {
            warn!(error = %e, "topology refresh failed in handle_transport_err");
        }
        let endpoint = self
            .topology
            .leader(store_id, group_id)
            .unwrap_or_else(|| current.to_string());
        tokio::time::sleep(*backoff).await;
        *backoff = (*backoff * 2).min(self.retry.backoff_max);
        endpoint
    }

    /// Count one non-`NotLeaderHint` retryable outcome; errors once the
    /// configured retry budget (`RetryConfig::max_retries`) is exhausted.
    ///
    /// # Errors
    /// `Error::RetriesExhausted` once `attempts` exceeds the budget.
    pub(crate) fn count_other(&self, attempts: u32, last: &str) -> Result<u32> {
        let attempts = attempts + 1;
        if attempts > self.retry.max_retries {
            self.metrics.record_retries_exhausted();
            return Err(Error::RetriesExhausted {
                attempts,
                last: last.to_string(),
            });
        }
        Ok(attempts)
    }
}
