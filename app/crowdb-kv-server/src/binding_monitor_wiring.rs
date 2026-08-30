// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb range binding monitor wiring.
//!
//! Spawns the generic `BindingMonitor` (from `crowdb-kv-client`) with the
//! chunkdb range strategy as a leader-gated background task on this
//! `crowdb-kv-server`'s group-0 replica. Only the group-0 leader writes
//! the binding table; followers run the tick (read instances + compute
//! assignment) but skip the write phase, so they are ready to take over
//! immediately on leader change.
//!
//! See `doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5
//! (Dynamic Binding Monitor).

use std::sync::Arc;

use crowdb_kv_client::{
    BindingMonitor, ChunkdbRangeStrategy, ClientConfig, CrowdbKvClient, ServiceRegistryClient,
};
use tracing::info;

use crate::store_registry::KvStoreRegistry;

/// Handle to the spawned binding monitor task. Drop to stop (sends the
/// stop signal); await is unnecessary — the task exits on the next tick
/// after the signal is sent.
pub struct BindingMonitorHandle {
    stop_tx: tokio::sync::watch::Sender<bool>,
}

impl BindingMonitorHandle {
    /// Stop the monitor task. Returns immediately; the task exits on
    /// its next tick.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }
}

impl Drop for BindingMonitorHandle {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
    }
}

/// Spawn the chunkdb range binding monitor on this group-0 replica.
///
/// `group0_endpoint` is the crowdb-rpc endpoint of this server's group-0
/// listener (used by the monitor's `CrowdbKvClient` to read/write the
/// binding table + scan the service registry). `interval_secs` is the
/// tick interval; `0` is rejected by the caller (do not call this
/// function when the interval is 0).
///
/// The monitor checks leader status on each tick via the local
/// registry's group-0 replica. If this node is the group-0 leader, it
/// writes the computed binding table; otherwise it computes but skips
/// the write.
#[must_use]
pub fn spawn_chunkdb_binding_monitor(
    registry: &Arc<KvStoreRegistry>,
    group0_endpoint: String,
    interval_secs: u64,
) -> BindingMonitorHandle {
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![
        group0_endpoint.clone()
    ])));
    kv.seed_leader(0, 0, group0_endpoint);
    let svc = ServiceRegistryClient::from_shared(Arc::clone(&kv));
    let strategy = ChunkdbRangeStrategy::new();
    let monitor = BindingMonitor::new(
        kv,
        svc,
        strategy,
        std::time::Duration::from_secs(interval_secs),
        "chunkdb",
    );

    // Leader-gating closure: reads the local group-0 replica's role.
    // `is_leader()` is the single source of truth (updated by
    // `become_leader` / `become_follower` in the election driver).
    let registry_for_leader = Arc::clone(registry);
    let is_leader = move || {
        registry_for_leader
            .get_store(0)
            .and_then(|s| s.get_group(0))
            .is_some_and(|g| g.local_replica().is_leader())
    };

    info!(interval_secs, "chunkdb binding monitor spawning");
    tokio::spawn(monitor.run(stop_rx, is_leader));

    BindingMonitorHandle { stop_tx }
}
