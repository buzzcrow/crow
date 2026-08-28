// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! kv-server keep-alive loop.
//!
//! Registers the instance under `/srv/kv-server/<instance_id>` in
//! group 0 via `ServiceRegistryClient` and heartbeats periodically.
//! On shutdown, the loop unregisters (clean shutdown).

use std::sync::Arc;

use crow_kv_client::{ClientConfig, CrowkvClient, ServiceRegistryClient};
use crow_protocol::common::HostedGroup;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::store_registry::KvStoreRegistry;

/// Keep-alive loop handle. Call `stop` to unregister and terminate the
/// background task.
pub struct KeepAliveLoop {
    handle: Option<JoinHandle<()>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl KeepAliveLoop {
    /// Spawn the keep-alive background task.
    ///
    /// The loop registers immediately, then heartbeats every
    /// `interval_secs`. It reads `hosted_stores` / `hosted_groups`
    /// from the registry each tick so the record reflects live state.
    pub fn spawn(
        registry: Arc<KvStoreRegistry>,
        instance_id: u64,
        rpc_endpoint: String,
        group0_endpoint: &str,
        data_root: String,
        interval_secs: u64,
    ) -> Self {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let ep = group0_endpoint.to_string();
        let handle = tokio::spawn(async move {
            let kv_client = CrowkvClient::new(ClientConfig::new(vec![ep.clone()]));
            kv_client.seed_leader(0, 0, ep);
            let svc = ServiceRegistryClient::new(kv_client);

            // Initial registration.
            let (stores, groups) = hosted_summary(&registry);
            if let Err(e) = svc
                .register_kv_server(instance_id, &rpc_endpoint, &stores, &groups, "ok", &data_root)
                .await
            {
                warn!(error = %e, "keep-alive: initial register failed");
            } else {
                info!(instance_id, "keep-alive: registered");
            }

            let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let mut stop_rx = stop_rx;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let (stores, groups) = hosted_summary(&registry);
                        if let Err(e) = svc
                            .heartbeat_kv_server(instance_id, &rpc_endpoint, &stores, &groups, "ok", &data_root)
                            .await
                        {
                            warn!(error = %e, "keep-alive: heartbeat failed");
                        }
                    }
                    _ = &mut stop_rx => {
                        info!(instance_id, "keep-alive: shutting down; unregistering");
                        // Best-effort unregister: if group-0 is unreachable
                        // (e.g. this server hosts group-0 and is shutting down
                        // its own RPC stack), the registry entry expires via
                        // heartbeat TTL anyway. Keep the timeout short so
                        // shutdown isn't blocked by a dead RPC endpoint.
                        match tokio::time::timeout(
                            tokio::time::Duration::from_millis(100),
                            svc.unregister("kv-server", instance_id),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                warn!(error = %e, "keep-alive: unregister failed");
                            }
                            Err(_) => {
                                warn!(instance_id, "keep-alive: unregister timed out");
                            }
                        }
                        break;
                    }
                }
            }
        });
        Self {
            handle: Some(handle),
            stop_tx: Some(stop_tx),
        }
    }

    /// Stop the loop and unregister. Returns once the task has exited.
    pub async fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            let _ = h.await;
        }
    }
}

impl Drop for KeepAliveLoop {
    fn drop(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

/// Collect hosted store IDs and group IDs from the registry.
fn hosted_summary(registry: &KvStoreRegistry) -> (Vec<u64>, Vec<HostedGroup>) {
    let stores: Vec<u64> = registry.store_ids();
    let mut groups: Vec<HostedGroup> = Vec::new();
    for &sid in &stores {
        if let Some(store) = registry.get_store(sid) {
            for gid in store.group_ids() {
                groups.push(HostedGroup {
                    store_id: sid,
                    group_id: gid,
                });
            }
        }
    }
    (stores, groups)
}
