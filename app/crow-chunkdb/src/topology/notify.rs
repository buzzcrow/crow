// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Watch/notify handler for chunkdb topology updates.
//!
//! Subscribes to group-0 prefixes (`/hw/node/`, `/hw/dg/`) and applies
//! fine-grained updates to the `TopologyCache` on notify. On
//! missed-notification (stream drop), the periodic refresh loop
//! corrects the cache within one refresh cycle.

use tokio::sync::mpsc;
use tracing::{info, warn};

use crow_kv_client::{HardwareClient, WatchNotifyClient};
use crow_protocol::key::{DiskGroupKey, NodeKey, TextKey};

use super::{TopologyCache, CHUNKDB_WATCH_PREFIXES, G0_GROUP, G0_STORE};

/// chunkdb-side watch/notify handler for topology updates.
pub struct NotifyHandler {
    watch: WatchNotifyClient,
    hw: HardwareClient,
    cache: TopologyCache,
}

impl NotifyHandler {
    #[must_use]
    pub fn new(watch: WatchNotifyClient, hw: HardwareClient, cache: TopologyCache) -> Self {
        Self { watch, hw, cache }
    }

    /// Open subscriptions and loop on notify frames. Each frame triggers
    /// a fine-grained cache update. Runs until the stop signal.
    pub async fn run(self, mut stop: tokio::sync::watch::Receiver<bool>) {
        let prefixes: &[&[u8]] = CHUNKDB_WATCH_PREFIXES;

        let mut subscriptions = Vec::new();
        for prefix in prefixes {
            match self.watch.subscribe(G0_STORE, G0_GROUP, prefix) {
                Ok(sub) => {
                    info!(prefix = ?prefix, "topology notify: subscribed");
                    subscriptions.push(sub);
                }
                Err(e) => {
                    warn!(prefix = ?prefix, error = %e, "topology notify: subscribe failed");
                }
            }
        }

        if subscriptions.is_empty() {
            warn!("topology notify: no subscriptions established, exiting");
            return;
        }

        let (merge_tx, mut merge_rx) = mpsc::channel::<Vec<u8>>(64);
        let mut merge_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for mut sub in subscriptions {
            let tx = merge_tx.clone();
            merge_handles.push(tokio::spawn(async move {
                while let Some(notify) = sub.notify_rx.recv().await {
                    // Forward the first changed key (coalesced refresh
                    // handles the rest).
                    if let Some(key) = notify.keys.first() {
                        if tx.send(key.clone()).await.is_err() {
                            break;
                        }
                    }
                }
                // sub drops here, triggering the abort signal.
            }));
        }
        drop(merge_tx);

        loop {
            tokio::select! {
                _ = stop.changed() => {
                    if *stop.borrow() {
                        info!("topology notify: stopping");
                        for handle in merge_handles {
                            handle.abort();
                        }
                        return;
                    }
                }
                recv = merge_rx.recv() => {
                    if let Some(key) = recv {
                        self.handle_notify_key(&key).await;
                    } else {
                        warn!("topology notify: all subscriptions closed, exiting");
                        return;
                    }
                }
            }
        }
    }

    /// Handle a single changed key from a notify frame.
    async fn handle_notify_key(&self, key: &[u8]) {
        let Ok(key_str) = std::str::from_utf8(key) else {
            return;
        };

        // Try parsing as a node key.
        if let Ok(node_key) = NodeKey::from_path(key_str) {
            match self.hw.get_node(node_key.rack_id, node_key.node_id).await {
                Ok(Some(nv)) => {
                    self.cache.update_node_status(
                        node_key.rack_id,
                        node_key.node_id,
                        nv.status,
                        nv.disk_group_ids,
                    );
                    info!(
                        rack = node_key.rack_id,
                        node = node_key.node_id,
                        status = ?nv.status,
                        "topology notify: node updated"
                    );
                }
                Ok(None) => {
                    warn!(key = key_str, "topology notify: node not found");
                }
                Err(e) => {
                    warn!(key = key_str, error = %e, "topology notify: node fetch failed");
                }
            }
            return;
        }

        // Try parsing as a disk-group key.
        if let Ok(dg_key) = DiskGroupKey::from_path(key_str) {
            match self
                .hw
                .get_disk_group(dg_key.rack_id, dg_key.node_id, dg_key.disk_group_id)
                .await
            {
                Ok(Some(entry)) => {
                    self.cache.update_disk_group(entry);
                    info!(
                        rack = dg_key.rack_id,
                        node = dg_key.node_id,
                        dg = dg_key.disk_group_id,
                        "topology notify: disk-group updated"
                    );
                }
                Ok(None) => {
                    self.cache.remove_disk_group(dg_key.disk_group_id);
                    info!(dg = dg_key.disk_group_id, "topology notify: disk-group deleted");
                }
                Err(e) => {
                    warn!(key = key_str, error = %e, "topology notify: disk-group fetch failed");
                }
            }
        }
    }
}
