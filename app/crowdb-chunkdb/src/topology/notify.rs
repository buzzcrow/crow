// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Watch/notify handler for chunkdb topology updates.
//!
//! Subscribes to group-0 prefixes (`/hw/node/`, `/hw/dg/`) and publishes
//! a complete refreshed `TopologyCache` snapshot on notify. On
//! missed-notification (stream drop), the periodic refresh loop
//! corrects the cache within one refresh cycle.

use tokio::sync::mpsc;
use tracing::{info, warn};

use super::{build_snapshot, TopologyCache, CHUNKDB_WATCH_PREFIXES, G0_GROUP, G0_STORE};
use crowdb_kv_client::{HardwareClient, WatchNotifyClient};

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
    /// a complete cache refresh. Runs until the stop signal.
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

        let (merge_tx, mut merge_rx) = mpsc::channel::<()>(1);
        let mut merge_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for mut sub in subscriptions {
            let tx = merge_tx.clone();
            merge_handles.push(tokio::spawn(async move {
                while let Some(notify) = sub.notify_rx.recv().await {
                    if !notify.keys.is_empty() && tx.send(()).await.is_err() {
                        break;
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
                    if recv.is_some() {
                        self.refresh_snapshot().await;
                    } else {
                        warn!("topology notify: all subscriptions closed, exiting");
                        return;
                    }
                }
            }
        }
    }

    async fn refresh_snapshot(&self) {
        if let Some(snapshot) = build_snapshot(&self.hw).await {
            info!(
                racks = snapshot.rack_ids().len(),
                dgs = snapshot.disk_group_count(),
                "topology notify: complete snapshot refreshed"
            );
            self.cache.replace(snapshot);
        } else {
            warn!("topology notify: full refresh failed, retaining previous snapshot");
        }
    }
}
