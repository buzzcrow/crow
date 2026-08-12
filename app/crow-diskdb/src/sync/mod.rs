// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `SyncLoop` — keep-alive + periodic hardware sync from group 0.

use std::sync::Arc;
use std::time::Duration;

use crow_kv_client::{HardwareClient, ServiceRegistryClient};
use tracing::{info, warn};

use crate::node::NodeContainer;

/// Elapsed millis as u64 (saturating cast from u128).
fn elapsed_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

/// Outcome of one sync tick.
#[derive(Debug, Default, Clone)]
pub struct SyncOutcome {
    pub groups_added: usize,
    pub groups_removed: usize,
    pub disks_added: usize,
    pub disks_removed: usize,
    pub status_changes: usize,
    pub sync_duration_ms: u64,
}

/// Configuration for the sync loop.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub interval: Duration,
    pub miss_threshold: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
        }
    }
}

/// Background sync loop: keep-alive + hardware read.
pub struct SyncLoop {
    hw: HardwareClient,
    svc: ServiceRegistryClient,
    container: Arc<NodeContainer>,
    config: SyncConfig,
    missed_count: u32,
}

impl SyncLoop {
    pub fn new(
        hw: HardwareClient,
        svc: ServiceRegistryClient,
        container: Arc<NodeContainer>,
        config: SyncConfig,
    ) -> Self {
        Self {
            hw,
            svc,
            container,
            config,
            missed_count: 0,
        }
    }

    /// Run one sync tick.
    pub async fn sync_once(&mut self) -> SyncOutcome {
        let start = std::time::Instant::now();
        let instance_id = self.container.instance_id;

        // a. Keep-alive heartbeat.
        if let Err(e) = self.svc.heartbeat_diskdb(instance_id, "", &[]).await {
            warn!(error = %e, "sync: heartbeat failed");
            self.missed_count += 1;
            if self.missed_count >= self.config.miss_threshold {
                self.container.enter_degraded_mode();
            }
            return SyncOutcome {
                sync_duration_ms: elapsed_ms(start),
                ..Default::default()
            };
        }

        // b. Read ownership map.
        let owners = match self.hw.list_owners().await {
            Ok(o) => o,
            Err(e) => {
                warn!(error = %e, "sync: read owner map failed");
                self.missed_count += 1;
                if self.missed_count >= self.config.miss_threshold {
                    self.container.enter_degraded_mode();
                }
                return SyncOutcome {
                    sync_duration_ms: elapsed_ms(start),
                    ..Default::default()
                };
            }
        };

        // c. Filter to owned disk-groups.
        let owned: Vec<_> = owners
            .into_iter()
            .filter(|o| o.instance_id == instance_id)
            .collect();

        // d. Read hardware for each owned disk-group.
        let mut outcome = SyncOutcome::default();
        let current_ids: Vec<_> = self.container.node_ids();

        for entry in &owned {
            if !current_ids.contains(&entry.dg_id) {
                // New disk-group assigned.
                let node = Arc::new(crate::node::Node::new(entry.dg_id, entry.node_id, entry.rack_id));
                self.container.add_node(node);
                outcome.groups_added += 1;
            }
        }

        // e. Detect removed disk-groups.
        for &id in &current_ids {
            if !owned.iter().any(|o| o.dg_id == id) {
                self.container.remove_node(id);
                outcome.groups_removed += 1;
            }
        }

        // f. Reset missed count on success.
        if self.missed_count > 0 {
            self.missed_count = 0;
            self.container.exit_degraded_mode();
        }

        outcome.sync_duration_ms = elapsed_ms(start);
        info!(
            groups_added = outcome.groups_added,
            groups_removed = outcome.groups_removed,
            duration_ms = outcome.sync_duration_ms,
            "sync complete"
        );
        outcome
    }

    /// Run the loop forever (until the stop signal fires).
    pub async fn run(mut self, mut stop: tokio::sync::oneshot::Receiver<()>) {
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = self.sync_once().await;
                }
                _ = &mut stop => {
                    info!("sync loop shutting down");
                    break;
                }
            }
        }
    }
}
