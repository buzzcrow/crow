// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Periodic topology refresh task.
//!
//! Fetches the full cluster hierarchy from group-0 via `HardwareClient`
//! at a configurable interval. On failure, retains the previous
//! snapshot (stale-but-valid).

use std::time::Duration;

use tracing::{info, warn};

use crow_kv_client::HardwareClient;

use super::{build_snapshot, TopologyCache};

/// Periodic refresh loop. Runs until the stop signal.
///
/// Does an initial synchronous refresh on startup (design decision:
/// option (c) from R86 Open Questions), then switches to periodic.
pub async fn run_refresh_loop(
    cache: TopologyCache,
    hw: HardwareClient,
    interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    // Initial synchronous refresh.
    if let Some(snap) = build_snapshot(&hw).await {
        info!(
            racks = snap.rack_ids().len(),
            dgs = snap.disk_group_count(),
            "topology: initial refresh complete"
        );
        cache.replace(snap);
    } else {
        warn!("topology: initial refresh failed, will retry periodically");
    }

    let mut ticker = tokio::time::interval(interval);
    // The first tick fires immediately; skip it since we just did the
    // initial refresh.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Some(snap) = build_snapshot(&hw).await {
                    info!(
                        racks = snap.rack_ids().len(),
                        dgs = snap.disk_group_count(),
                        "topology: periodic refresh complete"
                    );
                    cache.replace(snap);
                } else {
                    warn!("topology: periodic refresh failed, keeping previous snapshot");
                }
            }
            res = stop.changed() => {
                if res.is_ok() && *stop.borrow() {
                    info!("topology: refresh loop stopping");
                    return;
                }
            }
        }
    }
}
