// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `KeepAlive` — keep-alive + periodic hardware sync from group 0.
//!
//! Each tick: heartbeat, read ownership map, read bind map, read
//! member disks per owned disk-group, reconcile in-memory state
//! (disk-add init, status changes, removals).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crow_common::metrics::Counter;
use crow_kv_client::{HardwareClient, ServiceRegistryClient};
use crow_protocol::common::{DiskId, HwStatus};
use crow_protocol::diskdb::rpc::DiskValue;
use crow_protocol::{DiskGroupId, NodeId, RackId};
use crow_protocol::{DiskIdExt, ZoneValueExt};
use tracing::{info, warn};

use crate::data_group_client::DataGroupClient;
use crate::model::disk::DdbDisk;
use crate::model::disk_group::DdbDiskGroup;
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::DdbZone;
use crate::status_machine::HwStateMachine;

/// Elapsed millis as u64 (saturating cast from u128).
fn elapsed_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

/// Outcome of one sync tick.
#[derive(Debug, Default, Clone)]
pub struct KeepAliveOutcome {
    pub groups_added: usize,
    pub groups_removed: usize,
    pub disks_added: usize,
    pub disks_removed: usize,
    pub status_changes: usize,
    pub sync_duration_ms: u64,
}

/// Configuration for the sync loop.
#[derive(Debug, Clone)]
pub struct KeepAliveConfig {
    pub interval: Duration,
    pub miss_threshold: u32,
    pub zone_rotate_count: u32,
    pub cas_retry_limit: u32,
    pub temp_failure_timeout_secs: u32,
}

impl Default for KeepAliveConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        }
    }
}

/// Background sync loop: keep-alive + hardware read + disk-add init.
pub struct KeepAlive {
    hw: HardwareClient,
    svc: ServiceRegistryClient,
    container: Arc<DdbDiskGroupContainer>,
    config: KeepAliveConfig,
    status_machine: HwStateMachine,
    missed_count: u32,
    /// Optional `DataGroupClient` for writing baseline `ZoneValue`
    /// records during disk-add init. When `None`, disk-add init
    /// skips the baseline write (test mode).
    kv: Option<DataGroupClient>,
    /// Optional CAS retry counter handle, attached to each `Zone`
    /// during disk-add init via `Zone::with_cas_retry_metric`.
    cas_retry_metric: Option<Arc<Counter>>,
}

impl KeepAlive {
    pub fn new(
        hw: HardwareClient,
        svc: ServiceRegistryClient,
        container: Arc<DdbDiskGroupContainer>,
        config: KeepAliveConfig,
    ) -> Self {
        let status_machine = HwStateMachine::new(config.temp_failure_timeout_secs);
        Self {
            hw,
            svc,
            container,
            config,
            status_machine,
            missed_count: 0,
            kv: None,
            cas_retry_metric: None,
        }
    }

    /// Attach a `DataGroupClient` for disk-add init baseline writes.
    #[must_use]
    pub fn with_data_group_client(mut self, kv: DataGroupClient) -> Self {
        self.kv = Some(kv);
        self
    }

    /// Attach a CAS retry counter handle for `Zone::with_cas_retry_metric`.
    #[must_use]
    pub fn with_cas_retry_metric(mut self, counter: Arc<Counter>) -> Self {
        self.cas_retry_metric = Some(counter);
        self
    }

    /// Run one sync tick.
    #[allow(clippy::too_many_lines)]
    pub async fn tick(&mut self) -> KeepAliveOutcome {
        let start = std::time::Instant::now();
        let instance_id = self.container.instance_id;

        // a. Keep-alive heartbeat.
        if let Err(e) = self.svc.heartbeat_diskdb(instance_id, "", &[], &[]).await {
            warn!(error = %e, "sync: heartbeat failed");
            self.missed_count += 1;
            if self.missed_count >= self.config.miss_threshold {
                self.container.enter_degraded_mode();
            }
            return KeepAliveOutcome {
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
                return KeepAliveOutcome {
                    sync_duration_ms: elapsed_ms(start),
                    ..Default::default()
                };
            }
        };

        // c. Read bind map.
        let binds = match self.hw.list_binds().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "sync: read bind map failed");
                self.missed_count += 1;
                if self.missed_count >= self.config.miss_threshold {
                    self.container.enter_degraded_mode();
                }
                return KeepAliveOutcome {
                    sync_duration_ms: elapsed_ms(start),
                    ..Default::default()
                };
            }
        };
        let bind_map: HashMap<DiskGroupId, (u64, u64)> = binds
            .into_iter()
            .map(|b| (b.dg_id, (b.store_id, b.group_id)))
            .collect();

        // d. Filter to owned disk-groups.
        let owned: Vec<_> = owners
            .into_iter()
            .filter(|o| o.instance_id == instance_id)
            .collect();

        // e. Reconcile disk-groups.
        let mut outcome = KeepAliveOutcome::default();
        let current_ids: Vec<_> = self.container.disk_group_ids();

        for entry in &owned {
            if !current_ids.contains(&entry.dg_id) {
                // New disk-group assigned.
                let dg = Arc::new(DdbDiskGroup::new(entry.dg_id, entry.node_id, entry.rack_id));
                // Set bind from the bind map.
                if let Some(&(store_id, group_id)) = bind_map.get(&entry.dg_id) {
                    *dg.bind.write().unwrap() = (store_id, group_id);
                }
                self.container.add_disk_group(dg);
                outcome.groups_added += 1;
            } else if let Some(dg) = self.container.get_disk_group(entry.dg_id) {
                // Update bind if changed.
                if let Some(&(store_id, group_id)) = bind_map.get(&entry.dg_id) {
                    let mut bind = dg.bind.write().unwrap();
                    if *bind != (store_id, group_id) {
                        *bind = (store_id, group_id);
                    }
                }
            }
        }

        // f. Detect removed disk-groups.
        for &id in &current_ids {
            if !owned.iter().any(|o| o.dg_id == id) {
                self.container.remove_disk_group(id);
                outcome.groups_removed += 1;
            }
        }

        // g. For each owned disk-group, read member disks and reconcile.
        for entry in &owned {
            let Some(dg) = self.container.get_disk_group(entry.dg_id) else {
                continue;
            };
            let disks = match self
                .hw
                .list_disks_in_group(entry.rack_id, entry.node_id, entry.dg_id)
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    warn!(error = %e, dg_id = entry.dg_id, "sync: list disks failed");
                    continue;
                }
            };
            self.reconcile_disks(
                &dg,
                entry.rack_id,
                entry.node_id,
                entry.dg_id,
                &disks,
                &mut outcome,
            )
            .await;
        }

        // h. Reset missed count on success.
        if self.missed_count > 0 {
            self.missed_count = 0;
            self.container.exit_degraded_mode();
        }

        outcome.sync_duration_ms = elapsed_ms(start);
        info!(
            groups_added = outcome.groups_added,
            groups_removed = outcome.groups_removed,
            disks_added = outcome.disks_added,
            duration_ms = outcome.sync_duration_ms,
            "sync complete"
        );
        outcome
    }

    /// Reconcile the disk list for one disk-group: add new disks,
    /// update status on existing disks, detect removed disks.
    async fn reconcile_disks(
        &self,
        dg: &Arc<DdbDiskGroup>,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disks: &[(DiskId, DiskValue)],
        outcome: &mut KeepAliveOutcome,
    ) {
        let _ = (rack_id, node_id, dg_id);
        let current_disk_ids: Vec<DiskId> = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().map(|d| d.disk_id).collect()
        };

        for (disk_id, disk_value) in disks {
            if current_disk_ids.contains(disk_id) {
                // Existing disk — update status if changed.
                let disk = {
                    let disks_guard = dg.disks.read().unwrap();
                    disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
                };
                if let Some(disk) = disk {
                    let old_status = *disk.effective_status.read().unwrap();
                    let new_status = HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up);
                    if old_status != new_status {
                        match self.status_machine.transition_disk(&disk, new_status) {
                            Ok(_) => {
                                dg.rebuild_allocating_disks();
                                outcome.status_changes += 1;
                            }
                            Err(e) => {
                                warn!(
                                    disk = ?disk.disk_id,
                                    from = ?old_status,
                                    to = ?new_status,
                                    error = %e,
                                    "illegal disk status transition; keeping current"
                                );
                            }
                        }
                    }
                }
            } else {
                // New disk — disk-add init flow.
                self.disk_add_init(dg, *disk_id, disk_value).await;
                outcome.disks_added += 1;
            }
        }

        // Detect removed disks (present in memory but absent from sync).
        for disk_id in &current_disk_ids {
            if !disks.iter().any(|(id, _)| id == disk_id) {
                // Mark as Missing (not removed — §10 says transition
                // to Missing, then Bad after confirmation).
                let disk = {
                    let disks_guard = dg.disks.read().unwrap();
                    disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
                };
                if let Some(disk) = disk {
                    let old_status = *disk.effective_status.read().unwrap();
                    if old_status != HwStatus::Missing {
                        match self.status_machine.transition_disk(&disk, HwStatus::Missing) {
                            Ok(_) => {
                                dg.rebuild_allocating_disks();
                                outcome.status_changes += 1;
                            }
                            Err(e) => {
                                warn!(
                                    disk = ?disk.disk_id,
                                    from = ?old_status,
                                    to = ?HwStatus::Missing,
                                    error = %e,
                                    "illegal disk status transition; keeping current"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Disk-add init flow (§3.5): create `DdbDisk` + `DdbZone`s, write
    /// baseline `ZoneValue` records, rebuild active zones, add to
    /// `DdbDiskGroup.disks`.
    #[allow(clippy::cast_possible_truncation)]
    async fn disk_add_init(&self, dg: &Arc<DdbDiskGroup>, disk_id: DiskId, disk_value: &DiskValue) {
        let zone_count = disk_value.zone_count;
        let zone_size_units = disk_value.zone_size_units;
        let unit_size_bytes = disk_value.unit_size_bytes;
        let _ = unit_size_bytes;

        let disk = Arc::new(DdbDisk::new(
            disk_id,
            dg.disk_group_id,
            dg.node_id,
            dg.rack_id,
            *disk_value,
        ));

        for zi in 0..zone_count {
            // Last zone may be smaller; round down to multiple of 64.
            let unit_capacity = if zi == zone_count - 1 {
                let remaining = disk_value.capacity_units - (u64::from(zi) * zone_size_units);
                let rounded = (remaining / 64) * 64;
                rounded as u32
            } else {
                zone_size_units as u32
            };
            let zone = DdbZone::new(disk_id, zi, dg.disk_group_id, unit_capacity);
            let zone = if let Some(ref counter) = self.cas_retry_metric {
                zone.with_cas_retry_metric(Arc::clone(counter))
            } else {
                zone
            };
            disk.add_zone(Arc::new(zone));
        }

        // Write baseline ZoneValue records (empty bitmap, snapshot_slot=0)
        // — but only if no snapshot already exists (R73: previously-owned
        // disk-groups have real snapshots that must not be overwritten;
        // recovery runs after tick to replay the journal from
        // snapshot_slot).
        if let Some(ref kv) = self.kv {
            let bind = *dg.bind.read().unwrap();
            // Check the first zone only — if it has a snapshot, the
            // disk was previously initialized (disk_add_init writes
            // baseline snapshots for all zones atomically per zone).
            let snapshots_exist = crate::recovery::zone_snapshots_exist(kv, bind, &disk_id, zone_count).await;
            if snapshots_exist {
                info!(
                    disk = %disk_id.to_display_string(),
                    "disk-add init: snapshots already exist, skipping baseline write (recovery will replay)"
                );
            } else {
                for zi in 0..zone_count {
                    let mut zv = crow_protocol::diskdb::rpc::ZoneValue {
                        usage_bitmap: vec![],
                        snapshot_slot: 0,
                        crc32: 0,
                    };
                    zv.compute_checksum();
                    if let Err(e) = kv.put_zone(bind, &disk_id, zi, &zv).await {
                        warn!(error = %e, disk = %disk_id.to_display_string(), zone = zi, "disk-add init: put_zone failed");
                    }
                }
            }
        }

        disk.rebuild_active_zones(self.config.zone_rotate_count);
        dg.add_disk(disk);
        dg.rebuild_allocating_disks();
    }

    /// Run the loop forever (until the stop signal fires).
    pub async fn run(mut self, mut stop: tokio::sync::oneshot::Receiver<()>) {
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = self.tick().await;
                }
                _ = &mut stop => {
                    info!("sync loop shutting down");
                    break;
                }
            }
        }
    }
}
