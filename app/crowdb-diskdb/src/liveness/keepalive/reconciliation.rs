use super::{
    info, recover_disk_to_up, warn, Arc, AtomicBool, DdbDisk, DdbDiskGroup, DiskGroupId, DiskId, DiskValue,
    HwStateMachine, HwStatus, KeepAlive, KeepAliveOutcome, NodeId, ObservedDiskGroup, Ordering, RackId,
    RecoveryScanHandle, RecoveryScanTask,
};

impl KeepAlive {
    pub(super) fn reconcile_ownership(&self, observed: &[ObservedDiskGroup]) -> (usize, usize) {
        let current_ids: Vec<_> = self.container.disk_group_ids();
        let mut groups_added = 0usize;
        let mut groups_removed = 0usize;

        for entry in observed {
            if !current_ids.contains(&entry.owner.dg_id) {
                let dg = Arc::new(DdbDiskGroup::new(
                    entry.owner.dg_id,
                    entry.owner.node_id,
                    entry.owner.rack_id,
                ));
                dg.set_bind(entry.bind);
                self.container.add_disk_group(dg);
                groups_added += 1;
            } else if let Some(dg) = self.container.get_disk_group(entry.owner.dg_id) {
                if dg.bind() != entry.bind {
                    dg.set_bind(entry.bind);
                }
            }
        }

        for &id in &current_ids {
            if !observed.iter().any(|entry| entry.owner.dg_id == id) {
                self.container.remove_disk_group(id);
                groups_removed += 1;
            }
        }

        (groups_added, groups_removed)
    }

    pub(super) async fn reconcile_observed_disks(&self, observed: &[ObservedDiskGroup]) -> KeepAliveOutcome {
        let mut outcome = KeepAliveOutcome::default();
        for entry in observed {
            let Some(dg) = self.container.get_disk_group(entry.owner.dg_id) else {
                continue;
            };
            self.reconcile_disks(&dg, entry, &mut outcome).await;
        }
        outcome
    }

    /// Reconcile the disk list for one disk-group: add new disks,
    /// update status on existing disks, detect removed disks. Drives
    /// the `HwStateMachine` on status changes + the R76 Missing→Bad
    /// confirmation + recovery scan spawn + recovery-to-Up path.
    ///
    /// A.1: fetches `NodeValue` and `DiskGroupValue` from group 0 to
    /// compute the three-level effective status `max(node, group,
    /// disk)`. The group status is applied to `DdbDiskGroup` via
    /// `transition_disk_group`; the effective status is stored on each
    /// `DdbDisk`.
    pub(super) async fn reconcile_disks(
        &self,
        dg: &Arc<DdbDiskGroup>,
        observed: &ObservedDiskGroup,
        outcome: &mut KeepAliveOutcome,
    ) {
        let rack_id = observed.owner.rack_id;
        let node_id = observed.owner.node_id;
        let dg_id = observed.owner.dg_id;
        let disks = &observed.disks;
        let node_status = observed.node_status;
        let group_status = observed.group_status;
        // A.1: apply group status to the in-memory DdbDiskGroup.
        let current_group_status = dg.status();
        if current_group_status != group_status {
            match self.status_machine.transition_disk_group(dg, group_status) {
                Ok(_) => {
                    dg.rebuild_allocating_disks();
                    outcome.status_changes += 1;
                    info!(dg_id, from = ?current_group_status, to = ?group_status, "disk-group status changed");
                }
                Err(e) => {
                    warn!(dg_id, from = ?current_group_status, to = ?group_status, error = %e, "illegal disk-group transition; keeping current");
                }
            }
        }

        let current_disk_ids: Vec<DiskId> = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().map(|d| d.disk_id).collect()
        };

        for (disk_id, disk_value) in disks {
            if current_disk_ids.contains(disk_id) {
                // Existing disk — reset miss count (present in sync).
                self.disk_miss_counts.write().unwrap().remove(disk_id);
                // A.3: disk present → clear Suspect timer.
                self.disk_suspect_since.write().unwrap().remove(disk_id);
                self.reconcile_existing_disk(dg, disk_id, disk_value, node_status, group_status, outcome)
                    .await;
            } else {
                // New disk — disk-add init flow (R81: Init-state).
                self.disk_add_init(dg, *disk_id, disk_value);
                outcome.disks_added += 1;
            }
        }

        // Detect removed disks (present in memory but absent from sync).
        for disk_id in &current_disk_ids {
            if !disks.iter().any(|(id, _)| id == disk_id) {
                self.reconcile_absent_disk(
                    dg,
                    rack_id,
                    node_id,
                    dg_id,
                    disk_id,
                    node_status,
                    group_status,
                    outcome,
                )
                .await;
            }
        }
    }

    /// Reconcile an existing disk present in the sync response: update
    /// status if changed, resume recovery scan if still Bad. A.1:
    /// computes the three-level effective status and stores it on the
    /// disk.
    pub(super) async fn reconcile_existing_disk(
        &self,
        dg: &Arc<DdbDiskGroup>,
        disk_id: &DiskId,
        disk_value: &DiskValue,
        node_status: HwStatus,
        group_status: HwStatus,
        outcome: &mut KeepAliveOutcome,
    ) {
        let disk = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
        };
        let Some(disk) = disk else { return };
        let old_status = disk.effective_status();
        // R81: an Init disk's status is owned by the background zone
        // load task — it transitions Init → disk_value.status only
        // after all zones are loaded. Skipping reconciliation here
        // prevents a sync tick from flipping Init → Up with zero or
        // partially-loaded zones (making the disk allocatable early).
        if old_status == HwStatus::Init {
            // Deferred zone load: the disk was added while the
            // disk-group was unbound (bind == (0,0)). If the bind is
            // now set, spawn the zone load task that was deferred in
            // disk_add_init.
            let bind = dg.bind();
            if bind != (0, 0) {
                if let Some(ref kv) = self.kv {
                    if disk.try_claim_zone_load() {
                        info!(
                            disk = ?disk_id,
                            dg_id = dg.disk_group_id,
                            "reconcile: Init disk now bound; spawning deferred zone load"
                        );
                        self.spawn_zone_load(dg, &disk, disk_value, bind, kv.clone());
                    }
                }
            }
            return;
        }
        let raw_disk_status = HwStatus::try_from(disk_value.status).unwrap_or(HwStatus::Up);
        // A.1: effective = max(node, group, disk).
        let new_effective = HwStateMachine::effective_status(node_status, group_status, raw_disk_status);
        if old_status != new_effective {
            // R76: unified recovery path for → Up transitions.
            if new_effective == HwStatus::Up
                && matches!(
                    old_status,
                    HwStatus::Missing | HwStatus::Bad | HwStatus::Offline | HwStatus::Suspect
                )
            {
                self.recover_disk_to_up(dg, &disk).await;
                outcome.status_changes += 1;
            } else {
                // A.1: directly set the effective status (the
                // transition may not be legal per the disk-only
                // transition table — e.g. node going Offline while
                // disk is Up → effective Offline. This is not a disk
                // status change, it's an effective-status derivation).
                disk.set_effective_status(new_effective);
                dg.rebuild_allocating_disks();
                outcome.status_changes += 1;
            }
        } else if old_status == HwStatus::Bad {
            // R76: on restart, a disk that is still Bad needs its
            // recovery scan resumed (if not already running).
            let scan_running = self.recovery_scans.read().unwrap().contains_key(&disk.disk_id);
            if !scan_running {
                info!(disk = ?disk.disk_id, "disk still Bad on sync — resuming recovery scan");
                self.spawn_recovery_scan(dg, &disk);
            }
        }
    }

    /// Reconcile a disk absent from the sync response. A.3: implements
    /// the Suspect anti-flapping path — `Up → Suspect` on first
    /// absence, response-driven resolution after `miss_threshold`.
    /// A.2: write-back to group 0 before the local transition.
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub(super) async fn reconcile_absent_disk(
        &self,
        dg: &Arc<DdbDiskGroup>,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
        node_status: HwStatus,
        group_status: HwStatus,
        outcome: &mut KeepAliveOutcome,
    ) {
        let disk = {
            let disks_guard = dg.disks.read().unwrap();
            disks_guard.iter().find(|d| d.disk_id == *disk_id).cloned()
        };
        let Some(disk) = disk else { return };
        let old_status = disk.effective_status();

        // R81: Bad disks are kept in memory (recovery scan running).
        if old_status == HwStatus::Bad {
            return;
        }

        // R81: Offline, Maintenance, Init disks are removed from
        // memory directly. Their DiskKey was deleted from group 0
        // (moved or removed); absence means the disk is gone.
        if matches!(
            old_status,
            HwStatus::Offline | HwStatus::Maintenance | HwStatus::Init
        ) {
            dg.remove_disk_from_memory(disk_id);
            self.disk_miss_counts.write().unwrap().remove(disk_id);
            self.disk_suspect_since.write().unwrap().remove(disk_id);
            outcome.disks_removed += 1;
            info!(disk = ?disk_id, status = ?old_status, "disk absent from sync → removed from memory");
            return;
        }

        // Increment consecutive miss count.
        let miss_count = {
            let mut counts = self.disk_miss_counts.write().unwrap();
            let c = counts.entry(*disk_id).or_insert(0);
            *c += 1;
            *c
        };

        // A.3: Up → Suspect on first absence (anti-flapping buffer).
        // Suspect is not allocatable; free is allowed. Suspect is
        // local-only — NOT written back to group 0. Writing it back
        // would conflict with A.1c (effective = max over group-0 disk
        // status): on rediscovery the raw group-0 status would be
        // Suspect, making effective = Suspect with no path back to Up
        // (the Suspect timer is cleared on presence, so
        // check_suspect_timeout never fires). Keeping Suspect local
        // means the group-0 disk status stays Up, so rediscovery
        // yields effective = Up and recover_disk_to_up fires.
        if old_status == HwStatus::Up {
            disk.set_effective_status(HwStatus::Suspect);
            self.disk_suspect_since
                .write()
                .unwrap()
                .insert(*disk_id, std::time::Instant::now());
            dg.rebuild_allocating_disks();
            outcome.status_changes += 1;
            info!(disk = ?disk_id, "disk absent from sync → Suspect (anti-flapping)");
            return;
        }

        // A.3: Suspect state — resolve based on miss count.
        if old_status == HwStatus::Suspect {
            if miss_count >= self.config.miss_threshold {
                // Threshold reached — resolve based on the sync
                // response context. Since the disk is absent, the
                // response either didn't contain it (Missing) or the
                // whole sync failed (Offline). We can't distinguish
                // here (the caller already has the response), so we
                // use the node/group status: if the node or group is
                // not Up, the whole thing is down → Offline;
                // otherwise the disk was removed → Missing.
                let resolved = if node_status != HwStatus::Up || group_status != HwStatus::Up {
                    HwStatus::Offline
                } else {
                    HwStatus::Missing
                };
                self.write_back_disk_status(rack_id, node_id, dg_id, disk_id, resolved)
                    .await;
                match self.status_machine.transition_disk(&disk, resolved) {
                    Ok(_) => {
                        dg.rebuild_allocating_disks();
                        outcome.status_changes += 1;
                        self.disk_suspect_since.write().unwrap().remove(disk_id);
                        info!(disk = ?disk_id, miss_count, resolved = ?resolved, "Suspect → resolved after miss_threshold");
                        if resolved == HwStatus::Missing {
                            // Keep counting — Missing → Bad after
                            // further confirmation.
                        } else {
                            // Offline — stop counting.
                            self.disk_miss_counts.write().unwrap().remove(disk_id);
                        }
                    }
                    Err(e) => {
                        warn!(
                            disk = ?disk_id,
                            from = ?old_status,
                            to = ?resolved,
                            error = %e,
                            "illegal Suspect resolution; keeping current"
                        );
                    }
                }
            } else {
                // A.3: check_suspect_timeout — Suspect → Offline
                // after temp_failure_timeout (intermittent absences
                // that never reach miss_threshold).
                let suspect_timed_out = {
                    let since_guard = self.disk_suspect_since.read().unwrap();
                    since_guard.get(disk_id).is_some_and(|&s| {
                        self.status_machine
                            .check_suspect_timeout(s, std::time::Instant::now())
                    })
                };
                if suspect_timed_out {
                    self.write_back_disk_status(rack_id, node_id, dg_id, disk_id, HwStatus::Offline)
                        .await;
                    match self.status_machine.transition_disk(&disk, HwStatus::Offline) {
                        Ok(_) => {
                            dg.rebuild_allocating_disks();
                            outcome.status_changes += 1;
                            self.disk_suspect_since.write().unwrap().remove(disk_id);
                            self.disk_miss_counts.write().unwrap().remove(disk_id);
                            info!(disk = ?disk_id, "Suspect → Offline (temp_failure_timeout)");
                        }
                        Err(e) => {
                            warn!(
                                disk = ?disk_id,
                                from = ?old_status,
                                to = ?HwStatus::Offline,
                                error = %e,
                                "illegal Suspect → Offline; keeping current"
                            );
                        }
                    }
                }
            }
            return;
        }

        // old_status == Missing — continue the Missing → Bad path.
        if old_status == HwStatus::Missing && miss_count >= self.config.miss_threshold {
            self.write_back_disk_status(rack_id, node_id, dg_id, disk_id, HwStatus::Bad)
                .await;
            match self.status_machine.transition_disk(&disk, HwStatus::Bad) {
                Ok(_) => {
                    dg.rebuild_allocating_disks();
                    outcome.status_changes += 1;
                    info!(
                        disk = ?disk_id,
                        miss_count = miss_count,
                        "disk absent for N ticks → Bad; starting recovery scan"
                    );
                    self.spawn_recovery_scan(dg, &disk);
                    self.disk_miss_counts.write().unwrap().remove(disk_id);
                }
                Err(e) => {
                    warn!(
                        disk = ?disk_id,
                        from = ?old_status,
                        to = ?HwStatus::Bad,
                        error = %e,
                        "illegal disk status transition; keeping current"
                    );
                }
            }
        }
    }

    /// A.2: best-effort write-back of a disk's status to group 0
    /// before the local transition. Failures are logged and ignored —
    /// the local transition is the safety-critical action; the
    /// write-back is observability for the operator.
    pub(super) async fn write_back_disk_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
        status: HwStatus,
    ) {
        if let Err(e) = self
            .hw
            .set_disk_status(rack_id, node_id, dg_id, disk_id, status)
            .await
        {
            warn!(
                disk = ?disk_id,
                status = ?status,
                error = %e,
                "write-back: set_disk_status failed (best-effort; local transition proceeds)"
            );
        }
    }

    /// Spawn a per-disk recovery scan task (R76). The task runs
    /// independently (`tokio::spawn`) and is tracked in
    /// `recovery_scans` for cancellation on recovery.
    pub(super) fn spawn_recovery_scan(&self, dg: &Arc<DdbDiskGroup>, disk: &Arc<DdbDisk>) {
        let Some(ref kv) = self.kv else {
            warn!(disk = ?disk.disk_id, "recovery scan: no kv client, skipping");
            return;
        };
        let Some(ref impacted) = self.impacted_blocks else {
            warn!(disk = ?disk.disk_id, "recovery scan: no impacted-blocks gauge, skipping");
            return;
        };
        let bind = dg.bind();
        let cancel = Arc::new(AtomicBool::new(false));
        let task = RecoveryScanTask::new(
            Arc::clone(disk),
            bind,
            kv.clone(),
            Arc::clone(&cancel),
            Arc::clone(impacted),
        );
        let disk_id = disk.disk_id;
        let join = tokio::spawn(async move {
            task.run().await;
        });
        self.recovery_scans
            .write()
            .unwrap()
            .insert(disk_id, RecoveryScanHandle { cancel, join });
    }

    /// Unified recovery path for `Missing → Up`, `Bad → Up`, `Offline
    /// → Up` (R76). Cancels the recovery scan (if running), drops the
    /// disk's impacted-blocks contribution, routes the status
    /// transition through the state machine (single source of truth),
    /// then runs compaction + rebuild + re-include.
    pub(super) async fn recover_disk_to_up(&self, dg: &Arc<DdbDiskGroup>, disk: &Arc<DdbDisk>) {
        // Cancel any running recovery scan.
        let scan_handle = self.recovery_scans.write().unwrap().remove(&disk.disk_id);
        if let Some(handle) = scan_handle {
            handle.cancel.store(true, Ordering::Release);
            // The task will see the cancel flag on its next zone
            // boundary and exit. We don't await it here — the scan
            // persists its own progress on cancel.
            info!(disk = ?disk.disk_id, "recovery scan cancelled (disk → Up)");
        }

        // Drop this disk's contribution to the cluster impacted-blocks
        // gauge.
        if let Some(ref ib) = self.impacted_blocks {
            ib.remove_disk(&disk.disk_id);
        }

        // Route the status transition through the state machine so it
        // is the single source of truth (validates legality, including
        // the operator-override `Bad → Up`, and sets `effective_status`).
        if let Err(e) = self.status_machine.transition_disk(disk, HwStatus::Up) {
            warn!(
                disk = ?disk.disk_id,
                error = %e,
                "recovery-to-Up: illegal transition, keeping current status"
            );
            return;
        }

        // Run the recovery-to-Up path: compaction + rebuild. The
        // status is already set by `transition_disk` above; the
        // `recover_disk_to_up` helper no longer sets it itself.
        if let Some(ref kv) = self.kv {
            let bind = dg.bind();
            if let Some(m) = &self.metrics {
                recover_disk_to_up(disk, bind, kv, self.config.zone_rotate_count, m).await;
            } else {
                let m = crate::metrics::DiskdbMetrics::disabled();
                recover_disk_to_up(disk, bind, kv, self.config.zone_rotate_count, &m).await;
            }
        } else {
            // No kv client (test mode) — status already set; just
            // rebuild active zones.
            disk.rebuild_active_zones(self.config.zone_rotate_count);
        }
        dg.rebuild_allocating_disks();
        info!(disk = ?disk.disk_id, "disk recovered → Up");
    }
}
