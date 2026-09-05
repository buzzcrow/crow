use super::{
    build_compact_zone_response, build_get_scan_status_response, build_rebuild_zone_bitmap_response,
    build_recalc_response, build_trigger_scan_response, compact_zone, submit_error, submit_fb_response,
    unit_capacity_for_zone, Arc, DdbDisk, DdbDiskGroup, DiskId, DiskValue, DiskdbRpcService,
    FBCompactZoneRequest, FBDiskdbRetCode, FBGetScanStatusRequest, FBMsgType, FBRebuildZoneBitmapRequest,
    FBRecalcDiskUsageRequest, FBTriggerScanRequest, RpcServer, ScanSummary, ServerRequest, ALL_ZONES,
};

impl DiskdbRpcService {
    // ── RebuildZoneBitmap ────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_rebuild_zone_bitmap(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ERebuildZoneBitmapResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBRebuildZoneBitmapRequest>(req.control()) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };

        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::Unavailable,
                "diskdb not ready",
            );
            return;
        }

        let Some(fb_disk_id) = fb_req.disk_id() else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "disk_id required",
            );
            return;
        };
        let disk_id = DiskId {
            high: fb_disk_id.high(),
            low: fb_disk_id.low(),
        };
        let req_zone_index = fb_req.zone_index();

        // Find the disk-group + disk that owns this disk.
        let Some((dg, disk_value, disk_value_disk_id)) = self.find_disk_value(&disk_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::NotOwner,
                "disk not owned",
            );
            return;
        };

        let bind = dg.bind();
        let zone_count = disk_value.zone_count;
        let zones_to_rebuild: Vec<u32> = if req_zone_index == ALL_ZONES {
            (0..zone_count).collect()
        } else {
            if req_zone_index >= zone_count {
                submit_error(
                    server,
                    req.conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    FBDiskdbRetCode::InvalidArgument,
                    "zone_index out of range",
                );
                return;
            }
            vec![req_zone_index]
        };

        let zone_loader = Arc::clone(&self.zone_loader);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let mut rebuilt_zone_count = 0u32;
            let mut total_busy_units = 0u64;
            let mut total_free_units = 0u64;
            let mut err_msg: Option<String> = None;
            for zi in zones_to_rebuild {
                let unit_capacity =
                    unit_capacity_for_zone(&disk_value, zi, zone_count, disk_value.zone_size_units);
                match zone_loader
                    .rebuild_zone_bitmap_full_scan(
                        bind,
                        disk_value_disk_id,
                        zi,
                        dg.disk_group_id,
                        unit_capacity,
                    )
                    .await
                {
                    Ok((_zone, stats)) => {
                        rebuilt_zone_count += 1;
                        total_busy_units += stats.used_units;
                        total_free_units += stats.free_units;
                    }
                    Err(e) => {
                        err_msg = Some(format!("rebuild_zone_bitmap failed for zone {zi}: {e}"));
                        break;
                    }
                }
            }
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            if let Some(msg) = err_msg {
                let ctrl = build_rebuild_zone_bitmap_response(
                    req_id,
                    create_nano,
                    FBDiskdbRetCode::Internal,
                    Some(&msg),
                    0,
                    0,
                    0,
                );
                submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
            } else {
                let ctrl = build_rebuild_zone_bitmap_response(
                    req_id,
                    create_nano,
                    FBDiskdbRetCode::Success,
                    None,
                    rebuilt_zone_count,
                    total_busy_units,
                    total_free_units,
                );
                submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
            }
        });
    }

    // ── RecalcDiskUsage ──────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_recalc_disk_usage(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ERecalcDiskUsageResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBRecalcDiskUsageRequest>(req.control()) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };

        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::Unavailable,
                "diskdb not ready",
            );
            return;
        }

        let has_dg = fb_req.has_disk_group_id();
        let dg_id = fb_req.disk_group_id();
        let recalc = Arc::clone(&self.recalc);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let results = if has_dg {
                if let Some(r) = recalc.recalc_disk_group(dg_id).await {
                    vec![r]
                } else {
                    let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
                    let ctrl = build_recalc_response(
                        req_id,
                        create_nano,
                        FBDiskdbRetCode::DiskGroupNotFound,
                        Some("disk-group not owned"),
                        &[],
                    );
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                    return;
                }
            } else {
                recalc.recalc_all().await
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            let ctrl = build_recalc_response(req_id, create_nano, FBDiskdbRetCode::Success, None, &results);
            submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
        });
    }

    // ── CompactZone ──────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_compact_zone(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ECompactZoneResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBCompactZoneRequest>(req.control()) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };

        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::Unavailable,
                "diskdb not ready",
            );
            return;
        }

        let Some(fb_disk_id) = fb_req.disk_id() else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "disk_id required",
            );
            return;
        };
        let disk_id = DiskId {
            high: fb_disk_id.high(),
            low: fb_disk_id.low(),
        };
        let zone_indices: Vec<u32> = fb_req
            .zone_indices()
            .map(|v| v.iter().collect())
            .unwrap_or_default();

        let Some((dg, disk)) = self.find_disk(&disk_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::NotOwner,
                "disk not owned",
            );
            return;
        };

        let bind = dg.bind();
        let zone_count = disk.disk_value.zone_count;
        let zones_to_compact: Vec<u32> = if zone_indices.is_empty() {
            (0..zone_count).collect()
        } else {
            for &zi in &zone_indices {
                if zi >= zone_count {
                    submit_error(
                        server,
                        req.conn_handle,
                        req_id,
                        create_nano,
                        msg_type,
                        FBDiskdbRetCode::InvalidArgument,
                        "zone_index out of range",
                    );
                    return;
                }
            }
            zone_indices
        };

        let kv = Arc::clone(&self.kv);
        let metrics = Arc::clone(&self.metrics);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let mut results: Vec<(u32, bool, u32, Option<String>)> =
                Vec::with_capacity(zones_to_compact.len());
            let mut compacted_count = 0u32;
            let mut total_deleted = 0u32;
            for zi in zones_to_compact {
                let zone = {
                    let zones = disk.zones.load();
                    let loaded = zones.len();
                    if zi as usize >= loaded {
                        drop(zones);
                        results.push((
                            zi,
                            false,
                            0,
                            Some(format!("zone_index {zi} out of range (loaded zones {loaded})")),
                        ));
                        continue;
                    }
                    Arc::clone(&zones[zi as usize])
                };
                let backlog_before = zone
                    .uncompacted_free_record_count
                    .load(std::sync::atomic::Ordering::Acquire);
                match compact_zone(&kv, bind, disk_id, &zone, zi, &metrics).await {
                    Ok(()) => {
                        let backlog_after = zone
                            .uncompacted_free_record_count
                            .load(std::sync::atomic::Ordering::Acquire);
                        let deleted = backlog_before.saturating_sub(backlog_after);
                        compacted_count += 1;
                        total_deleted += deleted;
                        results.push((zi, true, deleted, None));
                    }
                    Err(e) => {
                        results.push((zi, false, 0, Some(format!("{e}"))));
                    }
                }
            }
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            let ctrl = build_compact_zone_response(
                req_id,
                create_nano,
                FBDiskdbRetCode::Success,
                None,
                compacted_count,
                total_deleted,
                &results,
            );
            submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
        });
    }

    // ── TriggerScan ──────────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_trigger_scan(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ETriggerScanResponse.0 as u16;

        let Ok(_fb_req) = flatbuffers::root::<FBTriggerScanRequest>(req.control()) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };

        let in_progress = self.scan_state.request_scan();
        let summary = self.scan_state.last_summary().unwrap_or_default();
        let ctrl = build_trigger_scan_response(
            req_id,
            create_nano,
            FBDiskdbRetCode::Success,
            None,
            &summary,
            in_progress,
        );
        submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
    }

    // ── GetScanStatus ────────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_get_scan_status(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EGetScanStatusResponse.0 as u16;

        let Ok(_fb_req) = flatbuffers::root::<FBGetScanStatusRequest>(req.control()) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };

        let (summary, has_run) = match self.scan_state.last_summary() {
            Some(s) => (s, true),
            None => (ScanSummary::default(), false),
        };
        let ctrl = build_get_scan_status_response(
            req_id,
            create_nano,
            FBDiskdbRetCode::Success,
            None,
            &summary,
            has_run,
        );
        submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
    }

    // ── Helpers ──────────────────────────────────────────────────

    /// Find the disk-group that owns `disk_id`.
    pub(super) fn find_disk_group_for_disk(&self, disk_id: &DiskId) -> Option<Arc<DdbDiskGroup>> {
        let dg_ids = self.container.disk_group_ids();
        for dg_id in dg_ids {
            if let Some(n) = self.container.get_disk_group(dg_id) {
                let owns = {
                    let disks = n.disks.read().unwrap();
                    disks.iter().any(|d| &d.disk_id == disk_id)
                };
                if owns {
                    return Some(n);
                }
            }
        }
        None
    }

    /// Find the disk-group + disk that owns `disk_id`.
    pub(super) fn find_disk(&self, disk_id: &DiskId) -> Option<(Arc<DdbDiskGroup>, Arc<DdbDisk>)> {
        let dg_ids = self.container.disk_group_ids();
        for dg_id in dg_ids {
            if let Some(n) = self.container.get_disk_group(dg_id) {
                let disk_clone = {
                    let disks = n.disks.read().unwrap();
                    disks.iter().find(|d| &d.disk_id == disk_id).cloned()
                };
                if let Some(d) = disk_clone {
                    return Some((n, d));
                }
            }
        }
        None
    }

    /// Find the disk-group + `DiskValue` + `disk_id` for `disk_id`.
    pub(super) fn find_disk_value(&self, disk_id: &DiskId) -> Option<(Arc<DdbDiskGroup>, DiskValue, DiskId)> {
        let dg_ids = self.container.disk_group_ids();
        for dg_id in dg_ids {
            if let Some(n) = self.container.get_disk_group(dg_id) {
                let dv_clone = {
                    let disks = n.disks.read().unwrap();
                    disks
                        .iter()
                        .find(|d| &d.disk_id == disk_id)
                        .map(|d| (d.disk_value.clone(), d.disk_id))
                };
                if let Some((dv, did)) = dv_clone {
                    return Some((n, dv, did));
                }
            }
        }
        None
    }
}
