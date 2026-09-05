use super::{
    build_get_disk_group_info_response, build_get_disk_info_response, build_query_capacity_response,
    build_query_capacity_response_zone, submit_error, submit_fb_response, Arc, DdbDisk, DiskGroupQueryEntry,
    DiskId, DiskdbRpcService, FBDiskdbRetCode, FBGetDiskGroupInfoRequest, FBGetDiskInfoRequest, FBMsgType,
    FBQueryCapacityStatsRequest, RequestGuard, RpcServer, ServerRequest,
};

impl DiskdbRpcService {
    // ── QueryCapacityStats ───────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_query_capacity(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EQueryCapacityStatsResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBQueryCapacityStatsRequest>(req.control()) else {
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

        let disk_group_id = fb_req.disk_group_id();
        let has_zone = fb_req.has_zone_index();
        let zone_index = fb_req.zone_index();
        let disk_id = fb_req.disk_id().map(|id| DiskId {
            high: id.high(),
            low: id.low(),
        });

        // Read-only — no lifecycle/degraded check.
        let disk_groups: Vec<DiskGroupQueryEntry> = if let Some(did) = disk_id {
            // Disk-level or zone-level shape.
            let Some(dg) = self.container.get_disk_group(disk_group_id) else {
                submit_error(
                    server,
                    req.conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    FBDiskdbRetCode::DiskGroupNotFound,
                    "disk-group not owned",
                );
                return;
            };
            let disk = {
                let disks = dg.disks.read().unwrap();
                disks.iter().find(|d| d.disk_id == did).cloned()
            };
            let Some(disk) = disk else {
                submit_error(
                    server,
                    req.conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    FBDiskdbRetCode::DiskNotFound,
                    "disk not found in group",
                );
                return;
            };
            if has_zone {
                // Zone-level: attach usage_bitmap.
                let zi = zone_index;
                let Some(zu) = dg.zone_usage(did, zi) else {
                    submit_error(
                        server,
                        req.conn_handle,
                        req_id,
                        create_nano,
                        msg_type,
                        FBDiskdbRetCode::NotFound,
                        "zone out of range",
                    );
                    return;
                };
                let _ = zu; // zone-level bitmap handled in builder
                let agg = dg.aggregate_usage();
                let ctrl = build_query_capacity_response_zone(req_id, create_nano, &dg, &agg, &disk, zi);
                request.mark_success();
                submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
                return;
            }
            // Disk-level.
            let agg = dg.aggregate_usage();
            vec![(dg, agg, vec![did], vec![disk])]
        } else {
            // Disk-group level (or all owned if disk_group_id == 0).
            let dg_ids: Vec<u64> = if disk_group_id == 0 {
                self.container.disk_group_ids()
            } else {
                if self.container.get_disk_group(disk_group_id).is_none() {
                    submit_error(
                        server,
                        req.conn_handle,
                        req_id,
                        create_nano,
                        msg_type,
                        FBDiskdbRetCode::DiskGroupNotFound,
                        "disk-group not owned",
                    );
                    return;
                }
                vec![disk_group_id]
            };
            let mut out = Vec::with_capacity(dg_ids.len());
            for dg_id in dg_ids {
                if let Some(dg) = self.container.get_disk_group(dg_id) {
                    let usage = dg.aggregate_usage();
                    let disk_ids: Vec<DiskId> = {
                        let disks = dg.disks.read().unwrap();
                        disks.iter().map(|d| d.disk_id).collect()
                    };
                    let disks: Vec<Arc<DdbDisk>> = {
                        let disks = dg.disks.read().unwrap();
                        disks.iter().cloned().collect()
                    };
                    out.push((dg, usage, disk_ids, disks));
                }
            }
            out
        };

        let ctrl =
            build_query_capacity_response(req_id, create_nano, &disk_groups, disk_id.is_some() && !has_zone);
        request.mark_success();
        submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
    }

    // ── GetDiskGroupInfo ─────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_get_disk_group_info(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EGetDiskGroupInfoResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBGetDiskGroupInfoRequest>(req.control()) else {
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

        let dg_id = fb_req.disk_group_id();
        let Some(dg) = self.container.get_disk_group(dg_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::DiskGroupNotFound,
                "disk-group not owned",
            );
            return;
        };
        let usage = dg.aggregate_usage();
        let disk_ids: Vec<DiskId> = {
            let disks = dg.disks.read().unwrap();
            disks.iter().map(|d| d.disk_id).collect()
        };
        let ctrl = build_get_disk_group_info_response(
            req_id,
            create_nano,
            FBDiskdbRetCode::Success,
            None,
            &dg,
            &usage,
            &disk_ids,
            &[],
        );
        request.mark_success();
        submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
    }

    // ── GetDiskInfo ──────────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_get_disk_info(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EGetDiskInfoResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBGetDiskInfoRequest>(req.control()) else {
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

        let dg_id = fb_req.disk_group_id();
        let Some(dg) = self.container.get_disk_group(dg_id) else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::DiskGroupNotFound,
                "disk-group not owned",
            );
            return;
        };
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
        let disk = {
            let disks = dg.disks.read().unwrap();
            disks.iter().find(|d| d.disk_id == disk_id).cloned()
        };
        let Some(disk) = disk else {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::DiskNotFound,
                "disk not found",
            );
            return;
        };
        let ctrl =
            build_get_disk_info_response(req_id, create_nano, FBDiskdbRetCode::Success, None, &disk, false);
        request.mark_success();
        submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
    }
}
