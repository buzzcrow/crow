use super::{
    alloc, build_allocate_response, build_commit_response, build_free_response, elapsed_ns, map_free_error,
    mutation_gate, parse_segments, submit_error, submit_fb_response, AllocError, AllocateParams, Arc,
    ChunkId, DiskId, DiskdbRpcService, FBAllocateBlocksRequest, FBCommitBlocksRequest, FBDiskdbRetCode,
    FBFreeBlocksRequest, FBMsgType, RpcServer, ServerRequest, MAX_ALLOCATE_COUNT,
};

impl DiskdbRpcService {
    // ── AllocateBlocks ───────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_allocate(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAllocateBlocksResponse.0 as u16;

        let params = match self.validate_allocate(&req) {
            Ok(p) => p,
            Err((code, msg)) => {
                submit_error(server, req.conn_handle, req_id, create_nano, msg_type, code, msg);
                return;
            }
        };

        let kv = Arc::clone(&self.kv);
        let metrics = Arc::clone(&self.metrics);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let rpc_start = std::time::Instant::now();
            let result = alloc::allocate_blocks(
                &params.dg,
                params.unit_count,
                params.count,
                &params.exclude_disks,
                &params.owner_chunk,
                params.unit_size,
                &kv,
                params.cas_retry_limit,
                params.zone_rotate_count,
                &metrics,
            )
            .await;
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            match result {
                Ok(segments) => {
                    metrics.allocate_total.inc();
                    metrics.allocate_rpc_latency.observe(elapsed_ns(rpc_start));
                    let ctrl = build_allocate_response(
                        req_id,
                        create_nano,
                        FBDiskdbRetCode::Success,
                        None,
                        &segments,
                    );
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
                Err(AllocError::NoSpace) => {
                    let ctrl = build_allocate_response(
                        req_id,
                        create_nano,
                        FBDiskdbRetCode::NoSpace,
                        Some("no space available"),
                        &[],
                    );
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
                Err(AllocError::Persistence) => {
                    let ctrl = build_allocate_response(
                        req_id,
                        create_nano,
                        FBDiskdbRetCode::Internal,
                        Some("allocation persistence failed"),
                        &[],
                    );
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
            }
        });
    }

    pub(super) fn validate_allocate(
        &self,
        req: &ServerRequest,
    ) -> Result<AllocateParams, (FBDiskdbRetCode, &'static str)> {
        let Ok(fb_req) = flatbuffers::root::<FBAllocateBlocksRequest>(req.control()) else {
            return Err((FBDiskdbRetCode::InvalidArgument, "invalid request flatbuffer"));
        };

        let unit_count = fb_req.unit_count();
        let count = fb_req.count();
        if unit_count == 0 {
            return Err((FBDiskdbRetCode::InvalidArgument, "unit_count must be non-zero"));
        }
        if count == 0 {
            return Err((FBDiskdbRetCode::InvalidArgument, "count must be non-zero"));
        }
        if count > MAX_ALLOCATE_COUNT {
            return Err((FBDiskdbRetCode::InvalidArgument, "count exceeds maximum"));
        }

        mutation_gate::validate(&self.container)?;

        let disk_group_id = fb_req.disk_group_id();
        let dg = self
            .container
            .get_disk_group(disk_group_id)
            .ok_or((FBDiskdbRetCode::NotOwner, "disk-group not owned"))?;

        let fb_owner = fb_req
            .owner_chunk()
            .ok_or((FBDiskdbRetCode::InvalidArgument, "owner_chunk required"))?;
        let owner_chunk = ChunkId {
            high: fb_owner.high(),
            low: fb_owner.low(),
        };

        let exclude_disks: Vec<DiskId> = fb_req
            .exclude_disk_ids()
            .map(|v| {
                v.iter()
                    .map(|id| DiskId {
                        high: id.high(),
                        low: id.low(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(AllocateParams {
            dg,
            unit_count,
            count,
            exclude_disks,
            owner_chunk,
            unit_size: self.storage.block_size_bytes,
            cas_retry_limit: self.storage.cas_retry_limit,
            zone_rotate_count: self.storage.zone_rotate_count,
        })
    }

    // ── FreeBlocks ───────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_free(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EFreeBlocksResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBFreeBlocksRequest>(req.control()) else {
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

        let segments = match parse_segments(fb_req.segments()) {
            Some(s) if !s.is_empty() => s,
            _ => {
                let ctrl = build_free_response(req_id, create_nano, FBDiskdbRetCode::Success, None, 0);
                submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
                return;
            }
        };

        if let Err((code, message)) = mutation_gate::validate(&self.container) {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                code,
                message,
            );
            return;
        }

        let first_disk_id = segments[0].disk_id.unwrap_or_default();
        let Some(dg) = self.find_disk_group_for_disk(&first_disk_id) else {
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

        // Validate all segments belong to the resolved disk-group.
        let group_disk_ids: std::collections::HashSet<DiskId> = {
            let disks = dg.disks.read().unwrap();
            disks.iter().map(|d| d.disk_id).collect()
        };
        for seg in &segments {
            if let Some(did) = seg.disk_id {
                if !group_disk_ids.contains(&did) {
                    submit_error(
                        server,
                        req.conn_handle,
                        req_id,
                        create_nano,
                        msg_type,
                        FBDiskdbRetCode::InvalidArgument,
                        "segment disk not in resolved disk-group",
                    );
                    return;
                }
            }
        }

        let kv = Arc::clone(&self.kv);
        let metrics = Arc::clone(&self.metrics);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        #[allow(clippy::cast_possible_truncation)]
        let freed_count = segments.len() as u32;
        self.rt.spawn(async move {
            let rpc_start = std::time::Instant::now();
            let result = alloc::free_blocks(&dg, &segments, &kv).await;
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            match result {
                Ok(()) => {
                    metrics.free_total.inc();
                    metrics.free_rpc_latency.observe(elapsed_ns(rpc_start));
                    let ctrl =
                        build_free_response(req_id, create_nano, FBDiskdbRetCode::Success, None, freed_count);
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
                Err(e) => {
                    let (code, msg) = map_free_error(&e);
                    let ctrl = build_free_response(req_id, create_nano, code, Some(&msg), 0);
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
            }
        });
    }

    // ── CommitBlocks ─────────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_commit(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ECommitBlocksResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBCommitBlocksRequest>(req.control()) else {
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

        let segments = match parse_segments(fb_req.segments()) {
            Some(s) if !s.is_empty() => s,
            _ => {
                let ctrl = build_commit_response(req_id, create_nano, FBDiskdbRetCode::Success, None, 0);
                submit_fb_response(server, req.conn_handle, ctrl, msg_type, req_id);
                return;
            }
        };

        if let Err((code, message)) = mutation_gate::validate(&self.container) {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                code,
                message,
            );
            return;
        }

        let first_disk_id = segments[0].disk_id.unwrap_or_default();
        let Some(dg) = self.find_disk_group_for_disk(&first_disk_id) else {
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

        let kv = Arc::clone(&self.kv);
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let result = alloc::commit_blocks(&dg, &segments, &kv).await;
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            match result {
                Ok(committed) => {
                    let ctrl =
                        build_commit_response(req_id, create_nano, FBDiskdbRetCode::Success, None, committed);
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
                Err(e) => {
                    let (code, msg) = map_free_error(&e);
                    let ctrl = build_commit_response(req_id, create_nano, code, Some(&msg), 0);
                    submit_fb_response(&server, conn_handle, ctrl, msg_type, req_id);
                }
            }
        });
    }
}
