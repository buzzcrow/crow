use super::{
    Arc, Buffer, ChunkId, DdbDisk, DdbDiskGroup, DdbZoneHealth, DiskGroupQueryEntry, DiskGroupRecalcResult,
    DiskGroupUsage, DiskId, FBAllocateResponse, FBAllocateResponseArgs, FBCommitBlocksResponse,
    FBCommitBlocksResponseArgs, FBCompactZoneResponse, FBCompactZoneResponseArgs, FBDiskGroupInfo,
    FBDiskGroupInfoArgs, FBDiskGroupRecalcResult, FBDiskGroupRecalcResultArgs, FBDiskInfo, FBDiskInfoArgs,
    FBDiskType, FBDiskdbRetCode, FBFreeResponse, FBFreeResponseArgs, FBGetDiskGroupInfoResponse,
    FBGetDiskGroupInfoResponseArgs, FBGetDiskInfoResponse, FBGetDiskInfoResponseArgs,
    FBGetScanStatusResponse, FBGetScanStatusResponseArgs, FBHwStatus, FBInt128, FBMsgType,
    FBQueryCapacityStatsResponse, FBQueryCapacityStatsResponseArgs, FBRebuildZoneBitmapResponse,
    FBRebuildZoneBitmapResponseArgs, FBRecalcDiskUsageResponse, FBRecalcDiskUsageResponseArgs, FBScanSummary,
    FBScanSummaryArgs, FBSegment, FBTriggerScanResponse, FBTriggerScanResponseArgs, FBZoneAllocationState,
    FBZoneCompactionResult, FBZoneCompactionResultArgs, FBZoneRecalcResult, FBZoneRecalcResultArgs,
    FBZoneUsage, FBZoneUsageArgs, FlatBufferBuilder, FreeError, HwStatus, RpcServer, ScanSummary, Segment,
    ZoneUsage,
};

/// Validated parameters for `alloc::allocate_blocks`.
pub(super) struct AllocateParams {
    pub(super) dg: Arc<DdbDiskGroup>,
    pub(super) unit_count: u32,
    pub(super) count: u32,
    pub(super) exclude_disks: Vec<DiskId>,
    pub(super) owner_chunk: ChunkId,
    pub(super) unit_size: u32,
    pub(super) cas_retry_limit: u32,
    pub(super) zone_rotate_count: u32,
}

/// Parse a flatbuffer `FBSegment` vector into proto `Segment`s.
pub(super) fn parse_segments<'a, V: IntoIterator<Item = &'a FBSegment> + Clone>(
    fb_segs: Option<V>,
) -> Option<Vec<Segment>> {
    let vec = fb_segs?;
    let mut out = Vec::with_capacity(vec.clone().into_iter().count());
    for s in vec {
        out.push(Segment {
            disk_id: Some(DiskId {
                high: s.disk_id().high(),
                low: s.disk_id().low(),
            }),
            owner_chunk: Some(ChunkId {
                high: s.owner_chunk().high(),
                low: s.owner_chunk().low(),
            }),
            unit_offset: s.unit_offset(),
            zone_index: s.zone_index(),
            unit_count: s.unit_count(),
            allocation_ts: s.allocation_ts(),
        });
    }
    Some(out)
}

/// Map a `FreeError` to a `(ret_code, message)` pair.
pub(super) fn map_free_error(e: &FreeError) -> (FBDiskdbRetCode, String) {
    match e {
        FreeError::NotBusy { .. } => (FBDiskdbRetCode::NotFound, format!("free failed: {e}")),
        FreeError::Kv(_) => (FBDiskdbRetCode::Internal, format!("free persist failed: {e}")),
    }
}

/// Submit a flatbuffer response via the zero-copy `submit_response_buffer`
/// path. Takes ownership of `ctrl` (moved). If the buffer is empty (null
/// handle), falls back to `submit_response` with an empty control slice.
pub(super) fn submit_fb_response(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    ctrl: (Vec<u8>, usize),
    msg_type: u16,
    req_id: u64,
) {
    let buf = Buffer::from_vec_offset(ctrl.0, ctrl.1);
    if buf.is_null_handle() {
        unsafe {
            let _ = server.submit_response(conn_handle, &[], None, msg_type, req_id);
        }
        return;
    }
    unsafe {
        let _ = server.submit_response_buffer(conn_handle, buf, None, msg_type, req_id);
    }
}

/// Submit an error response (synchronous, from the dispatch thread).
pub(super) fn submit_error(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    ret_code: FBDiskdbRetCode,
    msg: &str,
) {
    let ctrl = build_error_response(req_id, create_nano, ret_code, Some(msg), msg_type);
    submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
}

// ── Response builders ────────────────────────────────────────────

/// Build a generic error response. The response table is selected by
/// `msg_type` — all diskdb response tables share the same first 4
/// fields (`id`, `rpc_create_nano`, `ret_code`, `error_msg`), so we
/// build the matching table for the given `msg_type`.
pub(super) fn build_error_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    msg_type: u16,
) -> (Vec<u8>, usize) {
    // Dispatch on msg_type to build the right response table. All
    // response tables have the same first 4 fields, so we use the
    // allocate response as a generic carrier for error-only responses
    // when the specific type isn't critical (the client parses by
    // msg_type and reads ret_code + error_msg).
    match msg_type {
        mt if mt == FBMsgType::EAllocateBlocksResponse.0 as u16 => {
            build_allocate_response(req_id, create_nano, ret_code, error_msg, &[])
        }
        mt if mt == FBMsgType::EFreeBlocksResponse.0 as u16 => {
            build_free_response(req_id, create_nano, ret_code, error_msg, 0)
        }
        mt if mt == FBMsgType::ECommitBlocksResponse.0 as u16 => {
            build_commit_response(req_id, create_nano, ret_code, error_msg, 0)
        }
        _ => {
            // Fallback: build an allocate response (generic shape).
            build_allocate_response(req_id, create_nano, ret_code, error_msg, &[])
        }
    }
}

pub(super) fn build_allocate_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    segments: &[Segment],
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let seg_vec: Vec<FBSegment> = segments
        .iter()
        .map(|s| {
            let disk_id = s.disk_id.unwrap_or_default();
            let owner = s.owner_chunk.unwrap_or_default();
            FBSegment::new(
                &FBInt128::new(disk_id.high, disk_id.low),
                &FBInt128::new(owner.high, owner.low),
                s.unit_offset,
                s.allocation_ts,
                s.zone_index,
                s.unit_count,
            )
        })
        .collect();
    let seg_off = fbb.create_vector(&seg_vec);
    let off = FBAllocateResponse::create(
        &mut fbb,
        &FBAllocateResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            segments: Some(seg_off),
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_free_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    freed_count: u32,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let off = FBFreeResponse::create(
        &mut fbb,
        &FBFreeResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            freed_count,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_commit_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    committed_count: u32,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let off = FBCommitBlocksResponse::create(
        &mut fbb,
        &FBCommitBlocksResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            committed_count,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_query_capacity_response(
    req_id: u64,
    create_nano: u64,
    disk_groups: &[DiskGroupQueryEntry],
    include_zones: bool,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let dg_offs: Vec<flatbuffers::WIPOffset<FBDiskGroupInfo>> = disk_groups
        .iter()
        .map(|(dg, usage, disk_ids, disks)| {
            build_disk_group_info_offset(&mut fbb, dg, usage, disk_ids, disks, include_zones)
        })
        .collect();
    let dg_vec = fbb.create_vector(&dg_offs);
    let off = FBQueryCapacityStatsResponse::create(
        &mut fbb,
        &FBQueryCapacityStatsResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: FBDiskdbRetCode::Success,
            error_msg: None,
            disk_groups: Some(dg_vec),
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_query_capacity_response_zone(
    req_id: u64,
    create_nano: u64,
    dg: &Arc<DdbDiskGroup>,
    usage: &DiskGroupUsage,
    disk: &Arc<DdbDisk>,
    zone_index: u32,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    // Build the disk info with the single zone + bitmap.
    let zu = dg.zone_usage(disk.disk_id, zone_index).unwrap_or(ZoneUsage {
        zone_index,
        capacity_bytes: 0,
        busy_bytes: 0,
        free_bytes: 0,
        busy_block_count: 0,
        free_block_count: 0,
        alloc_state: crowdb_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocActive,
        zone_state: DdbZoneHealth::Healthy,
    });
    let bitmap_bytes = {
        let disk_opt = dg.get_disk(disk.disk_id);
        disk_opt.and_then(|d| {
            let zones = d.zones.load();
            let zi = zone_index as usize;
            (zi < zones.len()).then(|| zones[zi].usage_bits.snapshot())
        })
    };
    let zone_off = build_zone_usage_offset(&mut fbb, &zu, bitmap_bytes.as_deref());
    let disk_off = build_disk_info_offset(&mut fbb, disk, false, Some(zone_off));
    let disk_id_off = fbb.create_vector(&[FBInt128::new(disk.disk_id.high, disk.disk_id.low)]);
    let disk_vec_off = fbb.create_vector(&[disk_off]);
    let status = dg.status();
    let dg_off = FBDiskGroupInfo::create(
        &mut fbb,
        &FBDiskGroupInfoArgs {
            rack_id: dg.rack_id,
            node_id: dg.node_id,
            disk_group_id: dg.disk_group_id,
            status: hw_status_to_fb(status.into()),
            disk_ids: Some(disk_id_off),
            disks: Some(disk_vec_off),
            capacity_bytes: usage.capacity_bytes,
            busy_bytes: usage.busy_bytes,
            free_bytes: usage.free_bytes,
            allocatable_disk_count: usage.allocatable_disk_count,
        },
    );
    let dg_vec = fbb.create_vector(&[dg_off]);
    let off = FBQueryCapacityStatsResponse::create(
        &mut fbb,
        &FBQueryCapacityStatsResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: FBDiskdbRetCode::Success,
            error_msg: None,
            disk_groups: Some(dg_vec),
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_get_disk_group_info_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    dg: &Arc<DdbDiskGroup>,
    usage: &DiskGroupUsage,
    disk_ids: &[DiskId],
    disks: &[Arc<DdbDisk>],
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let dg_off = if ret_code == FBDiskdbRetCode::Success {
        Some(build_disk_group_info_offset(
            &mut fbb, dg, usage, disk_ids, disks, false,
        ))
    } else {
        None
    };
    let off = FBGetDiskGroupInfoResponse::create(
        &mut fbb,
        &FBGetDiskGroupInfoResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            group: dg_off,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_get_disk_info_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    disk: &Arc<DdbDisk>,
    include_zones: bool,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let disk_off = if ret_code == FBDiskdbRetCode::Success {
        Some(build_disk_info_offset(&mut fbb, disk, include_zones, None))
    } else {
        None
    };
    let off = FBGetDiskInfoResponse::create(
        &mut fbb,
        &FBGetDiskInfoResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            disk: disk_off,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_rebuild_zone_bitmap_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    rebuilt_zone_count: u32,
    total_busy_units: u64,
    total_free_units: u64,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let off = FBRebuildZoneBitmapResponse::create(
        &mut fbb,
        &FBRebuildZoneBitmapResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            rebuilt_zone_count,
            total_busy_units,
            total_free_units,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_recalc_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    results: &[DiskGroupRecalcResult],
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let result_offs: Vec<flatbuffers::WIPOffset<FBDiskGroupRecalcResult>> = results
        .iter()
        .map(|dg_r| {
            let zone_offs: Vec<flatbuffers::WIPOffset<FBZoneRecalcResult>> = dg_r
                .zone_results
                .iter()
                .map(|zr| {
                    let fb_off = zr.fallback_reason_str().map(|s| fbb.create_string(s));
                    FBZoneRecalcResult::create(
                        &mut fbb,
                        &FBZoneRecalcResultArgs {
                            disk_id: Some(&FBInt128::new(zr.disk_id.high, zr.disk_id.low)),
                            zone_index: zr.zone_index,
                            matches: zr.matches,
                            drift_detected: zr.drift_detected,
                            live_busy_blocks: zr.live_busy_blocks,
                            replayed_busy_blocks: zr.replayed_busy_blocks,
                            live_snapshot_slot: zr.live_snapshot_slot,
                            replayed_snapshot_slot: zr.replayed_snapshot_slot,
                            fallback_reason: fb_off,
                        },
                    )
                })
                .collect();
            let zone_vec = fbb.create_vector(&zone_offs);
            FBDiskGroupRecalcResult::create(
                &mut fbb,
                &FBDiskGroupRecalcResultArgs {
                    disk_group_id: dg_r.disk_group_id,
                    drift_detected: dg_r.drift_detected,
                    zones: Some(zone_vec),
                },
            )
        })
        .collect();
    let results_vec = fbb.create_vector(&result_offs);
    let off = FBRecalcDiskUsageResponse::create(
        &mut fbb,
        &FBRecalcDiskUsageResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            results: Some(results_vec),
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_compact_zone_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    compacted_zone_count: u32,
    total_free_records_deleted: u32,
    zones: &[(u32, bool, u32, Option<String>)],
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let zone_offs: Vec<flatbuffers::WIPOffset<FBZoneCompactionResult>> = zones
        .iter()
        .map(|(zi, success, deleted, err)| {
            let err_off = err.as_ref().map(|e| fbb.create_string(e));
            FBZoneCompactionResult::create(
                &mut fbb,
                &FBZoneCompactionResultArgs {
                    zone_index: *zi,
                    success: *success,
                    free_records_deleted: *deleted,
                    error: err_off,
                },
            )
        })
        .collect();
    let zone_vec = fbb.create_vector(&zone_offs);
    let off = FBCompactZoneResponse::create(
        &mut fbb,
        &FBCompactZoneResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            compacted_zone_count,
            total_free_records_deleted,
            zones: Some(zone_vec),
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_trigger_scan_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    summary: &ScanSummary,
    scan_in_progress: bool,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let leak_off = fbb.create_string(&summary.leak_status);
    let summary_off = FBScanSummary::create(
        &mut fbb,
        &FBScanSummaryArgs {
            started_at_ms: summary.started_at_ms,
            duration_ms: summary.duration_ms,
            zones_scanned: summary.zones_scanned,
            zones_skipped_active: summary.zones_skipped_active,
            zones_skipped_compacting: summary.zones_skipped_compacting,
            ghost_busy: summary.ghost_busy,
            ghost_free: summary.ghost_free,
            uncompacted_lag: summary.uncompacted_lag,
            corrupt_snapshots: summary.corrupt_snapshots,
            corrupt_records: summary.corrupt_records,
            owner_mismatches: summary.owner_mismatches,
            leak_status: Some(leak_off),
        },
    );
    let off = FBTriggerScanResponse::create(
        &mut fbb,
        &FBTriggerScanResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            summary: Some(summary_off),
            scan_in_progress,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_get_scan_status_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    summary: &ScanSummary,
    has_run: bool,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let leak_off = fbb.create_string(&summary.leak_status);
    let summary_off = FBScanSummary::create(
        &mut fbb,
        &FBScanSummaryArgs {
            started_at_ms: summary.started_at_ms,
            duration_ms: summary.duration_ms,
            zones_scanned: summary.zones_scanned,
            zones_skipped_active: summary.zones_skipped_active,
            zones_skipped_compacting: summary.zones_skipped_compacting,
            ghost_busy: summary.ghost_busy,
            ghost_free: summary.ghost_free,
            uncompacted_lag: summary.uncompacted_lag,
            corrupt_snapshots: summary.corrupt_snapshots,
            corrupt_records: summary.corrupt_records,
            owner_mismatches: summary.owner_mismatches,
            leak_status: Some(leak_off),
        },
    );
    let off = FBGetScanStatusResponse::create(
        &mut fbb,
        &FBGetScanStatusResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            summary: Some(summary_off),
            has_run,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

// ── Flatbuffer offset builders (shared) ──────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn build_disk_group_info_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    dg: &Arc<DdbDiskGroup>,
    usage: &DiskGroupUsage,
    disk_ids: &[DiskId],
    disks: &[Arc<DdbDisk>],
    include_zones: bool,
) -> flatbuffers::WIPOffset<FBDiskGroupInfo<'a>> {
    let disk_id_offs: Vec<FBInt128> = disk_ids.iter().map(|id| FBInt128::new(id.high, id.low)).collect();
    let disk_id_vec = fbb.create_vector(&disk_id_offs);
    let disk_offs: Vec<flatbuffers::WIPOffset<FBDiskInfo>> = disks
        .iter()
        .map(|d| build_disk_info_offset(fbb, d, include_zones, None))
        .collect();
    let disk_vec = fbb.create_vector(&disk_offs);
    let status = dg.status();
    FBDiskGroupInfo::create(
        fbb,
        &FBDiskGroupInfoArgs {
            rack_id: dg.rack_id,
            node_id: dg.node_id,
            disk_group_id: dg.disk_group_id,
            status: hw_status_to_fb(status.into()),
            disk_ids: Some(disk_id_vec),
            disks: Some(disk_vec),
            capacity_bytes: usage.capacity_bytes,
            busy_bytes: usage.busy_bytes,
            free_bytes: usage.free_bytes,
            allocatable_disk_count: usage.allocatable_disk_count,
        },
    )
}

pub(super) fn build_disk_info_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    disk: &Arc<DdbDisk>,
    include_zones: bool,
    single_zone: Option<flatbuffers::WIPOffset<FBZoneUsage<'a>>>,
) -> flatbuffers::WIPOffset<FBDiskInfo<'a>> {
    let dv = &disk.disk_value;
    let usage = disk.usage();
    let zone_usages = if let Some(zo) = single_zone {
        Some(fbb.create_vector(&[zo]))
    } else if include_zones {
        let zone_offs: Vec<flatbuffers::WIPOffset<FBZoneUsage>> = disk
            .zone_usages()
            .iter()
            .map(|z| build_zone_usage_offset(fbb, z, None))
            .collect();
        Some(fbb.create_vector(&zone_offs))
    } else {
        None
    };
    let device_path_off = fbb.create_string(&dv.device_path);
    let disk_id_off = FBInt128::new(disk.disk_id.high, disk.disk_id.low);
    FBDiskInfo::create(
        fbb,
        &FBDiskInfoArgs {
            rack_id: disk.rack_id,
            node_id: disk.node_id,
            disk_group_id: disk.disk_group_id,
            disk_id: Some(&disk_id_off),
            disk_type: disk_type_to_fb(dv.disk_type),
            capacity_units: dv.capacity_units,
            zone_size_units: dv.zone_size_units,
            unit_size_bytes: dv.unit_size_bytes,
            zone_count: dv.zone_count,
            status: hw_status_to_fb(dv.status),
            busy_units: usage.busy_bytes / u64::from(dv.unit_size_bytes).max(1),
            free_units: usage.free_bytes / u64::from(dv.unit_size_bytes).max(1),
            capacity_bytes: usage.capacity_bytes,
            busy_bytes: usage.busy_bytes,
            free_bytes: usage.free_bytes,
            active_zone_count: usage.active_zone_count,
            zone_usages,
            device_path: Some(device_path_off),
        },
    )
}

pub(super) fn build_zone_usage_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    zu: &ZoneUsage,
    bitmap: Option<&[u8]>,
) -> flatbuffers::WIPOffset<FBZoneUsage<'a>> {
    let bitmap_off = bitmap.map(|b| fbb.create_vector(b));
    FBZoneUsage::create(
        fbb,
        &FBZoneUsageArgs {
            zone_index: zu.zone_index,
            capacity_bytes: zu.capacity_bytes,
            busy_bytes: zu.busy_bytes,
            free_bytes: zu.free_bytes,
            busy_block_count: zu.busy_block_count,
            free_block_count: zu.free_block_count,
            alloc_state: zone_alloc_state_to_fb(zu.alloc_state as i32),
            usage_bitmap: bitmap_off,
        },
    )
}

// ── Enum conversions ─────────────────────────────────────────────

pub(super) fn hw_status_to_fb(v: i32) -> FBHwStatus {
    match v {
        x if x == HwStatus::Init as i32 => FBHwStatus::Init,
        x if x == HwStatus::Up as i32 => FBHwStatus::Up,
        x if x == HwStatus::Maintenance as i32 => FBHwStatus::Maintenance,
        x if x == HwStatus::Suspect as i32 => FBHwStatus::Suspect,
        x if x == HwStatus::Missing as i32 => FBHwStatus::Missing,
        x if x == HwStatus::Bad as i32 => FBHwStatus::Bad,
        x if x == HwStatus::Offline as i32 => FBHwStatus::Offline,
        _ => FBHwStatus::Init,
    }
}

pub(super) fn disk_type_to_fb(v: i32) -> FBDiskType {
    match v {
        x if x == crowdb_protocol::diskdb::rpc::DiskType::BlockHdd as i32 => FBDiskType::BlockHdd,
        x if x == crowdb_protocol::diskdb::rpc::DiskType::BlockSsd as i32 => FBDiskType::BlockSsd,
        x if x == crowdb_protocol::diskdb::rpc::DiskType::ZoneSsd as i32 => FBDiskType::ZoneSsd,
        x if x == crowdb_protocol::diskdb::rpc::DiskType::SmrHdd as i32 => FBDiskType::SmrHdd,
        _ => FBDiskType::BlockHdd,
    }
}

pub(super) fn zone_alloc_state_to_fb(v: i32) -> FBZoneAllocationState {
    match v {
        x if x == crowdb_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocActive as i32 => {
            FBZoneAllocationState::Active
        }
        x if x == crowdb_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocAvailable as i32 => {
            FBZoneAllocationState::Available
        }
        x if x == crowdb_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocFull as i32 => {
            FBZoneAllocationState::Full
        }
        _ => FBZoneAllocationState::Active,
    }
}
