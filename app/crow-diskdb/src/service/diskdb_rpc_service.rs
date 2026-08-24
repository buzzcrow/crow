// Copyright 2026-present buzzcrow <buzzcrow@126.com>.
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]

//! crow-rpc handler set for `DiskdbService` (R115 migration).
//!
//! Each handler dispatches by `msg_type` to the existing diskdb logic
//! (allocator, scanner, zone management) — the same logic bodies as the
//! tonic `DiskdbService` in `diskdb_service.rs`. The response is a
//! flatbuffer frame built per `design-crow-rpc.md` §6 (build → finish →
//! attach) and submitted via `RpcServer::submit_response`.
//!
//! Handlers run on the C++ I/O worker thread. Synchronous paths
//! (validation, in-memory allocator checks) run inline; async paths
//! (KV persist via `DdbKvClient`) spawn a tokio task via the captured
//! `Handle` and submit the response from the task. Each handler closure
//! captures an `Arc<RpcServer>` so it can submit responses from either
//! the dispatch thread (sync error path) or the spawned task (async
//! success path).

use std::sync::Arc;

use crow_protocol::common::{ChunkId, DiskId, HwStatus};
use crow_protocol::diskdb::rpc::{DiskValue, Segment};
use crow_protocol::diskdb_fb::{
    FBAllocateBlocksRequest, FBAllocateResponse, FBAllocateResponseArgs, FBCommitBlocksRequest,
    FBCommitBlocksResponse, FBCommitBlocksResponseArgs, FBCompactZoneRequest, FBCompactZoneResponse,
    FBCompactZoneResponseArgs, FBDiskGroupInfo, FBDiskGroupInfoArgs, FBDiskGroupRecalcResult,
    FBDiskGroupRecalcResultArgs, FBDiskInfo, FBDiskInfoArgs, FBDiskType, FBDiskdbRetCode,
    FBFreeBlocksRequest, FBFreeResponse, FBFreeResponseArgs, FBGetDiskGroupInfoRequest,
    FBGetDiskGroupInfoResponse, FBGetDiskGroupInfoResponseArgs, FBGetDiskInfoRequest, FBGetDiskInfoResponse,
    FBGetDiskInfoResponseArgs, FBGetScanStatusRequest, FBGetScanStatusResponse, FBGetScanStatusResponseArgs,
    FBHwStatus, FBInt128, FBQueryCapacityStatsRequest, FBQueryCapacityStatsResponse,
    FBQueryCapacityStatsResponseArgs, FBRebuildZoneBitmapRequest, FBRebuildZoneBitmapResponse,
    FBRebuildZoneBitmapResponseArgs, FBRecalcDiskUsageRequest, FBRecalcDiskUsageResponse,
    FBRecalcDiskUsageResponseArgs, FBScanSummary, FBScanSummaryArgs, FBSegment, FBTriggerScanRequest,
    FBTriggerScanResponse, FBTriggerScanResponseArgs, FBZoneAllocationState, FBZoneCompactionResult,
    FBZoneCompactionResultArgs, FBZoneRecalcResult, FBZoneRecalcResultArgs, FBZoneUsage, FBZoneUsageArgs,
};
use crow_protocol::fb::FBMsgType;
use crow_rpc_ffi::{RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;

use crate::ddb_config::StorageDefaults;
use crate::ddb_kv_client::DdbKvClient;
use crate::metrics::{DiskGroupRecalcResult, DiskdbMetrics, RecalcEngine};
use crate::model::alloc::{self, FreeError};
use crate::model::disk::DdbDisk;
use crate::model::disk_group::{AllocError, DdbDiskGroup, DiskGroupUsage};
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::{DdbZoneHealth, ZoneUsage};
use crate::recovery::compaction::compact_zone;
use crate::recovery::{unit_capacity_for_zone, ZoneLoader};
use crate::scanner::{ScanState, ScanSummary};

use super::diskdb_service::{elapsed_ns, ALL_ZONES, MAX_ALLOCATE_COUNT};

/// Disk-group + usage + `disk_ids` + disks tuple for query responses.
type DiskGroupQueryEntry = (Arc<DdbDiskGroup>, DiskGroupUsage, Vec<DiskId>, Vec<Arc<DdbDisk>>);

/// crow-rpc handler set for `DiskdbService`. Holds the same dependencies
/// as the tonic `DiskdbService`; `register_handlers` wires one handler
/// per request `msg_type` into a `RpcServer`.
pub struct DiskdbRpcService {
    container: Arc<DdbDiskGroupContainer>,
    kv: Arc<DdbKvClient>,
    storage: StorageDefaults,
    zone_loader: Arc<ZoneLoader>,
    recalc: Arc<RecalcEngine>,
    scan_state: ScanState,
    metrics: Arc<DiskdbMetrics>,
    /// Tokio runtime handle for spawning async work from the C++ I/O
    /// thread callback.
    rt: Handle,
}

impl DiskdbRpcService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        container: Arc<DdbDiskGroupContainer>,
        kv: Arc<DdbKvClient>,
        storage: StorageDefaults,
        zone_loader: Arc<ZoneLoader>,
        recalc: Arc<RecalcEngine>,
        scan_state: ScanState,
        metrics: Arc<DiskdbMetrics>,
        rt: Handle,
    ) -> Self {
        Self {
            container,
            kv,
            storage,
            zone_loader,
            recalc,
            scan_state,
            metrics,
            rt,
        }
    }

    /// Register all 11 diskdb request handlers into the `RpcServer`.
    pub fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EAllocateBlocksRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_allocate),
        );
        server.register_handler(
            FBMsgType::EFreeBlocksRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_free),
        );
        server.register_handler(
            FBMsgType::ECommitBlocksRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_commit),
        );
        server.register_handler(
            FBMsgType::EQueryCapacityStatsRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_query_capacity),
        );
        server.register_handler(
            FBMsgType::EGetDiskGroupInfoRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                Self::handle_get_disk_group_info,
            ),
        );
        server.register_handler(
            FBMsgType::EGetDiskInfoRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_get_disk_info),
        );
        server.register_handler(
            FBMsgType::ERebuildZoneBitmapRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                Self::handle_rebuild_zone_bitmap,
            ),
        );
        server.register_handler(
            FBMsgType::ERecalcDiskUsageRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                Self::handle_recalc_disk_usage,
            ),
        );
        server.register_handler(
            FBMsgType::ECompactZoneRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_compact_zone),
        );
        server.register_handler(
            FBMsgType::ETriggerScanRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_trigger_scan),
        );
        server.register_handler(
            FBMsgType::EGetScanStatusRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_get_scan_status),
        );
    }

    /// Build a handler closure that dispatches to the given method.
    fn make_handler(
        this: Arc<Self>,
        server: Arc<RpcServer>,
        f: fn(&Self, &ServerRequest<'_>, &Arc<RpcServer>),
    ) -> impl Fn(ServerRequest<'_>) + Send + 'static {
        move |req| {
            f(&this, &req, &server);
        }
    }

    // ── AllocateBlocks ───────────────────────────────────────────

    fn handle_allocate(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAllocateBlocksResponse.0 as u16;

        let params = match self.validate_allocate(req) {
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
                    unsafe {
                        if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                            tracing::warn!(?e, "allocate_blocks crow-rpc submit failed");
                        }
                    }
                }
                Err(AllocError::NoSpace) => {
                    let ctrl = build_allocate_response(
                        req_id,
                        create_nano,
                        FBDiskdbRetCode::NoSpace,
                        Some("no space available"),
                        &[],
                    );
                    unsafe {
                        if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                            tracing::warn!(?e, "allocate_blocks NoSpace submit failed");
                        }
                    }
                }
            }
        });
    }

    fn validate_allocate(
        &self,
        req: &ServerRequest<'_>,
    ) -> Result<AllocateParams, (FBDiskdbRetCode, &'static str)> {
        let Ok(fb_req) = flatbuffers::root::<FBAllocateBlocksRequest>(req.control) else {
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

        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            return Err((FBDiskdbRetCode::Unavailable, "diskdb not ready"));
        }
        if self.container.is_degraded() {
            return Err((FBDiskdbRetCode::Degraded, "diskdb in degraded mode"));
        }

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
    fn handle_free(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EFreeBlocksResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBFreeBlocksRequest>(req.control) else {
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
                unsafe {
                    let _ = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id);
                }
                return;
            }
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
        if self.container.is_degraded() {
            submit_error(
                server,
                req.conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBDiskdbRetCode::Degraded,
                "diskdb in degraded mode",
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
        let validate_owner = self.storage.validate_owner_on_free;
        let conn_handle_usize = req.conn_handle as usize;
        let server = Arc::clone(server);
        #[allow(clippy::cast_possible_truncation)]
        let freed_count = segments.len() as u32;
        self.rt.spawn(async move {
            let rpc_start = std::time::Instant::now();
            let result = alloc::free_blocks(&dg, &segments, &kv, validate_owner).await;
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            match result {
                Ok(()) => {
                    metrics.free_total.inc();
                    metrics.free_rpc_latency.observe(elapsed_ns(rpc_start));
                    let ctrl =
                        build_free_response(req_id, create_nano, FBDiskdbRetCode::Success, None, freed_count);
                    unsafe {
                        if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                            tracing::warn!(?e, "free_blocks crow-rpc submit failed");
                        }
                    }
                }
                Err(e) => {
                    let (code, msg) = map_free_error(&e);
                    let ctrl = build_free_response(req_id, create_nano, code, Some(&msg), 0);
                    unsafe {
                        if let Err(e2) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                            tracing::warn!(?e2, "free_blocks error submit failed");
                        }
                    }
                }
            }
        });
    }

    // ── CommitBlocks ─────────────────────────────────────────────

    fn handle_commit(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ECommitBlocksResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBCommitBlocksRequest>(req.control) else {
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
                unsafe {
                    let _ = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id);
                }
                return;
            }
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
                    unsafe {
                        if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                            tracing::warn!(?e, "commit_blocks crow-rpc submit failed");
                        }
                    }
                }
                Err(e) => {
                    let (code, msg) = map_free_error(&e);
                    let ctrl = build_commit_response(req_id, create_nano, code, Some(&msg), 0);
                    unsafe {
                        if let Err(e2) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                            tracing::warn!(?e2, "commit_blocks error submit failed");
                        }
                    }
                }
            }
        });
    }

    // ── QueryCapacityStats ───────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_query_capacity(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EQueryCapacityStatsResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBQueryCapacityStatsRequest>(req.control) else {
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
                unsafe {
                    if let Err(e) = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id) {
                        tracing::warn!(?e, "query_capacity zone submit failed");
                    }
                }
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
        unsafe {
            if let Err(e) = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id) {
                tracing::warn!(?e, "query_capacity submit failed");
            }
        }
    }

    // ── GetDiskGroupInfo ─────────────────────────────────────────

    fn handle_get_disk_group_info(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EGetDiskGroupInfoResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBGetDiskGroupInfoRequest>(req.control) else {
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
        unsafe {
            if let Err(e) = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id) {
                tracing::warn!(?e, "get_disk_group_info submit failed");
            }
        }
    }

    // ── GetDiskInfo ──────────────────────────────────────────────

    fn handle_get_disk_info(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EGetDiskInfoResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBGetDiskInfoRequest>(req.control) else {
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
        unsafe {
            if let Err(e) = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id) {
                tracing::warn!(?e, "get_disk_info submit failed");
            }
        }
    }

    // ── RebuildZoneBitmap ────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_rebuild_zone_bitmap(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ERebuildZoneBitmapResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBRebuildZoneBitmapRequest>(req.control) else {
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

        let bind = *dg.bind.read().unwrap();
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
                unsafe {
                    let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
                }
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
                unsafe {
                    if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                        tracing::warn!(?e, "rebuild_zone_bitmap submit failed");
                    }
                }
            }
        });
    }

    // ── RecalcDiskUsage ──────────────────────────────────────────

    fn handle_recalc_disk_usage(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ERecalcDiskUsageResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBRecalcDiskUsageRequest>(req.control) else {
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
                    unsafe {
                        let _ = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id);
                    }
                    return;
                }
            } else {
                recalc.recalc_all().await
            };
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            let ctrl = build_recalc_response(req_id, create_nano, FBDiskdbRetCode::Success, None, &results);
            unsafe {
                if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                    tracing::warn!(?e, "recalc_disk_usage submit failed");
                }
            }
        });
    }

    // ── CompactZone ──────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_compact_zone(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ECompactZoneResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBCompactZoneRequest>(req.control) else {
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

        let bind = *dg.bind.read().unwrap();
        let zone_count = disk.disk_value.read().unwrap().zone_count;
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
                    let zones = disk.zones.read().unwrap();
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
            unsafe {
                if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                    tracing::warn!(?e, "compact_zone submit failed");
                }
            }
        });
    }

    // ── TriggerScan ──────────────────────────────────────────────

    fn handle_trigger_scan(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ETriggerScanResponse.0 as u16;

        let Ok(_fb_req) = flatbuffers::root::<FBTriggerScanRequest>(req.control) else {
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
        unsafe {
            if let Err(e) = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id) {
                tracing::warn!(?e, "trigger_scan submit failed");
            }
        }
    }

    // ── GetScanStatus ────────────────────────────────────────────

    fn handle_get_scan_status(&self, req: &ServerRequest<'_>, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EGetScanStatusResponse.0 as u16;

        let Ok(_fb_req) = flatbuffers::root::<FBGetScanStatusRequest>(req.control) else {
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
        unsafe {
            if let Err(e) = server.submit_response(req.conn_handle, &ctrl, None, msg_type, req_id) {
                tracing::warn!(?e, "get_scan_status submit failed");
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────

    /// Find the disk-group that owns `disk_id`.
    fn find_disk_group_for_disk(&self, disk_id: &DiskId) -> Option<Arc<DdbDiskGroup>> {
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
    fn find_disk(&self, disk_id: &DiskId) -> Option<(Arc<DdbDiskGroup>, Arc<DdbDisk>)> {
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
    fn find_disk_value(&self, disk_id: &DiskId) -> Option<(Arc<DdbDiskGroup>, DiskValue, DiskId)> {
        let dg_ids = self.container.disk_group_ids();
        for dg_id in dg_ids {
            if let Some(n) = self.container.get_disk_group(dg_id) {
                let dv_clone = {
                    let disks = n.disks.read().unwrap();
                    disks
                        .iter()
                        .find(|d| &d.disk_id == disk_id)
                        .map(|d| (d.disk_value.read().unwrap().clone(), d.disk_id))
                };
                if let Some((dv, did)) = dv_clone {
                    return Some((n, dv, did));
                }
            }
        }
        None
    }
}

/// Validated parameters for `alloc::allocate_blocks`.
struct AllocateParams {
    dg: Arc<DdbDiskGroup>,
    unit_count: u32,
    count: u32,
    exclude_disks: Vec<DiskId>,
    owner_chunk: ChunkId,
    unit_size: u32,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
}

/// Parse a flatbuffer `FBSegment` vector into proto `Segment`s.
fn parse_segments<'a, V: IntoIterator<Item = &'a FBSegment> + Clone>(
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
        });
    }
    Some(out)
}

/// Map a `FreeError` to a `(ret_code, message)` pair.
fn map_free_error(e: &FreeError) -> (FBDiskdbRetCode, String) {
    match e {
        FreeError::NotBusy { .. } => (FBDiskdbRetCode::NotFound, format!("free failed: {e}")),
        FreeError::OwnerMismatch { .. } => (FBDiskdbRetCode::NotOwner, format!("free failed: {e}")),
        FreeError::Kv(_) => (FBDiskdbRetCode::Internal, format!("free persist failed: {e}")),
    }
}

/// Submit an error response (synchronous, from the dispatch thread).
fn submit_error(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    ret_code: FBDiskdbRetCode,
    msg: &str,
) {
    let ctrl = build_error_response(req_id, create_nano, ret_code, Some(msg), msg_type);
    unsafe {
        if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
            tracing::warn!(?e, "diskdb crow-rpc error submit failed");
        }
    }
}

// ── Response builders ────────────────────────────────────────────

/// Build a generic error response. The response table is selected by
/// `msg_type` — all diskdb response tables share the same first 4
/// fields (`id`, `rpc_create_nano`, `ret_code`, `error_msg`), so we
/// build the matching table for the given `msg_type`.
fn build_error_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    msg_type: u16,
) -> Vec<u8> {
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

fn build_allocate_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    segments: &[Segment],
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

fn build_free_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    freed_count: u32,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

fn build_commit_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    committed_count: u32,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_query_capacity_response(
    req_id: u64,
    create_nano: u64,
    disk_groups: &[DiskGroupQueryEntry],
    include_zones: bool,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_query_capacity_response_zone(
    req_id: u64,
    create_nano: u64,
    dg: &Arc<DdbDiskGroup>,
    usage: &DiskGroupUsage,
    disk: &Arc<DdbDisk>,
    zone_index: u32,
) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    // Build the disk info with the single zone + bitmap.
    let zu = dg.zone_usage(disk.disk_id, zone_index).unwrap_or(ZoneUsage {
        zone_index,
        capacity_bytes: 0,
        busy_bytes: 0,
        free_bytes: 0,
        busy_block_count: 0,
        free_block_count: 0,
        alloc_state: crow_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocActive,
        zone_state: DdbZoneHealth::Healthy,
    });
    let bitmap_bytes = {
        let disk_opt = dg.get_disk(disk.disk_id);
        disk_opt.and_then(|d| {
            let zones = d.zones.read().unwrap();
            let zi = zone_index as usize;
            (zi < zones.len()).then(|| zones[zi].usage_bits.snapshot())
        })
    };
    let zone_off = build_zone_usage_offset(&mut fbb, &zu, bitmap_bytes.as_deref());
    let disk_off = build_disk_info_offset(&mut fbb, disk, false, Some(zone_off));
    let disk_id_off = fbb.create_vector(&[FBInt128::new(disk.disk_id.high, disk.disk_id.low)]);
    let disk_vec_off = fbb.create_vector(&[disk_off]);
    let status = *dg.status.read().unwrap();
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
    fbb.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_get_disk_group_info_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    dg: &Arc<DdbDiskGroup>,
    usage: &DiskGroupUsage,
    disk_ids: &[DiskId],
    disks: &[Arc<DdbDisk>],
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

fn build_get_disk_info_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    disk: &Arc<DdbDisk>,
    include_zones: bool,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_rebuild_zone_bitmap_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    rebuilt_zone_count: u32,
    total_busy_units: u64,
    total_free_units: u64,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

fn build_recalc_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    results: &[DiskGroupRecalcResult],
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

#[allow(clippy::too_many_arguments)]
fn build_compact_zone_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    compacted_zone_count: u32,
    total_free_records_deleted: u32,
    zones: &[(u32, bool, u32, Option<String>)],
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

fn build_trigger_scan_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    summary: &ScanSummary,
    scan_in_progress: bool,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

fn build_get_scan_status_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBDiskdbRetCode,
    error_msg: Option<&str>,
    summary: &ScanSummary,
    has_run: bool,
) -> Vec<u8> {
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
    fbb.finished_data().to_vec()
}

// ── Flatbuffer offset builders (shared) ──────────────────────────

#[allow(clippy::too_many_arguments)]
fn build_disk_group_info_offset<'a>(
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
    let status = *dg.status.read().unwrap();
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

fn build_disk_info_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    disk: &Arc<DdbDisk>,
    include_zones: bool,
    single_zone: Option<flatbuffers::WIPOffset<FBZoneUsage<'a>>>,
) -> flatbuffers::WIPOffset<FBDiskInfo<'a>> {
    let dv = disk.disk_value.read().unwrap();
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

fn build_zone_usage_offset<'a>(
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

fn hw_status_to_fb(v: i32) -> FBHwStatus {
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

fn disk_type_to_fb(v: i32) -> FBDiskType {
    match v {
        x if x == crow_protocol::diskdb::rpc::DiskType::BlockHdd as i32 => FBDiskType::BlockHdd,
        x if x == crow_protocol::diskdb::rpc::DiskType::BlockSsd as i32 => FBDiskType::BlockSsd,
        x if x == crow_protocol::diskdb::rpc::DiskType::ZoneSsd as i32 => FBDiskType::ZoneSsd,
        x if x == crow_protocol::diskdb::rpc::DiskType::SmrHdd as i32 => FBDiskType::SmrHdd,
        _ => FBDiskType::BlockHdd,
    }
}

fn zone_alloc_state_to_fb(v: i32) -> FBZoneAllocationState {
    match v {
        x if x == crow_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocActive as i32 => {
            FBZoneAllocationState::Active
        }
        x if x == crow_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocAvailable as i32 => {
            FBZoneAllocationState::Available
        }
        x if x == crow_protocol::diskdb::rpc::ZoneAllocationState::ZoneAllocFull as i32 => {
            FBZoneAllocationState::Full
        }
        _ => FBZoneAllocationState::Active,
    }
}
