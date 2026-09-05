// Copyright 2026-present Gian <crow.db@outlook.com>.
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]

//! crowdb-rpc handler set for `DiskdbService` (R115 migration).
//!
//! Each handler dispatches by `msg_type` to the existing diskdb logic
//! (allocator, scanner, zone management) — the same logic bodies as the
//! tonic `DiskdbService` in `diskdb_service.rs`. The response is a
//! flatbuffer frame built per `design-crowdb-rpc.md` §6 (build → finish →
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

use crowdb_protocol::common::{ChunkId, DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::{DiskValue, Segment};
use crowdb_protocol::diskdb_fb::{
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
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
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

use crate::service::diskdb_service::{elapsed_ns, ALL_ZONES, MAX_ALLOCATE_COUNT};
use crate::service::mutation_gate;

/// Disk-group + usage + `disk_ids` + disks tuple for query responses.
type DiskGroupQueryEntry = (Arc<DdbDiskGroup>, DiskGroupUsage, Vec<DiskId>, Vec<Arc<DdbDisk>>);

/// crowdb-rpc handler set for `DiskdbService`. Holds the same dependencies
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
        f: fn(&Self, ServerRequest, &Arc<RpcServer>),
    ) -> impl Fn(ServerRequest) + Send + 'static {
        move |req| {
            f(&this, req, &server);
        }
    }
}

#[path = "admin.rs"]
mod admin;
#[path = "mutations.rs"]
mod mutations;
#[path = "queries.rs"]
mod queries;
#[path = "wire.rs"]
mod wire;

use wire::{
    build_allocate_response, build_commit_response, build_compact_zone_response, build_free_response,
    build_get_disk_group_info_response, build_get_disk_info_response, build_get_scan_status_response,
    build_query_capacity_response, build_query_capacity_response_zone, build_rebuild_zone_bitmap_response,
    build_recalc_response, build_trigger_scan_response, map_free_error, parse_segments, submit_error,
    submit_fb_response, AllocateParams,
};
