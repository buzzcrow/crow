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

use crow_protocol::common::{ChunkId, DiskId};
use crow_protocol::diskdb::rpc::Segment;
use crow_protocol::diskdb_fb::{
    FBAllocateBlocksRequest, FBAllocateResponse, FBAllocateResponseArgs, FBDiskdbRetCode, FBInt128, FBSegment,
};
use crow_protocol::fb::FBMsgType;
use crow_rpc_ffi::{RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;

use crate::ddb_config::StorageDefaults;
use crate::ddb_kv_client::DdbKvClient;
use crate::metrics::DiskdbMetrics;
use crate::model::alloc;
use crate::model::disk_group::{AllocError, DdbDiskGroup};
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::recovery::ZoneLoader;
use crate::scanner::ScanState;

use super::diskdb_service::MAX_ALLOCATE_COUNT;

/// crow-rpc handler set for `DiskdbService`. Holds the same dependencies
/// as the tonic `DiskdbService`; `register_handlers` wires one handler
/// per request `msg_type` into a `RpcServer`.
pub struct DiskdbRpcService {
    container: Arc<DdbDiskGroupContainer>,
    kv: Arc<DdbKvClient>,
    storage: StorageDefaults,
    #[allow(dead_code)] // wired in later handlers (compact/scan).
    zone_loader: Arc<ZoneLoader>,
    #[allow(dead_code)] // wired in later handlers (recalc/compact/scan).
    recalc: Arc<crate::metrics::RecalcEngine>,
    #[allow(dead_code)] // wired in later handlers (trigger_scan/get_scan_status).
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
        recalc: Arc<crate::metrics::RecalcEngine>,
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

    /// Register all diskdb request handlers into the `RpcServer`. R115
    /// ships the allocate handler first (proof-of-pattern); the remaining
    /// 10 are registered as the implementation progresses.
    pub fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EAllocateBlocksRequest.0 as u16,
            Self::make_allocate_handler(Arc::clone(self), Arc::clone(server)),
        );
        // Remaining handlers (free, commit, query, get_info, rebuild,
        // recalc, compact, trigger_scan, get_scan_status) are registered
        // as they are implemented — see plan-diskdb-rpc-migration.md.
    }

    fn make_allocate_handler(
        this: Arc<Self>,
        server: Arc<RpcServer>,
    ) -> impl Fn(ServerRequest<'_>) + Send + 'static {
        move |req| {
            this.handle_allocate(&req, &server);
        }
    }

    /// `AllocateBlocks` handler. Parses the request flatbuffer,
    /// validates, calls `alloc::allocate_blocks` (async — spawned on the
    /// tokio runtime), builds the response flatbuffer, and submits it.
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

        // Spawn the async allocate on the tokio runtime. Carry the raw
        // conn_handle as usize across the thread boundary (raw pointers
        // are not Send; usize is). The C++ transport keeps the
        // connection alive until the client disconnects or the server
        // stops, and submit_response is thread-safe.
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
            // Convert usize back to pointer only after the await, so the
            // non-Send `*mut c_void` is not live across the await point.
            let conn_handle = conn_handle_usize as *mut std::ffi::c_void;
            submit_allocate_result(
                result,
                &server,
                conn_handle,
                req_id,
                create_nano,
                msg_type,
                &metrics,
                rpc_start,
            );
        });
    }

    /// Parse + validate an `AllocateBlocks` request, extract the domain
    /// fields needed for `alloc::allocate_blocks`. Returns an error
    /// (`ret_code` + message) on validation failure.
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
}

/// Validated parameters for `alloc::allocate_blocks`, extracted from the
/// request flatbuffer by `validate_allocate`.
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

/// Submit the allocate result (success or `NoSpace`) as a flatbuffer
/// response. Runs in the spawned tokio task (synchronous — no await).
#[allow(clippy::too_many_arguments)]
fn submit_allocate_result(
    result: Result<Vec<Segment>, AllocError>,
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    metrics: &DiskdbMetrics,
    rpc_start: std::time::Instant,
) {
    match result {
        Ok(segments) => {
            metrics.allocate_total.inc();
            metrics
                .allocate_rpc_latency
                .observe(rpc_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
            let ctrl =
                build_allocate_response(req_id, create_nano, FBDiskdbRetCode::Success, None, &segments);
            // Safety: conn_handle is the Connection* from the dispatch
            // callback. The transport keeps the connection alive until
            // the client disconnects or the server stops; if it was
            // closed, submit_response returns an error (logged).
            unsafe {
                if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
                    tracing::warn!(?e, "allocate_blocks crow-rpc submit failed");
                }
            }
            tracing::debug!(
                "allocate_blocks crow-rpc ok: req_id={req_id}, segments={}",
                segments.len()
            );
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
    let ctrl = build_allocate_response(req_id, create_nano, ret_code, Some(msg), &[]);
    // Safety: conn_handle is the Connection* from the dispatch callback,
    // valid for the duration of this synchronous call.
    unsafe {
        if let Err(e) = server.submit_response(conn_handle, &ctrl, None, msg_type, req_id) {
            tracing::warn!(?e, "diskdb crow-rpc error submit failed");
        }
    }
}

/// Build an `FBAllocateResponse` control buffer.
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
