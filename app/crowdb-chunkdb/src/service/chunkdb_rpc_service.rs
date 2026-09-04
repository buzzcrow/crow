// Copyright 2026-present Gian <crow.db@outlook.com>.
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]

//! crowdb-rpc handler set for `ChunkdbService` (R116 migration).
//!
//! Each handler dispatches by `msg_type` to the existing lifecycle
//! logic — the same bodies as the tonic `ChunkdbService` in
//! `chunkdb_service.rs`. The response is a flatbuffer frame built per
//! `design-crowdb-rpc.md` §6 (build → finish → attach) and submitted via
//! `RpcServer::submit_response`.
//!
//! Handlers run on the C++ I/O worker thread. All chunkdb lifecycle
//! methods are async (KV persist, diskdb allocation), so each handler
//! spawns a tokio task via the captured `Handle` and submits the
//! response from the task. Each handler closure captures an
//! `Arc<RpcServer>` so it can submit responses from either the dispatch
//! thread (sync error path) or the spawned task (async success path).
//!
//! `NotMyRangeHint` is not a separate message — it is `ret_code =
//! NotMyRange` + `range_start`/`range_end` diagnostic fields on every
//! response table. The server does not know the owning instance; the
//! client refreshes its binding cache from group-0 and re-routes.

use std::sync::Arc;

use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState as ProtoChunkState, ChunkStrip, ChunkType as ProtoChunkType, EcState as ProtoEcState,
    Strip as ProtoStrip, StripType as ProtoStripType,
};
use crowdb_protocol::chunkdb_fb::{
    FBAllocateChunkRequest, FBAllocateChunkResponse, FBAllocateChunkResponseArgs, FBAppendChunkRequest,
    FBChunk, FBChunkArgs, FBChunkState, FBChunkStrip, FBChunkStripArgs, FBChunkType, FBChunkdbRetCode,
    FBDeleteChunkRangeRequest, FBDeleteChunkRangeResponse, FBDeleteChunkRangeResponseArgs,
    FBDeleteChunkRequest, FBEcState, FBEcStrip, FBEcStripArgs, FBInt128, FBListChunksRequest,
    FBListChunksResponse, FBListChunksResponseArgs, FBMirrorStrip, FBMirrorStripArgs, FBQueryChunkRequest,
    FBSealChunkRequest, FBSegment, FBStripBody, FBStripType, FBUpdateChunkStripRequest,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;

use crate::lifecycle::{LifecycleError, LifecycleHandler};

/// crowdb-rpc handler set for `ChunkdbService`. Holds the same
/// `LifecycleHandler` as the tonic `ChunkdbService`; `register_handlers`
/// wires one handler per request `msg_type` into a `RpcServer`.
pub struct ChunkdbRpcService {
    handler: Arc<LifecycleHandler>,
    /// Tokio runtime handle for spawning async work from the C++ I/O
    /// thread callback.
    rt: Handle,
}

impl ChunkdbRpcService {
    pub fn new(handler: Arc<LifecycleHandler>, rt: Handle) -> Self {
        Self { handler, rt }
    }

    /// Register all 8 chunkdb request handlers into the `RpcServer`.
    pub fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EAllocateChunkRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_allocate),
        );
        server.register_handler(
            FBMsgType::EAppendChunkRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_append),
        );
        server.register_handler(
            FBMsgType::EQueryChunkRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_query),
        );
        server.register_handler(
            FBMsgType::ESealChunkRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_seal),
        );
        server.register_handler(
            FBMsgType::EDeleteChunkRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_delete),
        );
        server.register_handler(
            FBMsgType::EDeleteChunkRangeRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_delete_range),
        );
        server.register_handler(
            FBMsgType::EUpdateChunkStripRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_update_strip),
        );
        server.register_handler(
            FBMsgType::EListChunksRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_list),
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

    // ── AllocateChunk ─────────────────────────────────────────────

    fn handle_allocate(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAllocateChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            // Parse the flatbuffer inside the async task — zero-copy from
            // the owned Frame (released when `req` drops at block end).
            let Ok(fb_req) = flatbuffers::root::<FBAllocateChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let chunk_id = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            });
            let Some(strip_type) = proto_strip_type(fb_req.strip_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip_type",
                );
                return;
            };
            let Some(chunk_type) = proto_chunk_type(fb_req.chunk_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid chunk_type",
                );
                return;
            };
            let write_granularity = fb_req.write_granularity();
            let strip_count = fb_req.strip_count();
            let data_num = fb_req.data_num();
            let code_num = fb_req.code_num();
            let copy_count = fb_req.copy_count();

            let result = handler
                .allocate_chunk(
                    chunk_id,
                    write_granularity,
                    strip_count,
                    strip_type,
                    data_num,
                    code_num,
                    copy_count,
                    chunk_type,
                )
                .await;
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── AppendChunk ───────────────────────────────────────────────

    fn handle_append(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAppendChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBAppendChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let Some(strip_type) = proto_strip_type(fb_req.strip_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip_type",
                );
                return;
            };
            let strip_count = fb_req.strip_count();
            let data_num = fb_req.data_num();
            let code_num = fb_req.code_num();
            let copy_count = fb_req.copy_count();
            let strip_size = fb_req.strip_size();

            let result = handler
                .append_chunk(
                    &chunk_id,
                    strip_count,
                    strip_type,
                    data_num,
                    code_num,
                    copy_count,
                    strip_size,
                )
                .await;
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── QueryChunk ────────────────────────────────────────────────

    fn handle_query(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EQueryChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBQueryChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };

            let result = handler.query_chunk(&chunk_id).await;
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── SealChunk ─────────────────────────────────────────────────

    fn handle_seal(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ESealChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBSealChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let seal_length = fb_req.seal_length();

            let result = handler.seal_chunk(&chunk_id, seal_length).await;
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── DeleteChunk ───────────────────────────────────────────────

    fn handle_delete(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDeleteChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBDeleteChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };

            let result = handler.delete_chunk(&chunk_id).await;
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── DeleteChunkRange ──────────────────────────────────────────

    fn handle_delete_range(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDeleteChunkRangeResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBDeleteChunkRangeRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let offset = fb_req.chunk_offset();
            let size = fb_req.chunk_size();

            let result = handler.delete_chunk_range(&chunk_id, offset, size).await;
            match result {
                Ok(()) => {
                    let ctrl = build_delete_range_response(
                        req_id,
                        create_nano,
                        FBChunkdbRetCode::Success,
                        None,
                        0,
                        0,
                    );
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
                Err(e) => {
                    let (code, msg, rs, re) = map_error(&e);
                    let ctrl = build_delete_range_response(req_id, create_nano, code, Some(&msg), rs, re);
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
            }
        });
    }

    // ── UpdateChunkStrip ──────────────────────────────────────────

    fn handle_update_strip(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EUpdateChunkStripResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBUpdateChunkStripRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let strip_index = fb_req.strip_index();
            let Some(fb_strip) = fb_req.strip() else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing strip",
                );
                return;
            };
            let Some(strip) = parse_fb_chunk_strip(&fb_strip) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip body",
                );
                return;
            };

            let result = handler.update_chunk_strip(&chunk_id, strip_index, strip).await;
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── ListChunks ────────────────────────────────────────────────

    fn handle_list(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EListChunksResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBListChunksRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let start_after = fb_req.start_token().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            });
            let max_keys = fb_req.max_keys();

            let result = handler.list_chunks(start_after.as_ref(), max_keys).await;
            match result {
                Ok(chunks) => {
                    let next_token = chunks.last().and_then(|c| c.id);
                    let has_next = next_token.is_some();
                    let ctrl = build_list_response(
                        req_id,
                        create_nano,
                        FBChunkdbRetCode::Success,
                        None,
                        0,
                        0,
                        &chunks,
                        next_token.as_ref(),
                        has_next,
                    );
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
                Err(e) => {
                    let (code, msg, rs, re) = map_error(&e);
                    let ctrl =
                        build_list_response(req_id, create_nano, code, Some(&msg), rs, re, &[], None, false);
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
            }
        });
    }
}

// ── Error mapping + submission helpers ────────────────────────────

/// Map a `LifecycleError` to `(ret_code, message, range_start, range_end)`.
fn map_error(e: &LifecycleError) -> (FBChunkdbRetCode, String, u32, u32) {
    match e {
        LifecycleError::NotMyRange { bucket } => {
            let b = u32::from(*bucket);
            (FBChunkdbRetCode::NotMyRange, e.to_string(), b, b)
        }
        LifecycleError::InvalidStateTransition(_) => {
            (FBChunkdbRetCode::FailedPrecondition, e.to_string(), 0, 0)
        }
        LifecycleError::ChunkNotFound => (FBChunkdbRetCode::NotFound, e.to_string(), 0, 0),
        LifecycleError::ChunkAlreadyExists => (FBChunkdbRetCode::AlreadyExists, e.to_string(), 0, 0),
        LifecycleError::StateConflict => (FBChunkdbRetCode::Aborted, e.to_string(), 0, 0),
        LifecycleError::Allocation(_) | LifecycleError::Commit(_) | LifecycleError::Cleanup(_) => {
            (FBChunkdbRetCode::Internal, e.to_string(), 0, 0)
        }
        LifecycleError::Storage(_) => (FBChunkdbRetCode::Internal, e.to_string(), 0, 0),
        LifecycleError::InvalidRequest(_) => (FBChunkdbRetCode::InvalidArgument, e.to_string(), 0, 0),
        LifecycleError::LockBusy | LifecycleError::LockTimeout => {
            (FBChunkdbRetCode::Unavailable, e.to_string(), 0, 0)
        }
        LifecycleError::StripIndexOutOfRange { .. } => {
            (FBChunkdbRetCode::StripIndexOutOfRange, e.to_string(), 0, 0)
        }
    }
}

/// Submit a flatbuffer response via the zero-copy buffer path. Takes
/// ownership of `ctrl` (the `(Vec<u8>, usize)` from `collapse()`). If
/// the buffer is empty/null, falls back to an empty-control submit.
fn submit_fb_response(
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

/// Submit a chunk-returning result (allocate/append/query/seal/delete/
/// update_strip). On success the chunk is encoded into the response;
/// on error the error code + message + range hint are set.
fn submit_chunk_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    result: Result<Chunk, LifecycleError>,
) {
    match result {
        Ok(chunk) => {
            let ctrl =
                build_chunk_response(req_id, create_nano, FBChunkdbRetCode::Success, None, 0, 0, &chunk);
            submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
        }
        Err(e) => {
            let (code, msg, rs, re) = map_error(&e);
            let ctrl = build_chunk_response(req_id, create_nano, code, Some(&msg), rs, re, &Chunk::default());
            submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
        }
    }
}

/// Submit a synchronous error response (from the dispatch thread).
fn submit_error(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    ret_code: FBChunkdbRetCode,
    msg: &str,
) {
    let ctrl = build_chunk_response(req_id, create_nano, ret_code, Some(msg), 0, 0, &Chunk::default());
    submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
}

// ── Enum conversion helpers ───────────────────────────────────────

/// Convert a flatbuffer `FBStripType` to the proto `StripType`.
fn proto_strip_type(fb: FBStripType) -> Option<ProtoStripType> {
    match fb {
        FBStripType::Mirror => Some(ProtoStripType::Mirror),
        FBStripType::Ec => Some(ProtoStripType::Ec),
        _ => None,
    }
}

/// Convert a flatbuffer `FBChunkType` to the proto `ChunkType`.
fn proto_chunk_type(fb: FBChunkType) -> Option<ProtoChunkType> {
    match fb {
        FBChunkType::Repo => Some(ProtoChunkType::Repo),
        FBChunkType::Wal => Some(ProtoChunkType::Wal),
        FBChunkType::BtreePage => Some(ProtoChunkType::BtreePage),
        FBChunkType::PageIndex => Some(ProtoChunkType::PageIndex),
        _ => None,
    }
}

// ── Request parsing helpers ───────────────────────────────────────

/// Parse a flatbuffer `FBChunkStrip` into a proto `ChunkStrip`.
/// Returns `None` if the strip body union is missing/invalid.
fn parse_fb_chunk_strip(fb: &FBChunkStrip<'_>) -> Option<ChunkStrip> {
    use crowdb_protocol::chunkdb::rpc::{EcStrip, MirrorStrip};

    let strip_type = match fb.strip_type() {
        FBStripType::Mirror => ProtoStripType::Mirror,
        FBStripType::Ec => ProtoStripType::Ec,
        _ => return None,
    };

    let strip = match fb.strip_body_type() {
        FBStripBody::FBMirrorStrip => {
            let mirror = fb.strip_body_as_fbmirror_strip()?;
            let segments = parse_fb_segments(mirror.segments());
            ProtoStrip::MirrorStrip(MirrorStrip { segments })
        }
        FBStripBody::FBEcStrip => {
            let ec = fb.strip_body_as_fbec_strip()?;
            let segments = parse_fb_segments(ec.segments());
            let ec_state = match ec.ec_state() {
                FBEcState::NoParity => ProtoEcState::NoParity,
                FBEcState::Parity => ProtoEcState::Parity,
                _ => return None,
            };
            ProtoStrip::EcStrip(EcStrip {
                data_num: ec.data_num(),
                code_num: ec.code_num(),
                ec_state: ec_state as i32,
                segments,
            })
        }
        _ => return None,
    };

    Some(ChunkStrip {
        chunk_offset: fb.chunk_offset(),
        strip_sequence: fb.strip_sequence(),
        unit_kb: fb.unit_kb(),
        capacity: fb.capacity(),
        create_ts_ms: fb.create_ts_ms(),
        sealed_ts_ms: fb.sealed_ts_ms(),
        sealed_length: fb.sealed_length(),
        strip_type: strip_type as i32,
        strip: Some(strip),
        usage_bitmap: fb
            .usage_bitmap()
            .map(|v| v.iter().collect::<Vec<u8>>())
            .unwrap_or_default(),
    })
}

/// Parse a flatbuffer `FBSegment` vector into proto `Segment`s.
fn parse_fb_segments<'a, V>(fb_segs: Option<V>) -> Vec<crowdb_protocol::diskdb::rpc::Segment>
where
    V: IntoIterator<Item = &'a FBSegment>,
{
    let Some(vec) = fb_segs else {
        return Vec::new();
    };
    vec.into_iter()
        .map(|s| crowdb_protocol::diskdb::rpc::Segment {
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
        })
        .collect()
}

// ── Response builders ─────────────────────────────────────────────

/// Build a chunk-carrying response (allocate/append/query/seal/delete/
/// update_strip). All six response tables share the same field layout
/// (`id`, `rpc_create_nano`, `ret_code`, `error_msg`, `range_start`,
/// `range_end`, `chunk`), so a single builder covers all of them — the
/// caller selects the table type via the `FBMsgType` constant used to
/// finish + submit.
fn build_chunk_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBChunkdbRetCode,
    error_msg: Option<&str>,
    range_start: u32,
    range_end: u32,
    chunk: &Chunk,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let chunk_off = if ret_code == FBChunkdbRetCode::Success {
        Some(build_chunk_offset(&mut fbb, chunk))
    } else {
        None
    };
    // All chunk-carrying response tables share the same Args shape, so
    // we build an FBAllocateChunkResponse as the generic carrier. The
    // client parses by msg_type and reads ret_code + error_msg + chunk.
    let off = FBAllocateChunkResponse::create(
        &mut fbb,
        &FBAllocateChunkResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            range_start,
            range_end,
            chunk: chunk_off,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

/// Build a `FBDeleteChunkRangeResponse` (no chunk field).
#[allow(clippy::too_many_arguments)]
fn build_delete_range_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBChunkdbRetCode,
    error_msg: Option<&str>,
    range_start: u32,
    range_end: u32,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let off = FBDeleteChunkRangeResponse::create(
        &mut fbb,
        &FBDeleteChunkRangeResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            range_start,
            range_end,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

/// Build a `FBListChunksResponse`.
#[allow(clippy::too_many_arguments)]
fn build_list_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBChunkdbRetCode,
    error_msg: Option<&str>,
    range_start: u32,
    range_end: u32,
    chunks: &[Chunk],
    next_token: Option<&ChunkId>,
    has_next_token: bool,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let chunk_offs: Vec<flatbuffers::WIPOffset<FBChunk<'_>>> =
        chunks.iter().map(|c| build_chunk_offset(&mut fbb, c)).collect();
    let chunks_vec = if chunk_offs.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&chunk_offs))
    };
    let next_token_off = next_token.map(|id| FBInt128::new(id.high, id.low));
    let off = FBListChunksResponse::create(
        &mut fbb,
        &FBListChunksResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            range_start,
            range_end,
            chunks: chunks_vec,
            next_token: next_token_off.as_ref(),
            has_next_token,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

/// Build a `FBChunk` WIPOffset from a proto `Chunk`.
fn build_chunk_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    chunk: &Chunk,
) -> flatbuffers::WIPOffset<FBChunk<'a>> {
    let id = chunk.id.unwrap_or_default();
    let id_off = FBInt128::new(id.high, id.low);
    let strip_offs: Vec<flatbuffers::WIPOffset<FBChunkStrip<'a>>> = chunk
        .strips
        .iter()
        .map(|s| build_chunk_strip_offset(fbb, s))
        .collect();
    let strips_vec = if strip_offs.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&strip_offs))
    };
    let state = ProtoChunkState::try_from(chunk.state).unwrap_or(ProtoChunkState::Init);
    let chunk_type = ProtoChunkType::try_from(chunk.chunk_type).unwrap_or(ProtoChunkType::Repo);
    FBChunk::create(
        fbb,
        &FBChunkArgs {
            id: Some(&id_off),
            state: fb_chunk_state(state),
            create_ts_ms: chunk.create_ts_ms,
            sealed_ts_ms: chunk.sealed_ts_ms,
            capacity: chunk.capacity,
            sealed_length: chunk.sealed_length,
            strips: strips_vec,
            chunk_type: fb_chunk_type(chunk_type),
        },
    )
}

/// Build a `FBChunkStrip` WIPOffset from a proto `ChunkStrip`.
fn build_chunk_strip_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    strip: &ChunkStrip,
) -> flatbuffers::WIPOffset<FBChunkStrip<'a>> {
    let strip_type = ProtoStripType::try_from(strip.strip_type).unwrap_or(ProtoStripType::Mirror);
    let (body_type, body_off) = build_strip_body_offset(fbb, strip);
    let usage_bitmap_off = if strip.usage_bitmap.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&strip.usage_bitmap))
    };
    FBChunkStrip::create(
        fbb,
        &FBChunkStripArgs {
            chunk_offset: strip.chunk_offset,
            strip_sequence: strip.strip_sequence,
            unit_kb: strip.unit_kb,
            capacity: strip.capacity,
            create_ts_ms: strip.create_ts_ms,
            sealed_ts_ms: strip.sealed_ts_ms,
            sealed_length: strip.sealed_length,
            strip_type: fb_strip_type(strip_type),
            strip_body_type: body_type,
            strip_body: body_off,
            usage_bitmap: usage_bitmap_off,
        },
    )
}

/// Build the strip body union offset from a proto `ChunkStrip`.
/// Returns `(union_type, union_offset)`.
fn build_strip_body_offset(
    fbb: &mut FlatBufferBuilder<'_>,
    strip: &ChunkStrip,
) -> (
    FBStripBody,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
) {
    let Some(ref body) = strip.strip else {
        return (FBStripBody::NONE, None);
    };
    match body {
        ProtoStrip::MirrorStrip(mirror) => {
            let seg_offs: Vec<FBSegment> = mirror
                .segments
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
            let seg_vec = fbb.create_vector(&seg_offs);
            let off = FBMirrorStrip::create(
                fbb,
                &FBMirrorStripArgs {
                    segments: Some(seg_vec),
                },
            );
            (FBStripBody::FBMirrorStrip, Some(off.as_union_value()))
        }
        ProtoStrip::EcStrip(ec) => {
            let seg_offs: Vec<FBSegment> = ec
                .segments
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
            let seg_vec = fbb.create_vector(&seg_offs);
            let ec_state = ProtoEcState::try_from(ec.ec_state).unwrap_or(ProtoEcState::NoParity);
            let off = FBEcStrip::create(
                fbb,
                &FBEcStripArgs {
                    data_num: ec.data_num,
                    code_num: ec.code_num,
                    ec_state: fb_ec_state(ec_state),
                    segments: Some(seg_vec),
                },
            );
            (FBStripBody::FBEcStrip, Some(off.as_union_value()))
        }
    }
}

// ── Enum cast helpers (proto i32 → FB enum) ───────────────────────

fn fb_chunk_state(s: ProtoChunkState) -> FBChunkState {
    match s {
        ProtoChunkState::Init => FBChunkState::Init,
        ProtoChunkState::Active => FBChunkState::Active,
        ProtoChunkState::Sealed => FBChunkState::Sealed,
        ProtoChunkState::Deleted => FBChunkState::Deleted,
    }
}

fn fb_chunk_type(t: ProtoChunkType) -> FBChunkType {
    match t {
        ProtoChunkType::Repo => FBChunkType::Repo,
        ProtoChunkType::Wal => FBChunkType::Wal,
        ProtoChunkType::BtreePage => FBChunkType::BtreePage,
        ProtoChunkType::PageIndex => FBChunkType::PageIndex,
    }
}

fn fb_strip_type(t: ProtoStripType) -> FBStripType {
    match t {
        ProtoStripType::Mirror => FBStripType::Mirror,
        ProtoStripType::Ec => FBStripType::Ec,
    }
}

fn fb_ec_state(s: ProtoEcState) -> FBEcState {
    match s {
        ProtoEcState::NoParity => FBEcState::NoParity,
        ProtoEcState::Parity => FBEcState::Parity,
    }
}
