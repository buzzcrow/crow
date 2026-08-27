// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]
#![allow(dead_code)] // Wired in Phase 9 (server wiring)
#![allow(clippy::too_many_lines)]
#![allow(clippy::too_many_arguments)]

//! crow-rpc handler set for the KV client-facing service (R117
//! migration). Each handler dispatches by `msg_type` to the existing
//! `KvStore` trait methods — the same logic bodies as the former gRPC
//! `KvStoreService` in `kv_service.rs`. The response is a flatbuffer
//! frame built per `design-crow-rpc.md` §6 (build → finish → attach)
//! and submitted via `RpcServer::submit_response`.
//!
//! Handlers run on the C++ I/O worker thread. Async paths (the `KvStore`
//! trait methods) spawn a tokio task via the captured `Handle` and
//! submit the response from the task. Each handler closure captures an
//! `Arc<RpcServer>` so it can submit responses from either the dispatch
//! thread (sync error path) or the spawned task (async success path).
//!
//! `Get`/`Scan`/`JournalScan` preserve the transparent leader-forward
//! step (linearizable reads only). The loop-guard `forwarded: bool`
//! field on the request flatbuffer replaces the former gRPC
//! `x-crow-kv-forwarded` metadata header. The forwarder
//! (`KvClientRpcForwarder`) lives in `crow-kv` itself (not
//! `crow-kv-client`) to avoid a crate cycle.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;
use tracing::{debug, warn};

use crow_protocol::fb::FBMsgType;
use crow_protocol::fb_wrappers::kv_client::{
    FBKvJournalScanResponseRef, FBKvResponseRef, FBKvScanResponseRef,
};
use crow_protocol::kv_client_fb::{
    FBCreateSnapshotResponse, FBCreateSnapshotResponseArgs, FBKvClientRetCode, FBKvJournalOp,
    FBKvJournalOpArgs, FBKvJournalScanRequest, FBKvJournalScanRequestArgs, FBKvJournalScanResponse,
    FBKvJournalScanResponseArgs, FBKvResponse, FBKvResponseArgs, FBKvScanItem, FBKvScanItemArgs,
    FBKvScanRequest, FBKvScanRequestArgs, FBKvScanResponse, FBKvScanResponseArgs, FBReadMode,
    FBReleaseSnapshotResponse, FBReleaseSnapshotResponseArgs, FBSnapshotInfo, FBSnapshotInfoArgs,
    FBSnapshotScanResponse, FBSnapshotScanResponseArgs, FBWatchNotifyError, FBWatchNotifyErrorArgs,
    FBWatchSubscribe, FBWatchUnsubscribe,
};
use crow_rpc_ffi::{noop_completion, Buffer, Connection, RpcClient, RpcError, RpcServer};

use crate::cluster::kv_store::KvStore;
use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::{
    FBKvDeleteRequest, FBKvGetRequest, FBKvGetRequestArgs, FBKvSetRequest, KvBatchItem, ReadMode,
};

// ── KvClientRpcForwarder ─────────────────────────────────────────

/// Minimal server-side forwarder for transparent leader-forwarding of
/// linearizable reads (R117). Lives in `crow-kv` (not `crow-kv-client`)
/// to avoid a crate cycle. Holds an `Arc<RpcServer>`, an `Arc<RpcClient>`,
/// and a connection cache. Builds the request flatbuffer with
/// `forwarded = true`, calls `rpc.call()`, and returns the raw response
/// control buffer for the handler to submit back to the original
/// client.
pub(crate) struct KvClientRpcForwarder {
    pub(crate) server: Arc<RpcServer>,
    pub(crate) rpc: Arc<RpcClient>,
    connections: DashMap<String, Connection>,
    next_req_id: AtomicU64,
}

impl KvClientRpcForwarder {
    pub(crate) fn new() -> Self {
        Self::with_workers(2)
    }

    pub(crate) fn with_workers(workers: u32) -> Self {
        let server = Arc::new(RpcServer::with_engines(None, 1, workers));
        server.start();
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
        rpc.start_reaper(5_000_000_000, 500_000_000);
        Self {
            server,
            rpc,
            connections: DashMap::new(),
            next_req_id: AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a `Connection` for the given endpoint. The
    /// crow-rpc server listens on the same port as the gRPC endpoint
    /// (no port derivation).
    fn conn_for(&self, rpc_endpoint: &str) -> Result<Connection, RpcError> {
        let normalized = normalize_endpoint(rpc_endpoint);
        if let Some(conn) = self.connections.get(&normalized) {
            return Ok(conn.clone());
        }
        let (host, port) = parse_endpoint(&normalized).map_err(|_| RpcError::InvalidArg)?;
        let conn = self.server.connect(&host, port)?;
        self.rpc.attach(&conn);
        self.connections.insert(normalized, conn.clone());
        Ok(conn)
    }

    /// Forward a `Get` request to the leader. Returns the leader's
    /// response control buffer on success.
    pub(crate) async fn forward_get(
        &self,
        rpc_endpoint: &str,
        req: &FBKvGetRequest<'_>,
    ) -> Result<Vec<u8>, RpcError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let key = builder.create_vector(req.key().map_or(&[], |v| v.bytes()));
        let args = FBKvGetRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: req.version(),
            key: Some(key),
            request_id: req.request_id(),
            request_create_ms: req.request_create_ms(),
            group_id: req.group_id(),
            read_mode: req.read_mode(),
            min_slot: req.min_slot(),
            forwarded: true,
        };
        let fb_req = FBKvGetRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvGetRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)?;
        let resp = fut.await?;
        let ctrl = resp.control.ok_or(RpcError::ConnectionError)?;
        Ok(ctrl.bytes().to_vec())
    }

    /// Forward a `Scan` request to the leader.
    pub(crate) async fn forward_scan(
        &self,
        rpc_endpoint: &str,
        req: &FBKvScanRequest<'_>,
    ) -> Result<Vec<u8>, RpcError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let prefix = builder.create_vector(req.prefix().map_or(&[], |v| v.bytes()));
        let start_after = builder.create_vector(req.start_after().map_or(&[], |v| v.bytes()));
        let end_key = builder.create_vector(req.end_key().map_or(&[], |v| v.bytes()));
        let args = FBKvScanRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: req.version(),
            prefix: Some(prefix),
            limit: req.limit(),
            request_id: req.request_id(),
            request_create_ms: req.request_create_ms(),
            group_id: req.group_id(),
            read_mode: req.read_mode(),
            start_after: Some(start_after),
            end_key: Some(end_key),
            min_slot: req.min_slot(),
            keys_only: req.keys_only(),
            count_only: req.count_only(),
            deadline_ms: req.deadline_ms(),
            forwarded: true,
        };
        let fb_req = FBKvScanRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvScanRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)?;
        let resp = fut.await?;
        let ctrl = resp.control.ok_or(RpcError::ConnectionError)?;
        Ok(ctrl.bytes().to_vec())
    }

    /// Forward a `JournalScan` request to the leader.
    pub(crate) async fn forward_journal_scan(
        &self,
        rpc_endpoint: &str,
        req: &FBKvJournalScanRequest<'_>,
    ) -> Result<Vec<u8>, RpcError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let key_prefix = builder.create_vector(req.key_prefix().map_or(&[], |v| v.bytes()));
        let args = FBKvJournalScanRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: req.version(),
            group_id: req.group_id(),
            min_slot: req.min_slot(),
            max_slot: req.max_slot(),
            key_prefix: Some(key_prefix),
            limit: req.limit(),
            request_id: req.request_id(),
            request_create_ms: req.request_create_ms(),
            read_mode: req.read_mode(),
            forwarded: true,
        };
        let fb_req = FBKvJournalScanRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvJournalScanRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)?;
        let resp = fut.await?;
        let ctrl = resp.control.ok_or(RpcError::ConnectionError)?;
        Ok(ctrl.bytes().to_vec())
    }

    /// Send a fire-and-forget `WatchNotifyError` push frame on the
    /// given connection (server→client). Used when a subscribe arrives
    /// on a non-leader or for a missing group.
    pub(crate) fn send_watch_notify_error(
        &self,
        conn: &Connection,
        group_id: u64,
        not_leader_hint: &str,
        error: &str,
    ) {
        let req_id = self.next_id();
        let mut builder = FlatBufferBuilder::new();
        let hint = builder.create_string(not_leader_hint);
        let err = builder.create_string(error);
        let args = FBWatchNotifyErrorArgs {
            id: req_id,
            rpc_create_nano: 0,
            group_id,
            not_leader_hint: Some(hint),
            error: Some(err),
        };
        let fb = FBWatchNotifyError::create(&mut builder, &args);
        builder.finish(fb, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EWatchNotifyError.0 as u16;
        let _ = self.rpc.send_to_handle(
            &self.server,
            conn.handle().cast::<std::ffi::c_void>(),
            req_id,
            control,
            None,
            msg_type,
            noop_completion(),
            std::ptr::null_mut(),
        );
    }
}

// ── KvRpcService ─────────────────────────────────────────────────

/// crow-rpc handler set for the KV client-facing service. Holds the
/// same dependencies as the former `KvStoreService` plus a tokio
/// `Handle` for spawning async work from the C++ I/O thread callback,
/// and a `KvClientRpcForwarder` for transparent leader-forwarding.
pub struct KvRpcService {
    store: Arc<PxKvStore>,
    rt: Handle,
    forwarder: Arc<KvClientRpcForwarder>,
}

impl KvRpcService {
    pub(crate) fn new(store: Arc<PxKvStore>, rt: Handle, forwarder: Arc<KvClientRpcForwarder>) -> Self {
        Self { store, rt, forwarder }
    }

    /// Register all client-facing request handlers into the `RpcServer`.
    pub(crate) fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EKvSetRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_put),
        );
        server.register_handler(
            FBMsgType::EKvGetRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_get),
        );
        server.register_handler(
            FBMsgType::EKvDeleteRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_delete),
        );
        server.register_handler(
            FBMsgType::EKvBatchWriteRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_batch_write),
        );
        server.register_handler(
            FBMsgType::EKvScanRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_scan),
        );
        server.register_handler(
            FBMsgType::EKvJournalScanRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_journal_scan),
        );
        server.register_handler(
            FBMsgType::ECreateSnapshotRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_create_snapshot),
        );
        server.register_handler(
            FBMsgType::EListSnapshotsRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_list_snapshots),
        );
        server.register_handler(
            FBMsgType::ESnapshotScanRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_snapshot_scan),
        );
        server.register_handler(
            FBMsgType::EReleaseSnapshotRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                Self::handle_release_snapshot,
            ),
        );
        // WatchNotify: fire-and-forget client→server frames (no response).
        server.register_handler(
            FBMsgType::EWatchSubscribe.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_watch_subscribe),
        );
        server.register_handler(
            FBMsgType::EWatchUnsubscribe.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                Self::handle_watch_unsubscribe,
            ),
        );
    }

    fn make_handler(
        this: Arc<Self>,
        server: Arc<RpcServer>,
        f: fn(&Self, crow_rpc_ffi::ServerRequest, &Arc<RpcServer>),
    ) -> impl Fn(crow_rpc_ffi::ServerRequest) + Send + 'static {
        move |req| {
            f(&this, req, &server);
        }
    }

    // ── Put ───────────────────────────────────────────────────────

    fn handle_put(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBKvSetRequest>(req.control()) else {
                submit_kv_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let key = fb_req.key().map(|v| v.bytes()).unwrap_or_default();
            let value = fb_req.value().map(|v| v.bytes()).unwrap_or_default();
            let client_id = fb_req.client_id();
            let seq = fb_req.seq();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();
            let resp = store
                .kv_put(
                    group_id,
                    key,
                    value,
                    client_id,
                    seq,
                    request_id,
                    request_create_ms,
                )
                .await;
            let ctrl = build_kv_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── Get ───────────────────────────────────────────────────────

    fn handle_get(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server_clone = Arc::clone(server);

        // Transparent leader-forward (linearizable only, loop-guard via `forwarded`).
        // Parse the flatbuffer once to check forwarding, then move `req` into the
        // async task for zero-copy access to key/control bytes.
        let forward_info = {
            let Ok(fb_req) = flatbuffers::root::<FBKvGetRequest>(req.control()) else {
                submit_kv_error(
                    server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let read_mode = fb_req.read_mode();
            let forwarded = fb_req.forwarded();
            let linearizable = read_mode == FBReadMode::Linearizable;
            if linearizable && !forwarded {
                self.store
                    .forward_target_for(group_id)
                    .map(|endpoint| (group_id, endpoint))
            } else {
                None
            }
        };

        let fwd = Arc::clone(&self.forwarder);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBKvGetRequest>(req.control()) else {
                // Should not happen — already validated above — but guard anyway.
                submit_kv_error(
                    &server_clone,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let key = fb_req.key().map(|v| v.bytes()).unwrap_or_default();
            let read_mode = fb_req.read_mode();
            let min_slot = fb_req.min_slot();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();

            if let Some((_, endpoint)) = forward_info {
                match fwd.forward_get(&endpoint, &fb_req).await {
                    Ok(leader_ctrl) => {
                        debug!(group_id, request_id, leader = %endpoint, "kv get forwarded to leader");
                        submit_fb_response(
                            &server_clone,
                            conn_handle_usize as *mut std::ffi::c_void,
                            (leader_ctrl, 0),
                            msg_type,
                            req_id,
                        );
                        return;
                    }
                    Err(e) => {
                        warn!(group_id, request_id, leader = %endpoint, error = %e, "kv get forward failed; serving stale local with hint");
                    }
                }
                // Forward failed: serve stale local + hint.
                let resp = store
                    .kv_get(group_id, key, ReadMode::Linearizable as i32, min_slot, request_id, request_create_ms)
                    .await;
                let mut ctrl = build_kv_response(req_id, create_nano, &resp);
                patch_not_leader_hint(&mut ctrl, &endpoint);
                submit_fb_response(
                    &server_clone,
                    conn_handle_usize as *mut std::ffi::c_void,
                    ctrl,
                    msg_type,
                    req_id,
                );
                return;
            }

            // Serve locally.
            let linearizable = read_mode == FBReadMode::Linearizable;
            let read_mode_i32 = if linearizable {
                ReadMode::Linearizable as i32
            } else {
                ReadMode::MinSlot as i32
            };
            let resp = store
                .kv_get(
                    group_id,
                    key,
                    read_mode_i32,
                    min_slot,
                    request_id,
                    request_create_ms,
                )
                .await;
            let ctrl = build_kv_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server_clone,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── Delete ────────────────────────────────────────────────────

    fn handle_delete(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBKvDeleteRequest>(req.control()) else {
                submit_kv_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let key = fb_req.key().map(|v| v.bytes()).unwrap_or_default();
            let client_id = fb_req.client_id();
            let seq = fb_req.seq();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();
            let resp = store
                .kv_delete(group_id, key, client_id, seq, request_id, request_create_ms)
                .await;
            let ctrl = build_kv_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── BatchWrite ────────────────────────────────────────────────

    fn handle_batch_write(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<crate::rpc::FBKvBatchWriteRequest>(req.control()) else {
                submit_kv_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let items: Vec<KvBatchItem> = fb_req
                .items()
                .map(|v| {
                    v.iter()
                        .map(|item| KvBatchItem {
                            key: item.key().map(|k| k.bytes().to_vec()).unwrap_or_default().into(),
                            value: item
                                .value()
                                .map(|v| v.bytes().to_vec())
                                .unwrap_or_default()
                                .into(),
                            is_delete: item.is_delete(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let client_id = fb_req.client_id();
            let seq = fb_req.seq();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();
            let resp = store
                .kv_batch_write(group_id, items, client_id, seq, request_id, request_create_ms)
                .await;
            let ctrl = build_kv_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── Scan ──────────────────────────────────────────────────────

    fn handle_scan(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvScanResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server_clone = Arc::clone(server);

        // Determine forwarding decision up front (parses the flatbuffer once
        // on the dispatch thread), then move `req` into the async task for
        // zero-copy access to the request bytes.
        let forward_info = {
            let Ok(fb_req) = flatbuffers::root::<FBKvScanRequest>(req.control()) else {
                submit_scan_error(
                    server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let read_mode = fb_req.read_mode();
            let forwarded = fb_req.forwarded();
            let linearizable = read_mode == FBReadMode::Linearizable;
            if linearizable && !forwarded {
                self.store
                    .forward_target_for(group_id)
                    .map(|endpoint| (group_id, endpoint))
            } else {
                None
            }
        };

        let fwd = Arc::clone(&self.forwarder);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBKvScanRequest>(req.control()) else {
                submit_scan_error(
                    &server_clone,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let prefix = fb_req.prefix().map(|v| v.bytes()).unwrap_or_default();
            let start_after = fb_req.start_after().map(|v| v.bytes()).unwrap_or_default();
            let end_key = fb_req.end_key().map(|v| v.bytes()).unwrap_or_default();
            let limit = fb_req.limit();
            let read_mode = fb_req.read_mode();
            let min_slot = fb_req.min_slot();
            let keys_only = fb_req.keys_only();
            let count_only = fb_req.count_only();
            let deadline_ms = fb_req.deadline_ms();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();

            if let Some((_, endpoint)) = forward_info {
                match fwd.forward_scan(&endpoint, &fb_req).await {
                    Ok(leader_ctrl) => {
                        debug!(group_id, request_id, leader = %endpoint, "kv scan forwarded to leader");
                        submit_fb_response(
                            &server_clone,
                            conn_handle_usize as *mut std::ffi::c_void,
                            (leader_ctrl, 0),
                            msg_type,
                            req_id,
                        );
                        return;
                    }
                    Err(e) => {
                        warn!(group_id, request_id, leader = %endpoint, error = %e, "kv scan forward failed; serving stale local with hint");
                    }
                }
                let read_mode_i32 = ReadMode::Linearizable as i32;
                let resp = store
                    .kv_scan(group_id, prefix, start_after, end_key, limit, read_mode_i32, min_slot, keys_only, count_only, deadline_ms, request_id, request_create_ms)
                    .await;
                let mut ctrl = build_scan_response(req_id, create_nano, &resp);
                patch_scan_not_leader_hint(&mut ctrl, &endpoint);
                submit_fb_response(
                    &server_clone,
                    conn_handle_usize as *mut std::ffi::c_void,
                    ctrl,
                    msg_type,
                    req_id,
                );
                return;
            }

            // Serve locally.
            let linearizable = read_mode == FBReadMode::Linearizable;
            let read_mode_i32 = if linearizable {
                ReadMode::Linearizable as i32
            } else {
                ReadMode::MinSlot as i32
            };
            let resp = store
                .kv_scan(
                    group_id,
                    prefix,
                    start_after,
                    end_key,
                    limit,
                    read_mode_i32,
                    min_slot,
                    keys_only,
                    count_only,
                    deadline_ms,
                    request_id,
                    request_create_ms,
                )
                .await;
            let ctrl = build_scan_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server_clone,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── JournalScan ───────────────────────────────────────────────

    fn handle_journal_scan(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvJournalScanResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server_clone = Arc::clone(server);

        // Determine forwarding decision up front (parses the flatbuffer once
        // on the dispatch thread), then move `req` into the async task for
        // zero-copy access to the request bytes.
        let forward_info = {
            let Ok(fb_req) = flatbuffers::root::<FBKvJournalScanRequest>(req.control()) else {
                submit_journal_scan_error(
                    server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let read_mode = fb_req.read_mode();
            let forwarded = fb_req.forwarded();
            let linearizable = read_mode == FBReadMode::Linearizable;
            if linearizable && !forwarded {
                self.store
                    .forward_target_for(group_id)
                    .map(|endpoint| (group_id, endpoint))
            } else {
                None
            }
        };

        let fwd = Arc::clone(&self.forwarder);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBKvJournalScanRequest>(req.control()) else {
                submit_journal_scan_error(
                    &server_clone,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let min_slot = fb_req.min_slot();
            let max_slot = fb_req.max_slot();
            let key_prefix = fb_req.key_prefix().map(|v| v.bytes()).unwrap_or_default();
            let limit = fb_req.limit();
            let read_mode = fb_req.read_mode();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();

            if let Some((_, endpoint)) = forward_info {
                match fwd.forward_journal_scan(&endpoint, &fb_req).await {
                    Ok(leader_ctrl) => {
                        debug!(group_id, request_id, leader = %endpoint, "kv journal_scan forwarded to leader");
                        submit_fb_response(
                            &server_clone,
                            conn_handle_usize as *mut std::ffi::c_void,
                            (leader_ctrl, 0),
                            msg_type,
                            req_id,
                        );
                        return;
                    }
                    Err(e) => {
                        warn!(group_id, request_id, leader = %endpoint, error = %e, "kv journal_scan forward failed; serving stale local with hint");
                    }
                }
                let read_mode_i32 = ReadMode::Linearizable as i32;
                let resp = store
                    .kv_journal_scan(group_id, min_slot, max_slot, key_prefix, limit, read_mode_i32, request_id, request_create_ms)
                    .await;
                let mut ctrl = build_journal_scan_response(req_id, create_nano, &resp);
                patch_journal_scan_not_leader_hint(&mut ctrl, &endpoint);
                submit_fb_response(
                    &server_clone,
                    conn_handle_usize as *mut std::ffi::c_void,
                    ctrl,
                    msg_type,
                    req_id,
                );
                return;
            }

            // Serve locally.
            let linearizable = read_mode == FBReadMode::Linearizable;
            let read_mode_i32 = if linearizable {
                ReadMode::Linearizable as i32
            } else {
                ReadMode::MinSlot as i32
            };
            let resp = store
                .kv_journal_scan(
                    group_id,
                    min_slot,
                    max_slot,
                    key_prefix,
                    limit,
                    read_mode_i32,
                    request_id,
                    request_create_ms,
                )
                .await;
            let ctrl = build_journal_scan_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server_clone,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── CreateSnapshot ────────────────────────────────────────────

    fn handle_create_snapshot(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::ECreateSnapshotResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<crate::rpc::FBCreateSnapshotRequest>(req.control()) else {
                submit_create_snapshot_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let read_mode = fb_req.read_mode();
            let min_slot = fb_req.min_slot();
            let read_mode_i32 = if read_mode == FBReadMode::Linearizable {
                ReadMode::Linearizable as i32
            } else {
                ReadMode::MinSlot as i32
            };
            let resp = store.kv_create_snapshot(group_id, read_mode_i32, min_slot).await;
            let ctrl = build_create_snapshot_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── ListSnapshots ─────────────────────────────────────────────

    fn handle_list_snapshots(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EListSnapshotsResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<crate::rpc::FBListSnapshotsRequest>(req.control()) else {
                submit_list_snapshots_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let resp = store.kv_list_snapshots(group_id).await;
            let ctrl = build_list_snapshots_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── SnapshotScan ──────────────────────────────────────────────

    fn handle_snapshot_scan(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::ESnapshotScanResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<crate::rpc::FBSnapshotScanRequest>(req.control()) else {
                submit_snapshot_scan_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let snapshot_handle = fb_req.snapshot_handle();
            let prefix = fb_req.prefix().map(|v| v.bytes()).unwrap_or_default();
            let start_after = fb_req.start_after().map(|v| v.bytes()).unwrap_or_default();
            let limit = fb_req.limit();
            let resp = store
                .kv_snapshot_scan(group_id, snapshot_handle, prefix, start_after, limit)
                .await;
            let ctrl = build_snapshot_scan_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── ReleaseSnapshot ───────────────────────────────────────────

    fn handle_release_snapshot(&self, req: crow_rpc_ffi::ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EReleaseSnapshotResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<crate::rpc::FBReleaseSnapshotRequest>(req.control()) else {
                submit_release_snapshot_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            let snapshot_handle = fb_req.snapshot_handle();
            let resp = store.kv_release_snapshot(group_id, snapshot_handle).await;
            let ctrl = build_release_snapshot_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── WatchSubscribe (fire-and-forget, no response) ─────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    fn handle_watch_subscribe(&self, req: crow_rpc_ffi::ServerRequest, _server: &Arc<RpcServer>) {
        let conn_handle_usize = req.conn_handle as usize;
        let Ok(fb_req) = flatbuffers::root::<FBWatchSubscribe>(req.control()) else {
            debug!(
                store_id = self.store.store_id,
                "watch subscribe: invalid flatbuffer"
            );
            return;
        };
        let group_id = fb_req.group_id();
        let prefix = fb_req.prefix().map_or(&[][..], |v| v.bytes()).to_vec();

        let Some(group) = self.store.get_group(group_id) else {
            let conn = Connection::from_handle(conn_handle_usize as crow_rpc_ffi::sys::crow_rpc_conn_t);
            self.forwarder.send_watch_notify_error(
                &conn,
                group_id,
                "",
                &format!("group {group_id} not found on store {}", self.store.store_id),
            );
            return;
        };
        if !group.local_replica().is_leader() {
            let hint = group.leader_endpoint().unwrap_or_default();
            let conn = Connection::from_handle(conn_handle_usize as crow_rpc_ffi::sys::crow_rpc_conn_t);
            self.forwarder.send_watch_notify_error(&conn, group_id, &hint, "");
            return;
        }
        let conn = Connection::from_handle(conn_handle_usize as crow_rpc_ffi::sys::crow_rpc_conn_t);
        let target = Arc::new(crate::cluster::watch_registry::CrowRpcPushTarget::new(
            conn,
            Arc::clone(&self.forwarder.rpc),
            Arc::clone(&self.forwarder.server),
        ));
        let registry = group.watch_registry.clone();
        let watcher_id = registry.subscribe_crow_rpc(&prefix, target);
        debug!(
            store_id = self.store.store_id,
            group_id,
            watcher_id,
            prefix_len = prefix.len(),
            "watch subscribed (crow-rpc push target)"
        );
        // req dropped here, frame released
    }

    // ── WatchUnsubscribe (fire-and-forget, no response) ───────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    fn handle_watch_unsubscribe(&self, req: crow_rpc_ffi::ServerRequest, _server: &Arc<RpcServer>) {
        let Ok(fb_req) = flatbuffers::root::<FBWatchUnsubscribe>(req.control()) else {
            debug!(
                store_id = self.store.store_id,
                "watch unsubscribe: invalid flatbuffer"
            );
            return;
        };
        let group_id = fb_req.group_id();
        let prefix = fb_req.prefix().map_or(&[][..], |v| v.bytes()).to_vec();

        let Some(group) = self.store.get_group(group_id) else {
            debug!(
                store_id = self.store.store_id,
                group_id, "watch unsubscribe dropped (group not found)"
            );
            return;
        };
        let _registry = group.watch_registry.clone();
        debug!(
            store_id = self.store.store_id,
            group_id,
            prefix_len = prefix.len(),
            "watch unsubscribe received (crow-rpc: lazy cleanup via dead-connection detection)"
        );
        // req dropped here, frame released
    }
}

// ── Error → ret_code mapping ─────────────────────────────────────

/// Map a `KvErrorCode` to the flatbuffer `FBKvClientRetCode`.
fn kv_error_code_to_fb(code: i32) -> FBKvClientRetCode {
    match code {
        0 => FBKvClientRetCode::Success,
        1 => FBKvClientRetCode::NotLeader,
        2 => FBKvClientRetCode::Unavailable,
        4 => FBKvClientRetCode::JournalScanGcGap,
        _ => FBKvClientRetCode::Internal,
    }
}

// ── Response builders ────────────────────────────────────────────

fn build_kv_response(req_id: u64, create_nano: u64, resp: &crate::rpc::KvResponse) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let not_leader_hint = if resp.not_leader_hint.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.not_leader_hint))
    };
    let value = if resp.value.is_empty() {
        None
    } else {
        Some(builder.create_vector(&resp.value))
    };
    let args = FBKvResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: kv_error_code_to_fb(resp.error_code),
        error_msg,
        version: resp.version,
        ok: resp.ok,
        revision: resp.revision,
        not_found: resp.not_found,
        not_leader_hint,
        request_id: resp.request_id,
        request_create_ms: resp.request_create_ms,
        value,
        read_slot: resp.read_slot,
        safe_slot: resp.safe_slot,
    };
    let fb = FBKvResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

fn build_scan_response(req_id: u64, create_nano: u64, resp: &crate::rpc::KvScanResponse) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let not_leader_hint = if resp.not_leader_hint.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.not_leader_hint))
    };
    let items_vec: Vec<_> = resp
        .items
        .iter()
        .map(|item| {
            let key = builder.create_vector(&item.key);
            let value = builder.create_vector(&item.value);
            FBKvScanItem::create(
                &mut builder,
                &FBKvScanItemArgs {
                    key: Some(key),
                    value: Some(value),
                },
            )
        })
        .collect();
    let items = if items_vec.is_empty() {
        None
    } else {
        Some(builder.create_vector(&items_vec))
    };
    let args = FBKvScanResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: kv_error_code_to_fb(resp.error_code),
        error_msg,
        version: resp.version,
        ok: resp.ok,
        truncated: resp.truncated,
        items,
        request_id: resp.request_id,
        request_create_ms: resp.request_create_ms,
        read_slot: resp.read_slot,
        not_leader_hint,
        count: resp.count,
        timed_out: resp.timed_out,
    };
    let fb = FBKvScanResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

fn build_journal_scan_response(
    req_id: u64,
    create_nano: u64,
    resp: &crate::rpc::KvJournalScanResponse,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let not_leader_hint = if resp.not_leader_hint.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.not_leader_hint))
    };
    let ops_vec: Vec<_> = resp
        .ops
        .iter()
        .map(|op| {
            let key = builder.create_vector(&op.key);
            let value = builder.create_vector(&op.value);
            FBKvJournalOp::create(
                &mut builder,
                &FBKvJournalOpArgs {
                    key: Some(key),
                    value: Some(value),
                    is_delete: op.is_delete,
                    slot: op.slot,
                },
            )
        })
        .collect();
    let ops = if ops_vec.is_empty() {
        None
    } else {
        Some(builder.create_vector(&ops_vec))
    };
    let args = FBKvJournalScanResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: kv_error_code_to_fb(resp.error_code),
        error_msg,
        version: resp.version,
        ok: resp.ok,
        ops,
        truncated: resp.truncated,
        last_op_slot: resp.last_op_slot,
        read_slot: resp.read_slot,
        not_leader_hint,
        request_id: resp.request_id,
        request_create_ms: resp.request_create_ms,
    };
    let fb = FBKvJournalScanResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

fn build_create_snapshot_response(
    req_id: u64,
    create_nano: u64,
    resp: &crate::rpc::CreateSnapshotResponse,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let not_leader_hint = if resp.not_leader_hint.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.not_leader_hint))
    };
    let args = FBCreateSnapshotResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: kv_error_code_to_fb(resp.error_code),
        error_msg,
        ok: resp.ok,
        snapshot_handle: resp.snapshot_handle,
        at_slot: resp.at_slot,
        not_leader_hint,
    };
    let fb = FBCreateSnapshotResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

fn build_list_snapshots_response(
    req_id: u64,
    create_nano: u64,
    resp: &crate::rpc::ListSnapshotsResponse,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let snaps_vec: Vec<_> = resp
        .snapshots
        .iter()
        .map(|s| {
            FBSnapshotInfo::create(
                &mut builder,
                &FBSnapshotInfoArgs {
                    snapshot_handle: s.snapshot_handle,
                    at_slot: s.at_slot,
                    lease_remaining_ms: s.lease_remaining_ms,
                },
            )
        })
        .collect();
    let snapshots = if snaps_vec.is_empty() {
        None
    } else {
        Some(builder.create_vector(&snaps_vec))
    };
    let args = crate::rpc::FBListSnapshotsResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: if resp.ok {
            FBKvClientRetCode::Success
        } else {
            FBKvClientRetCode::Internal
        },
        error_msg,
        ok: resp.ok,
        snapshots,
    };
    let fb = crate::rpc::FBListSnapshotsResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

fn build_snapshot_scan_response(
    req_id: u64,
    create_nano: u64,
    resp: &crate::rpc::SnapshotScanResponse,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let items_vec: Vec<_> = resp
        .items
        .iter()
        .map(|item| {
            let key = builder.create_vector(&item.key);
            let value = builder.create_vector(&item.value);
            FBKvScanItem::create(
                &mut builder,
                &FBKvScanItemArgs {
                    key: Some(key),
                    value: Some(value),
                },
            )
        })
        .collect();
    let items = if items_vec.is_empty() {
        None
    } else {
        Some(builder.create_vector(&items_vec))
    };
    let args = FBSnapshotScanResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: kv_error_code_to_fb(resp.error_code),
        error_msg,
        ok: resp.ok,
        truncated: resp.truncated,
        items,
    };
    let fb = FBSnapshotScanResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

fn build_release_snapshot_response(
    req_id: u64,
    create_nano: u64,
    resp: &crate::rpc::ReleaseSnapshotResponse,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = if resp.error.is_empty() {
        None
    } else {
        Some(builder.create_string(&resp.error))
    };
    let args = FBReleaseSnapshotResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: if resp.ok {
            FBKvClientRetCode::Success
        } else {
            FBKvClientRetCode::Internal
        },
        error_msg,
        ok: resp.ok,
    };
    let fb = FBReleaseSnapshotResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    builder.collapse()
}

// ── NotLeaderHint patching (forward-failed fallback) ─────────────

/// Rebuild a `FBKvResponse` with `not_leader_hint` set to `endpoint`.
/// Used when a leader-forward fails and the handler serves a stale
/// local read with the known leader endpoint as the hint.
fn patch_not_leader_hint(ctrl: &mut (Vec<u8>, usize), endpoint: &str) {
    let view = FBKvResponseRef::new(&ctrl.0[ctrl.1..]);
    if !view.valid() {
        return;
    }
    // Rebuild with the hint. We need to re-serialize since flatbuffers
    // are immutable. Read all fields from the existing buffer and
    // rebuild with the hint.
    let mut builder = FlatBufferBuilder::new();
    let error_msg = view.error_msg().map(|s| builder.create_string(s));
    let hint = builder.create_string(endpoint);
    let value = view.value().map(|v| builder.create_vector(v));
    let args = FBKvResponseArgs {
        id: view.request_id().unwrap_or(0),
        rpc_create_nano: 0,
        ret_code: view.ret_code(),
        error_msg,
        version: 1,
        ok: view.ok(),
        revision: view.revision(),
        not_found: view.not_found(),
        not_leader_hint: Some(hint),
        request_id: view.request_id().unwrap_or(0),
        request_create_ms: 0,
        value,
        read_slot: view.read_slot(),
        safe_slot: view.safe_slot(),
    };
    let fb = FBKvResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    {
        let (v, h) = builder.collapse();
        *ctrl = (v, h);
    }
}

fn patch_scan_not_leader_hint(ctrl: &mut (Vec<u8>, usize), endpoint: &str) {
    let view = FBKvScanResponseRef::new(&ctrl.0[ctrl.1..]);
    if !view.valid() {
        return;
    }
    let mut builder = FlatBufferBuilder::new();
    let error_msg = view.error_msg().map(|s| builder.create_string(s));
    let hint = builder.create_string(endpoint);
    // Rebuild items from the existing buffer.
    let items_vec: Vec<_> = view
        .items()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    let key = if let Some(k) = item.key() {
                        builder.create_vector(k.bytes())
                    } else {
                        builder.create_vector::<u8>(&[])
                    };
                    let value = if let Some(v) = item.value() {
                        builder.create_vector(v.bytes())
                    } else {
                        builder.create_vector::<u8>(&[])
                    };
                    FBKvScanItem::create(
                        &mut builder,
                        &FBKvScanItemArgs {
                            key: Some(key),
                            value: Some(value),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let items = if items_vec.is_empty() {
        None
    } else {
        Some(builder.create_vector(&items_vec))
    };
    let args = FBKvScanResponseArgs {
        id: view.request_id().unwrap_or(0),
        rpc_create_nano: 0,
        ret_code: view.ret_code(),
        error_msg,
        version: 1,
        ok: view.ok(),
        truncated: view.truncated(),
        items,
        request_id: view.request_id().unwrap_or(0),
        request_create_ms: 0,
        read_slot: view.read_slot(),
        not_leader_hint: Some(hint),
        count: view.count(),
        timed_out: view.timed_out(),
    };
    let fb = FBKvScanResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    {
        let (v, h) = builder.collapse();
        *ctrl = (v, h);
    }
}

fn patch_journal_scan_not_leader_hint(ctrl: &mut (Vec<u8>, usize), endpoint: &str) {
    let view = FBKvJournalScanResponseRef::new(&ctrl.0[ctrl.1..]);
    if !view.valid() {
        return;
    }
    let mut builder = FlatBufferBuilder::new();
    let error_msg = view.error_msg().map(|s| builder.create_string(s));
    let hint = builder.create_string(endpoint);
    let ops_vec: Vec<_> = view
        .ops()
        .map(|ops| {
            ops.iter()
                .map(|op| {
                    let key = if let Some(k) = op.key() {
                        builder.create_vector(k.bytes())
                    } else {
                        builder.create_vector::<u8>(&[])
                    };
                    let value = if let Some(v) = op.value() {
                        builder.create_vector(v.bytes())
                    } else {
                        builder.create_vector::<u8>(&[])
                    };
                    FBKvJournalOp::create(
                        &mut builder,
                        &FBKvJournalOpArgs {
                            key: Some(key),
                            value: Some(value),
                            is_delete: op.is_delete(),
                            slot: op.slot(),
                        },
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let ops = if ops_vec.is_empty() {
        None
    } else {
        Some(builder.create_vector(&ops_vec))
    };
    let args = FBKvJournalScanResponseArgs {
        id: view.request_id().unwrap_or(0),
        rpc_create_nano: 0,
        ret_code: view.ret_code(),
        error_msg,
        version: 1,
        ok: view.ok(),
        ops,
        truncated: view.truncated(),
        last_op_slot: view.last_op_slot(),
        read_slot: view.read_slot(),
        not_leader_hint: Some(hint),
        request_id: view.request_id().unwrap_or(0),
        request_create_ms: 0,
    };
    let fb = FBKvJournalScanResponse::create(&mut builder, &args);
    builder.finish(fb, None);
    {
        let (v, h) = builder.collapse();
        *ctrl = (v, h);
    }
}

// ── Zero-copy response submit helper ──────────────────────────────

/// Submit a response from a collapsed `FlatBufferBuilder` (zero-copy).
/// `ctrl` is the `(Vec<u8>, head)` tuple from `builder.collapse()` — the
/// finished flatbuffer data is at `ctrl.0[ctrl.1..]`. The Vec allocation
/// is wrapped as an external C++ Buffer (no copy); C++ uses it directly
/// for the `OutFrame` and frees it when the write completes.
fn submit_fb_response(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    ctrl: (Vec<u8>, usize),
    msg_type: u16,
    req_id: u64,
) {
    let buf = Buffer::from_vec_offset(ctrl.0, ctrl.1);
    if buf.is_null_handle() {
        // Empty control — submit with raw bytes path.
        unsafe {
            let _ = server.submit_response(conn_handle, &[], None, msg_type, req_id);
        }
        return;
    }
    unsafe {
        let _ = server.submit_response_buffer(conn_handle, buf, None, msg_type, req_id);
    }
}

// ── Submit-error helpers ─────────────────────────────────────────

fn submit_kv_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBKvResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        version: 0,
        ok: false,
        revision: 0,
        not_found: false,
        not_leader_hint: None,
        request_id: req_id,
        request_create_ms: 0,
        value: None,
        read_slot: 0,
        safe_slot: 0,
    };
    let resp = FBKvResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

fn submit_scan_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBKvScanResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        version: 0,
        ok: false,
        truncated: false,
        items: None,
        request_id: req_id,
        request_create_ms: 0,
        read_slot: 0,
        not_leader_hint: None,
        count: 0,
        timed_out: false,
    };
    let resp = FBKvScanResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

fn submit_journal_scan_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBKvJournalScanResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        version: 0,
        ok: false,
        ops: None,
        truncated: false,
        last_op_slot: 0,
        read_slot: 0,
        not_leader_hint: None,
        request_id: req_id,
        request_create_ms: 0,
    };
    let resp = FBKvJournalScanResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

fn submit_create_snapshot_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBCreateSnapshotResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        ok: false,
        snapshot_handle: 0,
        at_slot: 0,
        not_leader_hint: None,
    };
    let resp = FBCreateSnapshotResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

fn submit_list_snapshots_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = crate::rpc::FBListSnapshotsResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        ok: false,
        snapshots: None,
    };
    let resp = crate::rpc::FBListSnapshotsResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

fn submit_snapshot_scan_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBSnapshotScanResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        ok: false,
        truncated: false,
        items: None,
    };
    let resp = FBSnapshotScanResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

fn submit_release_snapshot_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvClientRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBReleaseSnapshotResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        ok: false,
    };
    let resp = FBReleaseSnapshotResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let (vec, head) = builder.collapse();
    submit_fb_response(server, conn_handle, (vec, head), msg_type, req_id);
}

// ── Endpoint parsing (shared with px_rpc_transport) ──────────────

fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

fn parse_endpoint(endpoint: &str) -> Result<(String, i32), String> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let (host, port_str) = without_scheme
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid endpoint: {endpoint}"))?;
    let port: i32 = port_str
        .parse()
        .map_err(|_| format!("invalid port in endpoint: {endpoint}"))?;
    Ok((host.to_string(), port))
}
