// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc)]
#![allow(dead_code)] // Wired in Phase 7 (CrowkvClient transport selection)

//! crow-rpc client transport for the KV client-facing service (R117
//! migration). Mirrors `px_rpc_transport.rs` (consensus side): builds
//! flatbuffer requests, sends via `RpcClient::call`, awaits
//! `CallFuture`, parses flatbuffer responses via the zero-copy `Ref`
//! wrappers, and maps them to the existing response types
//! (`KvResponse`, `KvScanResponse`, `KvJournalScanResponse`) so the
//! retry/topology/`NotLeaderHint` logic in `client.rs` is unchanged.
//!
//! The only transport path (the former gRPC `ConnectionPool` was removed).
//! window; `CrowkvClient` selects the transport via
//! `with_rpc_transport`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use flatbuffers::FlatBufferBuilder;

use crow_kv::rpc::{
    CreateSnapshotResponse, KvErrorCode, KvJournalOp, KvJournalScanResponse, KvResponse, KvScanItem,
    KvScanResponse, ListSnapshotsResponse, ReadMode, ReleaseSnapshotResponse, SnapshotInfo,
    SnapshotScanResponse,
};
use crow_protocol::fb::FBMsgType;
use crow_protocol::fb_wrappers::kv_client::{
    FBCreateSnapshotResponseRef, FBKvJournalScanResponseRef, FBKvResponseRef, FBKvScanResponseRef,
    FBListSnapshotsResponseRef, FBReleaseSnapshotResponseRef, FBSnapshotScanResponseRef,
};
use crow_protocol::kv_client_fb::{
    FBCreateSnapshotRequest, FBCreateSnapshotRequestArgs, FBKvBatchItem, FBKvBatchItemArgs,
    FBKvBatchWriteRequest, FBKvBatchWriteRequestArgs, FBKvClientRetCode, FBKvDeleteRequest,
    FBKvDeleteRequestArgs, FBKvGetRequest, FBKvGetRequestArgs, FBKvJournalScanRequest,
    FBKvJournalScanRequestArgs, FBKvScanRequest, FBKvScanRequestArgs, FBKvSetRequest, FBKvSetRequestArgs,
    FBListSnapshotsRequest, FBListSnapshotsRequestArgs, FBReadMode, FBReleaseSnapshotRequest,
    FBReleaseSnapshotRequestArgs, FBSnapshotScanRequest, FBSnapshotScanRequestArgs,
};
use crow_rpc_ffi::{Buffer, Connection, RpcClient, RpcError, RpcServer};

use crate::error::{Error, Result};

/// crow-rpc transport for the KV client-facing service. Holds the
/// client-side `RpcServer` (manages connections), `RpcClient`
/// (request/response correlation), and a `Connection` cache per
/// endpoint.
pub struct KvRpcTransport {
    server: Arc<RpcServer>,
    rpc: Arc<RpcClient>,
    connections: DashMap<String, Connection>,
    next_req_id: AtomicU64,
}

impl std::fmt::Debug for KvRpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvRpcTransport")
            .field("next_req_id", &self.next_req_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl KvRpcTransport {
    /// Create a new crow-rpc transport. The `RpcServer` is the
    /// client-side transport — it does not listen but is used to
    /// establish connections to remote endpoints.
    #[must_use]
    pub fn new() -> Self {
        let server = Arc::new(RpcServer::new(None));
        server.start();
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
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
    fn conn_for(&self, grpc_endpoint: &str) -> Result<Connection> {
        let normalized = normalize_endpoint(grpc_endpoint);
        if let Some(conn) = self.connections.get(&normalized) {
            return Ok(conn.clone());
        }
        let (host, port) = parse_endpoint(&normalized).map_err(|e| Error::InvalidEndpoint {
            endpoint: grpc_endpoint.to_string(),
            reason: e,
        })?;
        let conn = self.server.connect(&host, port).map_err(|e| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: format!("rpc connect to {host}:{port}: {e:?}"),
        })?;
        self.rpc.attach(&conn);
        self.connections.insert(normalized, conn.clone());
        Ok(conn)
    }

    /// Send a `Put` request via crow-rpc. Returns the
    /// `KvResponse` so the caller's retry logic is unchanged.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_put(
        &self,
        grpc_endpoint: &str,
        key: &[u8],
        value: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
    ) -> Result<KvResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_key = builder.create_vector(key);
        let fb_value = builder.create_vector(value);
        let args = FBKvSetRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            key: Some(fb_key),
            value: Some(fb_value),
            seq,
            ttl_ms: 0,
            client_id,
            request_id,
            request_create_ms,
            group_id,
            forwarded: false,
        };
        let req = FBKvSetRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvSetRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "put response missing control buffer".into(),
        })?;
        parse_kv_response(ctrl.bytes())
    }

    /// Send a `Get` request via crow-rpc.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_get(
        &self,
        grpc_endpoint: &str,
        key: &[u8],
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: u64,
    ) -> Result<KvResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_key = builder.create_vector(key);
        let args = FBKvGetRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            key: Some(fb_key),
            request_id,
            request_create_ms,
            group_id,
            read_mode: read_mode_to_fb(read_mode),
            min_slot,
            forwarded: false,
        };
        let req = FBKvGetRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvGetRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "get response missing control buffer".into(),
        })?;
        parse_kv_response(ctrl.bytes())
    }

    /// Send a `Delete` request via crow-rpc.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_delete(
        &self,
        grpc_endpoint: &str,
        key: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
    ) -> Result<KvResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_key = builder.create_vector(key);
        let args = FBKvDeleteRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            key: Some(fb_key),
            seq,
            client_id,
            request_id,
            request_create_ms,
            group_id,
            forwarded: false,
        };
        let req = FBKvDeleteRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvDeleteRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "delete response missing control buffer".into(),
        })?;
        parse_kv_response(ctrl.bytes())
    }

    /// Send a `BatchWrite` request via crow-rpc.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_batch_write(
        &self,
        grpc_endpoint: &str,
        items: &[crow_kv::rpc::KvBatchItem],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
    ) -> Result<KvResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let item_offsets: Vec<_> = items
            .iter()
            .map(|item| {
                let key = builder.create_vector(&item.key);
                let value = builder.create_vector(&item.value);
                FBKvBatchItem::create(
                    &mut builder,
                    &FBKvBatchItemArgs {
                        key: Some(key),
                        value: Some(value),
                        is_delete: item.is_delete,
                    },
                )
            })
            .collect();
        let fb_items = builder.create_vector(&item_offsets);
        let args = FBKvBatchWriteRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            items: Some(fb_items),
            seq,
            client_id,
            request_id,
            request_create_ms,
            group_id,
            forwarded: false,
        };
        let req = FBKvBatchWriteRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvBatchWriteRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "batch_write response missing control buffer".into(),
        })?;
        parse_kv_response(ctrl.bytes())
    }

    /// Send a `Scan` request via crow-rpc.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_scan(
        &self,
        grpc_endpoint: &str,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: u64,
        keys_only: bool,
        count_only: bool,
        deadline_ms: u64,
    ) -> Result<KvScanResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_prefix = builder.create_vector(prefix);
        let fb_start_after = builder.create_vector(start_after);
        let fb_end_key = builder.create_vector(end_key);
        let args = FBKvScanRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            prefix: Some(fb_prefix),
            limit,
            request_id,
            request_create_ms,
            group_id,
            read_mode: read_mode_to_fb(read_mode),
            start_after: Some(fb_start_after),
            end_key: Some(fb_end_key),
            min_slot,
            keys_only,
            count_only,
            deadline_ms,
            forwarded: false,
        };
        let req = FBKvScanRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvScanRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "scan response missing control buffer".into(),
        })?;
        parse_scan_response(ctrl.bytes())
    }

    /// Send a `JournalScan` request via crow-rpc.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_journal_scan(
        &self,
        grpc_endpoint: &str,
        min_slot: u64,
        max_slot: u64,
        key_prefix: &[u8],
        limit: u32,
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
        read_mode: ReadMode,
    ) -> Result<KvJournalScanResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_key_prefix = builder.create_vector(key_prefix);
        let args = FBKvJournalScanRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            min_slot,
            max_slot,
            key_prefix: Some(fb_key_prefix),
            limit,
            request_id,
            request_create_ms,
            read_mode: read_mode_to_fb(read_mode),
            forwarded: false,
        };
        let req = FBKvJournalScanRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EKvJournalScanRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "journal_scan response missing control buffer".into(),
        })?;
        parse_journal_scan_response(ctrl.bytes())
    }

    /// Send a `CreateSnapshot` request via crow-rpc.
    pub async fn send_create_snapshot(
        &self,
        grpc_endpoint: &str,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: u64,
    ) -> Result<CreateSnapshotResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBCreateSnapshotRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            group_id,
            read_mode: match read_mode {
                ReadMode::Linearizable => FBReadMode::Linearizable,
                ReadMode::MinSlot => FBReadMode::MinSlot,
            },
            min_slot,
            forwarded: false,
        };
        let req = FBCreateSnapshotRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::ECreateSnapshotRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "create_snapshot response missing control buffer".into(),
        })?;
        let r = FBCreateSnapshotResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(Error::Transport {
                endpoint: grpc_endpoint.to_string(),
                status: "create_snapshot response malformed".into(),
            });
        }
        check_client_ret_code(r.ret_code(), r.error_msg(), grpc_endpoint)?;
        Ok(CreateSnapshotResponse {
            ok: r.ok(),
            error: r.error_msg().unwrap_or_default().to_string(),
            snapshot_handle: r.snapshot_handle(),
            at_slot: r.at_slot(),
            error_code: fb_ret_code_to_kv_error_code(r.ret_code()),
            not_leader_hint: r.not_leader_hint().unwrap_or_default().to_string(),
        })
    }

    /// Send a `ListSnapshots` request via crow-rpc.
    pub async fn send_list_snapshots(
        &self,
        grpc_endpoint: &str,
        group_id: u64,
    ) -> Result<ListSnapshotsResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBListSnapshotsRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            group_id,
        };
        let req = FBListSnapshotsRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EListSnapshotsRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "list_snapshots response missing control buffer".into(),
        })?;
        let r = FBListSnapshotsResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(Error::Transport {
                endpoint: grpc_endpoint.to_string(),
                status: "list_snapshots response malformed".into(),
            });
        }
        check_client_ret_code(r.ret_code(), r.error_msg(), grpc_endpoint)?;
        let snapshots = r
            .snapshots()
            .map(|items| {
                items
                    .iter()
                    .map(|s| SnapshotInfo {
                        snapshot_handle: s.snapshot_handle(),
                        at_slot: s.at_slot(),
                        lease_remaining_ms: s.lease_remaining_ms(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(ListSnapshotsResponse {
            ok: r.ok(),
            error: r.error_msg().unwrap_or_default().to_string(),
            snapshots,
        })
    }

    /// Send a `SnapshotScan` request via crow-rpc.
    pub async fn send_snapshot_scan(
        &self,
        grpc_endpoint: &str,
        snapshot_handle: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
        group_id: u64,
    ) -> Result<SnapshotScanResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let fb_prefix = builder.create_vector(prefix);
        let fb_start_after = builder.create_vector(start_after);
        let args = FBSnapshotScanRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            snapshot_handle,
            prefix: Some(fb_prefix),
            start_after: Some(fb_start_after),
            limit,
            group_id,
        };
        let req = FBSnapshotScanRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::ESnapshotScanRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "snapshot_scan response missing control buffer".into(),
        })?;
        let r = FBSnapshotScanResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(Error::Transport {
                endpoint: grpc_endpoint.to_string(),
                status: "snapshot_scan response malformed".into(),
            });
        }
        check_client_ret_code(r.ret_code(), r.error_msg(), grpc_endpoint)?;
        let items = r
            .items()
            .map(|items| {
                items
                    .iter()
                    .map(|item| KvScanItem {
                        key: Bytes::copy_from_slice(item.key().unwrap_or_default().bytes()),
                        value: Bytes::copy_from_slice(item.value().unwrap_or_default().bytes()),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(SnapshotScanResponse {
            ok: r.ok(),
            error: r.error_msg().unwrap_or_default().to_string(),
            truncated: r.truncated(),
            items,
            error_code: fb_ret_code_to_kv_error_code(r.ret_code()),
        })
    }

    /// Send a `ReleaseSnapshot` request via crow-rpc.
    pub async fn send_release_snapshot(
        &self,
        grpc_endpoint: &str,
        snapshot_handle: u64,
        group_id: u64,
    ) -> Result<ReleaseSnapshotResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(grpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let args = FBReleaseSnapshotRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            snapshot_handle,
            group_id,
        };
        let req = FBReleaseSnapshotRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EReleaseSnapshotRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(rpc_error_to_client)?;
        let resp = fut.await.map_err(rpc_error_to_client)?;
        let ctrl = resp.control.ok_or_else(|| Error::Transport {
            endpoint: grpc_endpoint.to_string(),
            status: "release_snapshot response missing control buffer".into(),
        })?;
        let r = FBReleaseSnapshotResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(Error::Transport {
                endpoint: grpc_endpoint.to_string(),
                status: "release_snapshot response malformed".into(),
            });
        }
        check_client_ret_code(r.ret_code(), r.error_msg(), grpc_endpoint)?;
        Ok(ReleaseSnapshotResponse {
            ok: r.ok(),
            error: r.error_msg().unwrap_or_default().to_string(),
        })
    }

    /// Get or create a `Connection` for the given gRPC endpoint
    /// (public — used by `WatchNotifyClient` for the persistent
    /// connection).
    pub fn get_conn(&self, grpc_endpoint: &str) -> Result<Connection> {
        self.conn_for(grpc_endpoint)
    }

    /// The client-side `RpcServer` (public — used by
    /// `WatchNotifyClient` for handler registration).
    pub fn server(&self) -> &Arc<RpcServer> {
        &self.server
    }

    /// The `RpcClient` (public — used by `WatchNotifyClient` for
    /// fire-and-forget `send`).
    pub fn rpc(&self) -> &Arc<RpcClient> {
        &self.rpc
    }

    /// Allocate a request ID (public — used by `WatchNotifyClient`).
    pub fn alloc_id(&self) -> u64 {
        self.next_id()
    }
}

impl Default for KvRpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

fn check_client_ret_code(code: FBKvClientRetCode, msg: Option<&str>, endpoint: &str) -> Result<()> {
    match code {
        FBKvClientRetCode::Success => Ok(()),
        FBKvClientRetCode::NotLeader => Err(Error::Transport {
            endpoint: endpoint.to_string(),
            status: msg.unwrap_or("not leader").into(),
        }),
        FBKvClientRetCode::Unavailable => Err(Error::Transport {
            endpoint: endpoint.to_string(),
            status: msg.unwrap_or("unavailable").into(),
        }),
        FBKvClientRetCode::JournalScanGcGap => Err(Error::JournalScanGcGap),
        _ => Err(Error::Transport {
            endpoint: endpoint.to_string(),
            status: msg.unwrap_or("internal error").into(),
        }),
    }
}

// ── Response parsing ─────────────────────────────────────────────

/// Parse a `FBKvResponse` flatbuffer into a `KvResponse`.
fn parse_kv_response(buf: &[u8]) -> Result<KvResponse> {
    let r = FBKvResponseRef::new(buf);
    if !r.valid() {
        return Err(Error::Transport {
            endpoint: String::new(),
            status: "kv response malformed".into(),
        });
    }
    let ret_code = r.ret_code();
    let error_msg = r.error_msg().unwrap_or("").to_string();
    // Map JournalScanGcGap to the dedicated error.
    if ret_code == FBKvClientRetCode::JournalScanGcGap {
        return Err(Error::JournalScanGcGap);
    }
    Ok(KvResponse {
        version: 1,
        ok: r.ok(),
        revision: r.revision(),
        error: error_msg,
        not_found: r.not_found(),
        not_leader_hint: r.not_leader_hint().unwrap_or("").to_string(),
        request_id: r.request_id().unwrap_or(0),
        request_create_ms: 0,
        value: Bytes::copy_from_slice(r.value().unwrap_or(&[])),
        read_slot: r.read_slot(),
        safe_slot: r.safe_slot(),
        error_code: fb_ret_code_to_kv_error_code(ret_code),
    })
}

/// Parse a `FBKvScanResponse` flatbuffer into a
/// `KvScanResponse`.
fn parse_scan_response(buf: &[u8]) -> Result<KvScanResponse> {
    let r = FBKvScanResponseRef::new(buf);
    if !r.valid() {
        return Err(Error::Transport {
            endpoint: String::new(),
            status: "scan response malformed".into(),
        });
    }
    let items: Vec<KvScanItem> = r
        .items()
        .map(|v| {
            v.iter()
                .map(|item| KvScanItem {
                    key: Bytes::copy_from_slice(item.key().map_or(&[], |k| k.bytes())),
                    value: Bytes::copy_from_slice(item.value().map_or(&[], |v| v.bytes())),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(KvScanResponse {
        version: 1,
        ok: r.ok(),
        error: r.error_msg().unwrap_or("").to_string(),
        truncated: r.truncated(),
        items,
        request_id: r.request_id().unwrap_or(0),
        request_create_ms: 0,
        read_slot: r.read_slot(),
        not_leader_hint: r.not_leader_hint().unwrap_or("").to_string(),
        error_code: fb_ret_code_to_kv_error_code(r.ret_code()),
        count: r.count(),
        timed_out: r.timed_out(),
    })
}

/// Parse a `FBKvJournalScanResponse` flatbuffer into a
/// `KvJournalScanResponse`.
fn parse_journal_scan_response(buf: &[u8]) -> Result<KvJournalScanResponse> {
    let r = FBKvJournalScanResponseRef::new(buf);
    if !r.valid() {
        return Err(Error::Transport {
            endpoint: String::new(),
            status: "journal_scan response malformed".into(),
        });
    }
    let ret_code = r.ret_code();
    if ret_code == FBKvClientRetCode::JournalScanGcGap {
        return Err(Error::JournalScanGcGap);
    }
    let ops: Vec<KvJournalOp> = r
        .ops()
        .map(|v| {
            v.iter()
                .map(|op| KvJournalOp {
                    key: Bytes::copy_from_slice(op.key().map_or(&[], |k| k.bytes())),
                    value: Bytes::copy_from_slice(op.value().map_or(&[], |v| v.bytes())),
                    is_delete: op.is_delete(),
                    slot: op.slot(),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(KvJournalScanResponse {
        version: 1,
        ok: r.ok(),
        error: r.error_msg().unwrap_or("").to_string(),
        ops,
        truncated: r.truncated(),
        last_op_slot: r.last_op_slot(),
        read_slot: r.read_slot(),
        error_code: fb_ret_code_to_kv_error_code(ret_code),
        not_leader_hint: r.not_leader_hint().unwrap_or("").to_string(),
        request_id: r.request_id().unwrap_or(0),
        request_create_ms: 0,
    })
}

// ── Helpers ──────────────────────────────────────────────────────

fn read_mode_to_fb(mode: ReadMode) -> FBReadMode {
    match mode {
        ReadMode::Linearizable => FBReadMode::Linearizable,
        ReadMode::MinSlot => FBReadMode::MinSlot,
    }
}

fn fb_ret_code_to_kv_error_code(code: FBKvClientRetCode) -> i32 {
    match code {
        FBKvClientRetCode::Success => KvErrorCode::KvErrorNone as i32,
        FBKvClientRetCode::NotLeader => KvErrorCode::KvErrorNotLeader as i32,
        FBKvClientRetCode::Unavailable => KvErrorCode::KvErrorUnavailable as i32,
        FBKvClientRetCode::JournalScanGcGap => KvErrorCode::KvErrorJournalScanGcGap as i32,
        _ => KvErrorCode::KvErrorInternal as i32,
    }
}

fn rpc_error_to_client(e: RpcError) -> Error {
    Error::Transport {
        endpoint: String::new(),
        status: format!("rpc error: {e:?}"),
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

fn parse_endpoint(endpoint: &str) -> std::result::Result<(String, i32), String> {
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
