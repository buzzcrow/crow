// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only client facades that preserve the former tonic
//! `KvServiceClient`/`PxServiceClient` call shape (`.put(req).await.
//! expect(..).into_inner()`) but send over the crowdb-rpc transports
//! (`KvRpcTransport` / `PxRpcTransport`). They exist so the many
//! integration-test call sites could be migrated off the deleted tonic
//! clients with minimal churn: the request/response structs are
//! unchanged, only the wire transport changed.

#![allow(dead_code)]
#![allow(clippy::unused_async)]

use std::sync::Arc;

use crowdb_kv::paxos::roles::{PxBallot, PxPrepareReply};
use crowdb_kv::rpc::PxRpcTransport;
use crowdb_kv::rpc::{
    KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvResponse, KvScanRequest, KvScanResponse,
    KvSetRequest, PromiseResponse, ReadMode,
};
use crowdb_kv_client::KvRpcTransport;

/// Status-like error returned by the facades. Implements `Debug` so
/// `Result<_, TestRpcStatus>::expect(..)` compiles, and exposes
/// `message()` so retry loops that did `status.message()` compile.
#[derive(Debug)]
pub struct TestRpcStatus {
    msg: String,
}

impl TestRpcStatus {
    #[must_use]
    pub fn message(&self) -> &str {
        &self.msg
    }
}

fn status(e: impl std::fmt::Display) -> TestRpcStatus {
    TestRpcStatus { msg: e.to_string() }
}

/// Mimics `tonic::Response<T>`: the test call sites call `.into_inner()`
/// to unwrap the response payload.
pub struct TestResponse<T>(pub T);

impl<T> TestResponse<T> {
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

fn read_mode_from_i32(v: i32) -> ReadMode {
    if v == ReadMode::MinSlot as i32 {
        ReadMode::MinSlot
    } else {
        ReadMode::Linearizable
    }
}

/// crowdb-rpc facade for the former `KvServiceClient<Channel>`. Construct
/// with `TestKvClient::connect(endpoint).await` (the `.await` mirrors the
/// old `KvServiceClient::connect(..).await` shape), or
/// `TestKvClient::with_transport(transport, endpoint)` to share a
/// single `KvRpcTransport` (and its underlying `RpcServer`/`RpcClient`)
/// across many clients — avoiding per-call `RpcServer` creation in
/// crash/restart tests that issue hundreds of sequential reads.
pub struct TestKvClient {
    transport: Arc<KvRpcTransport>,
    endpoint: String,
}

impl TestKvClient {
    #[allow(clippy::unused_async_trait_impl)]
    pub async fn connect(endpoint: String) -> Self {
        Self {
            transport: Arc::new(KvRpcTransport::new()),
            endpoint,
        }
    }

    /// Create a client that shares an existing transport. The transport
    /// holds the `RpcServer` (epoll loop) and `RpcClient` (reaper) —
    /// sharing them avoids spawning a new event-loop thread per RPC.
    #[must_use]
    pub fn with_transport(transport: Arc<KvRpcTransport>, endpoint: String) -> Self {
        Self { transport, endpoint }
    }

    pub async fn put(&self, req: KvSetRequest) -> Result<TestResponse<KvResponse>, TestRpcStatus> {
        let r = self
            .transport
            .send_put(
                &self.endpoint,
                &req.key,
                &req.value,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
                req.group_id,
            )
            .await
            .map_err(status)?;
        Ok(TestResponse(r))
    }

    pub async fn get(&self, req: KvGetRequest) -> Result<TestResponse<KvResponse>, TestRpcStatus> {
        self.get_with_forwarded(req, false).await
    }

    pub async fn get_with_forwarded(
        &self,
        req: KvGetRequest,
        forwarded: bool,
    ) -> Result<TestResponse<KvResponse>, TestRpcStatus> {
        let r = self
            .transport
            .send_get_with_forwarded(
                &self.endpoint,
                &req.key,
                req.request_id,
                req.request_create_ms,
                req.group_id,
                read_mode_from_i32(req.read_mode),
                req.min_slot,
                forwarded,
            )
            .await
            .map_err(status)?;
        Ok(TestResponse(r))
    }

    pub async fn delete(&self, req: KvDeleteRequest) -> Result<TestResponse<KvResponse>, TestRpcStatus> {
        let r = self
            .transport
            .send_delete(
                &self.endpoint,
                &req.key,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
                req.group_id,
            )
            .await
            .map_err(status)?;
        Ok(TestResponse(r))
    }

    pub async fn batch_write(
        &self,
        req: KvBatchWriteRequest,
    ) -> Result<TestResponse<KvResponse>, TestRpcStatus> {
        let r = self
            .transport
            .send_batch_write(
                &self.endpoint,
                &req.items,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
                req.group_id,
            )
            .await
            .map_err(status)?;
        Ok(TestResponse(r))
    }

    pub async fn scan(&self, req: KvScanRequest) -> Result<TestResponse<KvScanResponse>, TestRpcStatus> {
        let r = self
            .transport
            .send_scan(
                &self.endpoint,
                &req.prefix,
                &req.start_after,
                &req.end_key,
                req.limit,
                req.request_id,
                req.request_create_ms,
                req.group_id,
                read_mode_from_i32(req.read_mode),
                req.min_slot,
                req.keys_only,
                req.count_only,
                req.deadline_ms,
            )
            .await
            .map_err(status)?;
        Ok(TestResponse(r))
    }
}

/// crowdb-rpc facade for the former `PxServiceClient<Channel>`. Only the
/// `prepare` path is used by the surviving tests; the retired unary
/// `accept` test is `#[ignore]`d (see `paxos_error_test`).
pub struct TestPxClient {
    transport: PxRpcTransport,
    endpoint: String,
}

impl TestPxClient {
    #[allow(clippy::unused_async_trait_impl)]
    pub async fn connect(endpoint: String) -> Self {
        Self {
            transport: PxRpcTransport::new(),
            endpoint,
        }
    }

    pub async fn prepare(
        &self,
        req: crowdb_kv::rpc::PrepareRequest,
    ) -> Result<TestResponse<PromiseResponse>, TestRpcStatus> {
        let reply = self
            .transport
            .send_prepare(
                &self.endpoint,
                req.slot,
                PxBallot::new(req.round, req.leader_id),
                req.term,
                req.group_id,
                req.membership_epoch,
            )
            .await
            .map_err(status)?;
        let resp = match reply {
            PxPrepareReply::Promised { slot, .. } => PromiseResponse {
                slot,
                rejected: false,
                ..PromiseResponse::default()
            },
            PxPrepareReply::Rejected {
                slot,
                current_promised,
            } => PromiseResponse {
                slot,
                rejected: true,
                rejected_round: current_promised.round,
                rejected_leader_id: current_promised.leader_id,
                ..PromiseResponse::default()
            },
            PxPrepareReply::EpochMismatch { responder_epoch } => PromiseResponse {
                rejected: true,
                epoch_mismatch: true,
                membership_epoch: responder_epoch,
                ..PromiseResponse::default()
            },
            PxPrepareReply::TermStale { slot, new_term } => PromiseResponse {
                slot,
                rejected: true,
                term_stale: true,
                term: new_term,
                ..PromiseResponse::default()
            },
        };
        Ok(TestResponse(resp))
    }
}
