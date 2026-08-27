// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Test-only client facade that preserves the former tonic
//! `KvServiceClient` call shape (`.put(req).await.expect(..).
//! into_inner()`) but sends over the crow-rpc `KvRpcTransport`.
//! Exists so the process-level e2e tests could be migrated off the
//! deleted tonic client with minimal churn.

#![allow(dead_code)]
#![allow(clippy::unused_async)]

use crow_kv::rpc::{
    KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvResponse, KvScanRequest, KvScanResponse,
    KvSetRequest, ReadMode,
};
use crow_kv_client::KvRpcTransport;

/// Status-like error returned by the facade. Implements `Debug` so
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

/// crow-rpc facade for the former `KvServiceClient<Channel>`. Construct
/// with `TestKvClient::connect(endpoint).await` (the `.await` mirrors the
/// old `KvServiceClient::connect(..).await` shape).
pub struct TestKvClient {
    transport: KvRpcTransport,
    endpoint: String,
}

impl TestKvClient {
    #[allow(clippy::unused_async_trait_impl)]
    pub async fn connect(endpoint: String) -> Self {
        Self {
            transport: KvRpcTransport::new(),
            endpoint,
        }
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
        let r = self
            .transport
            .send_get(
                &self.endpoint,
                &req.key,
                req.request_id,
                req.request_create_ms,
                req.group_id,
                read_mode_from_i32(req.read_mode),
                req.min_slot,
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
