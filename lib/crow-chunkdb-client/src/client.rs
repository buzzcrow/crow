// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkdbClient` — client library for CROW chunkdb gRPC operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all chunkdb
//! instances from the service registry, populates a `DashMap` cache
//! (`instance_id -> rpc_endpoint`). On cache miss, lazily refreshes.
//! Channel pool: `DashMap<String, Channel>` per endpoint.
//! Retry: exponential backoff on transient errors (`Unavailable`,
//! `DeadlineExceeded`), up to `max_retries`.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tonic::transport::Channel;

use crow_kv_client::{RangeBindingClient, ServiceRegistryClient};
use crow_protocol::chunkdb::rpc::chunkdb_service_client::ChunkdbServiceClient;
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    ListChunksRequest, ListChunksResponse, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crow_protocol::common::ChunkId;
use crow_protocol::InstanceId;

use crate::{ChunkdbClientError, ChunkdbRpcTransport, Result};

/// Retry configuration for transient errors.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(50),
        }
    }
}

/// Client for CROW chunkdb gRPC operations.
pub struct ChunkdbClient {
    svc: ServiceRegistryClient,
    /// `instance_id -> rpc_endpoint` cache.
    endpoint_cache: DashMap<InstanceId, String>,
    /// `rpc_endpoint -> Channel` pool.
    channels: DashMap<String, Channel>,
    retry: RetryConfig,
    /// Optional range binding client for R99 sharded mode. When
    /// present, chunk IDs are routed to the owning instance. When
    /// `None`, falls back to "any instance" (v1 behavior).
    range_binding: Option<RangeBindingClient>,
    /// Optional crow-rpc transport (R116). When set via
    /// `with_rpc_transport`, all 8 RPCs send via crow-rpc instead of
    /// tonic. The retry/routing/`NotMyRange` logic is unchanged — only
    /// the wire send changes.
    rpc_transport: Option<Arc<ChunkdbRpcTransport>>,
}

impl ChunkdbClient {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            channels: DashMap::new(),
            retry: RetryConfig::default(),
            range_binding: None,
            rpc_transport: None,
        }
    }

    /// Override the default retry config.
    #[must_use]
    pub fn with_retry_config(svc: ServiceRegistryClient, retry: RetryConfig) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            channels: DashMap::new(),
            retry,
            range_binding: None,
            rpc_transport: None,
        }
    }

    /// Enable R99 range-based routing. When set, chunk IDs are routed
    /// to the owning chunkdb instance via the `RangeBindingClient`.
    #[must_use]
    pub fn with_range_binding(mut self, binding: RangeBindingClient) -> Self {
        self.range_binding = Some(binding);
        self
    }

    /// Switch the client to use crow-rpc (R116) for chunkdb operations.
    /// When set, all 8 RPCs send via the transport instead of tonic.
    /// The retry/routing/`NotMyRange` logic is unchanged.
    #[must_use]
    pub fn with_rpc_transport(mut self, transport: Arc<ChunkdbRpcTransport>) -> Self {
        self.rpc_transport = Some(transport);
        self
    }

    /// Eager warm: read all chunkdb instances, populate the endpoint cache.
    pub async fn refresh_endpoints(&self) -> Result<()> {
        let instances = self
            .svc
            .read_all_instances("chunkdb")
            .await
            .map_err(|e| ChunkdbClientError::Unreachable(format!("read_all_instances: {e}")))?;
        for (id, value) in instances {
            self.endpoint_cache.insert(id, value.rpc_endpoint);
        }
        Ok(())
    }

    /// Get the first cached endpoint (or refresh + pick first).
    async fn first_endpoint(&self) -> Result<String> {
        if let Some(entry) = self.endpoint_cache.iter().next() {
            return Ok(entry.value().clone());
        }
        self.refresh_endpoints().await?;
        self.endpoint_cache
            .iter()
            .next()
            .map(|e| e.value().clone())
            .ok_or_else(|| ChunkdbClientError::Unreachable("no chunkdb instances registered".into()))
    }

    /// Get or create a gRPC channel for the given endpoint.
    fn channel_for(&self, endpoint: &str) -> Result<Channel> {
        if let Some(ch) = self.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let ch = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ChunkdbClientError::Unreachable(format!("invalid endpoint {endpoint}: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .connect_lazy();
        self.channels.insert(endpoint.to_string(), ch.clone());
        Ok(ch)
    }

    /// Get or create a gRPC client (any registered instance).
    async fn client(&self) -> Result<ChunkdbServiceClient<Channel>> {
        let endpoint = self.first_endpoint().await?;
        let channel = self.channel_for(&endpoint)?;
        Ok(ChunkdbServiceClient::new(channel))
    }

    /// Get or create a gRPC client for the chunk ID's owning instance.
    /// Falls back to `client()` (any instance) when range binding is
    /// not configured or routing fails.
    async fn client_for_chunk(&self, chunk_id: Option<&ChunkId>) -> Result<ChunkdbServiceClient<Channel>> {
        if let Some(binding) = &self.range_binding {
            if let Some(id) = chunk_id {
                if let Ok(b) = binding.route(id).await {
                    let channel = self.channel_for(&b.rpc_endpoint)?;
                    return Ok(ChunkdbServiceClient::new(channel));
                }
                // Refresh failed or unbound — fall back to any instance.
            }
        }
        self.client().await
    }

    /// Resolve the gRPC endpoint string for the chunk ID's owning
    /// instance (crow-rpc path). Falls back to `first_endpoint` (any
    /// instance) when range binding is not configured or routing fails.
    async fn endpoint_for_chunk(&self, chunk_id: Option<&ChunkId>) -> Result<String> {
        if let Some(binding) = &self.range_binding {
            if let Some(id) = chunk_id {
                if let Ok(b) = binding.route(id).await {
                    return Ok(b.rpc_endpoint);
                }
            }
        }
        self.first_endpoint().await
    }

    /// Execute an RPC with retry on transient errors.
    /// `build_req` produces a fresh request clone for each attempt.
    /// `chunk_id` is used for range-based routing when available.
    async fn with_retry<Req, F, Fut, T>(
        &self,
        chunk_id: Option<&ChunkId>,
        build_req: impl Fn() -> Req,
        op: F,
    ) -> Result<T>
    where
        Req: Send,
        F: Fn(ChunkdbServiceClient<Channel>, Req) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>> + Send,
    {
        let mut attempts = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            let client = self.client_for_chunk(chunk_id).await?;
            let req = build_req();
            match op(client, req).await {
                Ok(value) => return Ok(value),
                Err(status) => {
                    let err = crate::from_status(&status);
                    if !err.is_transient() || attempts >= self.retry.max_retries {
                        return Err(err);
                    }
                    attempts += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                    let _ = self.refresh_endpoints().await;
                    // On NotMyRange, refresh the binding cache from
                    // group-0 and re-route so the next attempt hits
                    // the correct instance. The server only signals
                    // "not my range" — it does not carry the owning
                    // endpoint.
                    if matches!(err, ChunkdbClientError::NotMyRange(_)) {
                        if let Some(binding) = &self.range_binding {
                            if let Some(id) = chunk_id {
                                let _ = binding.refresh_and_route(id).await;
                            } else {
                                let _ = binding.refresh().await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Execute a crow-rpc call with retry on transient errors. Mirrors
    /// `with_retry` but uses the crow-rpc transport instead of a tonic
    /// channel. `op` receives an owned `Arc<ChunkdbRpcTransport>` +
    /// `String` endpoint to avoid lifetime issues with async closures.
    async fn with_rpc_retry<T, F, Fut>(&self, chunk_id: Option<&ChunkId>, op: F) -> Result<T>
    where
        F: Fn(Arc<ChunkdbRpcTransport>, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let transport = Arc::clone(
            self.rpc_transport
                .as_ref()
                .expect("with_rpc_retry called without rpc_transport"),
        );
        let mut attempts = 0u32;
        let mut backoff = self.retry.initial_backoff;
        loop {
            let endpoint = self.endpoint_for_chunk(chunk_id).await?;
            match op(Arc::clone(&transport), endpoint).await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    if !err.is_transient() || attempts >= self.retry.max_retries {
                        return Err(err);
                    }
                    attempts += 1;
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                    let _ = self.refresh_endpoints().await;
                    if matches!(err, ChunkdbClientError::NotMyRange(_)) {
                        if let Some(binding) = &self.range_binding {
                            if let Some(id) = chunk_id {
                                let _ = binding.refresh_and_route(id).await;
                            } else {
                                let _ = binding.refresh().await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Allocate a new chunk.
    pub async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| async move {
                t.send_allocate_chunk(&ep, &req).await
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req,
                |mut c, r| async move { c.allocate_chunk(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// Append strips to an existing chunk.
    pub async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| async move {
                t.send_append_chunk(&ep, &req).await
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req,
                |mut c, r| async move { c.append_chunk(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// Query a chunk by ID.
    pub async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| async move {
                t.send_query_chunk(&ep, &req).await
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req,
                |mut c, r| async move { c.query_chunk(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// Seal a chunk.
    pub async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| async move {
                t.send_seal_chunk(&ep, &req).await
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req,
                |mut c, r| async move { c.seal_chunk(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// Delete a chunk.
    pub async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| async move {
                t.send_delete_chunk(&ep, &req).await
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req,
                |mut c, r| async move { c.delete_chunk(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// Delete a range within a chunk.
    pub async fn delete_chunk_range(&self, req: DeleteChunkRangeRequest) -> Result<DeleteChunkRangeResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| async move {
                t.send_delete_chunk_range(&ep, &req).await
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req,
                |mut c, r| async move { c.delete_chunk_range(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// Update a single strip within a chunk.
    pub async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        let chunk_id = req.chunk_id;
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
                let req = req.clone();
                async move { t.send_update_chunk_strip(&ep, &req).await }
            })
            .await
        } else {
            self.with_retry(
                chunk_id.as_ref(),
                || req.clone(),
                |mut c, r| async move { c.update_chunk_strip(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }

    /// List chunks with pagination.
    pub async fn list_chunks(&self, req: ListChunksRequest) -> Result<ListChunksResponse> {
        if self.rpc_transport.is_some() {
            self.with_rpc_retry(None, |t, ep| async move { t.send_list_chunks(&ep, &req).await })
                .await
        } else {
            self.with_retry(
                None,
                || req,
                |mut c, r| async move { c.list_chunks(r).await.map(tonic::Response::into_inner) },
            )
            .await
        }
    }
}
