// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkdbClient` — client library for CROWDB chunkdb operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all chunkdb
//! instances from the service registry, populates a `DashMap` cache
//! (`instance_id -> rpc_endpoint`). On cache miss, lazily refreshes.
//! Retry: exponential backoff on transient errors, up to `max_retries`.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use crowdb_kv_client::{RangeBindingClient, ServiceRegistryClient};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    ListChunksRequest, ListChunksResponse, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::InstanceId;

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

/// Client for CROWDB chunkdb operations via crowdb-rpc.
pub struct ChunkdbClient {
    svc: ServiceRegistryClient,
    /// `instance_id -> rpc_endpoint` cache.
    endpoint_cache: DashMap<InstanceId, String>,
    retry: RetryConfig,
    /// Optional range binding client for R99 sharded mode. When
    /// present, chunk IDs are routed to the owning instance. When
    /// `None`, falls back to "any instance" (v1 behavior).
    range_binding: Option<RangeBindingClient>,
    /// crowdb-rpc transport.
    rpc_transport: Arc<ChunkdbRpcTransport>,
}

impl ChunkdbClient {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient, rpc_transport: Arc<ChunkdbRpcTransport>) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            retry: RetryConfig::default(),
            range_binding: None,
            rpc_transport,
        }
    }

    /// Override the default retry config.
    #[must_use]
    pub fn with_retry_config(
        svc: ServiceRegistryClient,
        retry: RetryConfig,
        rpc_transport: Arc<ChunkdbRpcTransport>,
    ) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            retry,
            range_binding: None,
            rpc_transport,
        }
    }

    /// Enable R99 range-based routing. When set, chunk IDs are routed
    /// to the owning chunkdb instance via the `RangeBindingClient`.
    #[must_use]
    pub fn with_range_binding(mut self, binding: RangeBindingClient) -> Self {
        self.range_binding = Some(binding);
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

    /// Resolve the endpoint string for the chunk ID's owning
    /// instance. Falls back to `first_endpoint` (any instance) when
    /// range binding is not configured or routing fails.
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

    /// Execute a crowdb-rpc call with retry on transient errors.
    async fn with_rpc_retry<T, F, Fut>(&self, chunk_id: Option<&ChunkId>, op: F) -> Result<T>
    where
        F: Fn(Arc<ChunkdbRpcTransport>, String) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let transport = Arc::clone(&self.rpc_transport);
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
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_allocate_chunk(&ep, &req).await }
        })
        .await
    }

    /// Append strips to an existing chunk.
    pub async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_append_chunk(&ep, &req).await }
        })
        .await
    }

    /// Query a chunk by ID.
    pub async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_query_chunk(&ep, &req).await }
        })
        .await
    }

    /// Seal a chunk.
    pub async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_seal_chunk(&ep, &req).await }
        })
        .await
    }

    /// Delete a chunk.
    pub async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_delete_chunk(&ep, &req).await }
        })
        .await
    }

    /// Delete a range within a chunk.
    pub async fn delete_chunk_range(&self, req: DeleteChunkRangeRequest) -> Result<DeleteChunkRangeResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_delete_chunk_range(&ep, &req).await }
        })
        .await
    }

    /// Update a single strip within a chunk.
    pub async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        let chunk_id = req.chunk_id;
        self.with_rpc_retry(chunk_id.as_ref(), |t, ep| {
            let req = req.clone();
            async move { t.send_update_chunk_strip(&ep, &req).await }
        })
        .await
    }

    /// List chunks with pagination.
    pub async fn list_chunks(&self, req: ListChunksRequest) -> Result<ListChunksResponse> {
        self.with_rpc_retry(None, |t, ep| {
            let req = req.clone();
            async move { t.send_list_chunks(&ep, &req).await }
        })
        .await
    }
}
