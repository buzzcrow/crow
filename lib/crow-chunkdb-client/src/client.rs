// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkdbClient` — client library for CROW chunkdb gRPC operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all chunkdb
//! instances from the service registry, populates a `DashMap` cache
//! (`instance_id -> grpc_endpoint`). On cache miss, lazily refreshes.
//! Channel pool: `DashMap<String, Channel>` per endpoint.
//!
//! R85 skeleton: method stubs wrap the 8 RPCs. Retry logic lands in R90.

use std::time::Duration;

use dashmap::DashMap;
use tonic::transport::Channel;

use crow_kv_client::ServiceRegistryClient;
use crow_protocol::chunkdb::rpc::chunkdb_service_client::ChunkdbServiceClient;
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    ListChunksRequest, ListChunksResponse, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crow_protocol::InstanceId;

use crate::{ChunkdbClientError, Result};

/// Client for CROW chunkdb gRPC operations.
pub struct ChunkdbClient {
    svc: ServiceRegistryClient,
    /// `instance_id -> grpc_endpoint` cache.
    endpoint_cache: DashMap<InstanceId, String>,
    /// `grpc_endpoint -> Channel` pool.
    channels: DashMap<String, Channel>,
}

impl ChunkdbClient {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            channels: DashMap::new(),
        }
    }

    /// Eager warm: read all chunkdb instances, populate the endpoint cache.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Unreachable` if the service registry read fails.
    pub async fn refresh_endpoints(&self) -> Result<()> {
        let instances = self
            .svc
            .read_all_instances("chunkdb")
            .await
            .map_err(|e| ChunkdbClientError::Unreachable(format!("read_all_instances: {e}")))?;
        for (id, value) in instances {
            self.endpoint_cache.insert(id, value.grpc_endpoint);
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

    /// Allocate a new chunk.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let mut client = self.client().await?;
        client
            .allocate_chunk(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// Append strips to an existing chunk.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let mut client = self.client().await?;
        client
            .append_chunk(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// Query a chunk by ID.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        let mut client = self.client().await?;
        client
            .query_chunk(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// Seal a chunk.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let mut client = self.client().await?;
        client
            .seal_chunk(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// Delete a chunk.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let mut client = self.client().await?;
        client
            .delete_chunk(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// Delete a range within a chunk.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn delete_chunk_range(&self, req: DeleteChunkRangeRequest) -> Result<DeleteChunkRangeResponse> {
        let mut client = self.client().await?;
        client
            .delete_chunk_range(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// Update a single strip within a chunk.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        let mut client = self.client().await?;
        client
            .update_chunk_strip(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }

    /// List chunks with pagination.
    ///
    /// # Errors
    /// Returns `ChunkdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn list_chunks(&self, req: ListChunksRequest) -> Result<ListChunksResponse> {
        let mut client = self.client().await?;
        client
            .list_chunks(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| ChunkdbClientError::Rpc(e.to_string()))
    }
}
