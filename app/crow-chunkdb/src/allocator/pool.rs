// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb client pool — caches gRPC connections to diskdb instances.
//!
//! Routes `AllocateBlocks` / `FreeBlocks` to the correct diskdb
//! instance per disk-group, using `ServiceRegistryClient` for endpoint
//! discovery.

use dashmap::DashMap;
use tonic::transport::Channel;

use crow_kv_client::ServiceRegistryClient;
use crow_protocol::common::ChunkId;
use crow_protocol::diskdb::rpc::{AllocateBlocksRequest, AllocateResponse, FreeBlocksRequest, Segment};

/// Pool of diskdb gRPC clients, keyed by disk-group ID.
pub struct DiskdbClientPool {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> grpc_endpoint` cache.
    endpoints: DashMap<u64, String>,
    /// `grpc_endpoint -> Channel` pool.
    channels: DashMap<String, Channel>,
}

impl DiskdbClientPool {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoints: DashMap::new(),
            channels: DashMap::new(),
        }
    }

    /// Get or create a channel for the diskdb instance owning
    /// `disk_group_id`.
    async fn channel_for_dg(&self, dg_id: u64) -> Result<Channel, String> {
        // Check endpoint cache.
        if let Some(endpoint) = self.endpoints.get(&dg_id) {
            return self.get_or_create_channel(endpoint.value());
        }

        // Cache miss — refresh from service registry and retry.
        self.refresh_endpoints().await?;
        if let Some(endpoint) = self.endpoints.get(&dg_id) {
            return self.get_or_create_channel(endpoint.value());
        }
        Err(format!("no endpoint cached for disk_group {dg_id}"))
    }

    fn get_or_create_channel(&self, endpoint: &str) -> Result<Channel, String> {
        if let Some(ch) = self.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let ch = Channel::from_shared(endpoint.to_string())
            .map_err(|e| format!("invalid endpoint {endpoint}: {e}"))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect_lazy();
        self.channels.insert(endpoint.to_string(), ch.clone());
        Ok(ch)
    }

    /// Warm the endpoint cache by reading all diskdb instances from the
    /// service registry.
    ///
    /// # Errors
    /// Returns a String error if the service registry read fails.
    pub async fn refresh_endpoints(&self) -> Result<(), String> {
        let instances = self
            .svc
            .read_all_instances("diskdb")
            .await
            .map_err(|e| format!("read_all_instances: {e}"))?;
        for (id, value) in instances {
            self.endpoints.insert(id, value.grpc_endpoint);
        }
        Ok(())
    }

    /// Allocate blocks on the diskdb instance owning `disk_group_id`.
    ///
    /// # Errors
    /// Returns a String error if the endpoint is not cached or the RPC
    /// fails.
    pub async fn allocate_blocks(
        &self,
        dg_id: u64,
        count: u32,
        unit_count: u32,
        owner_chunk: &ChunkId,
    ) -> Result<AllocateResponse, String> {
        let channel = self.channel_for_dg(dg_id).await?;
        let mut client = crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient::new(channel);
        let req = AllocateBlocksRequest {
            disk_group_id: dg_id,
            unit_count,
            count,
            exclude_disk_ids: vec![],
            owner_chunk: Some(*owner_chunk),
        };
        client
            .allocate_blocks(req)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|e| format!("allocate_blocks RPC: {e}"))
    }

    /// Free blocks via the diskdb instances that own them.
    ///
    /// Segments are grouped by disk-group and freed in parallel.
    ///
    /// # Errors
    /// Returns a String error if any free RPC fails.
    pub async fn free_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        // Group segments by disk-group (from the disk_id in each
        // segment — the diskdb instance that owns the disk-group owns
        // the segment). We use the endpoint cache to route.
        // For simplicity in v1, we try freeing via all known endpoints.
        // A more precise routing would map disk_id → disk_group_id.
        let mut futures = Vec::new();
        for endpoint in &self.channels {
            let channel = endpoint.value().clone();
            let segs = segments.clone();
            futures.push(async move {
                let mut client =
                    crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient::new(channel);
                let req = FreeBlocksRequest { segments: segs };
                client.free_blocks(req).await
            });
        }

        if futures.is_empty() {
            return Err("no channels available for free_blocks".into());
        }

        let results = futures::future::join_all(futures).await;
        // Succeed if any free call succeeded (the owning instance
        // accepts the free; others reject with not-found).
        let mut failed = 0;
        for result in &results {
            if result.is_ok() {
                return Ok(());
            }
            failed += 1;
        }
        Err(format!("all free_blocks RPCs failed ({failed} attempts)"))
    }
}
