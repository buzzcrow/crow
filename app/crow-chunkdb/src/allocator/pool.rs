// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb client pool — caches gRPC connections to diskdb instances.
//!
//! Routes `AllocateBlocks` / `FreeBlocks` to the correct diskdb
//! instance per disk-group, using `ServiceRegistryClient` for endpoint
//! discovery.

use std::collections::HashMap;

use dashmap::DashMap;
use tonic::transport::Channel;
use tracing::warn;

use crow_kv_client::ServiceRegistryClient;
use crow_protocol::common::{ChunkId, DiskId};
use crow_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CommitBlocksRequest, FreeBlocksRequest, FreeResponse, Segment,
};

/// Pinned boxed future for a `free_blocks` RPC call.
type FreeFut = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = std::result::Result<tonic::Response<FreeResponse>, tonic::Status>>
            + Send,
    >,
>;

/// Pool of diskdb gRPC clients, keyed by disk-group ID.
pub struct DiskdbClientPool {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> grpc_endpoint` cache.
    endpoints: DashMap<u64, String>,
    /// `grpc_endpoint -> Channel` pool.
    channels: DashMap<String, Channel>,
    /// `disk_id -> disk_group_id` reverse lookup cache (GAP-4).
    /// Populated from the topology cache's `DiskGroupEntry` list.
    /// Used for precise `free_blocks` routing.
    disk_id_to_dg: DashMap<DiskId, u64>,
}

impl DiskdbClientPool {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoints: DashMap::new(),
            channels: DashMap::new(),
            disk_id_to_dg: DashMap::new(),
        }
    }

    /// Update the `disk_id → disk_group_id` reverse lookup cache from
    /// a topology snapshot. Called by the topology refresh loop.
    pub fn update_disk_id_lookup(&self, entries: &[crow_protocol::sysdata::DiskGroupEntry]) {
        self.disk_id_to_dg.clear();
        for entry in entries {
            for disk_id in &entry.value.disk_ids {
                self.disk_id_to_dg.insert(*disk_id, entry.dg_id);
            }
        }
    }

    /// Look up the disk-group ID for a disk_id (reverse lookup).
    fn dg_for_disk(&self, disk_id: &DiskId) -> Option<u64> {
        self.disk_id_to_dg.get(disk_id).map(|r| *r)
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
        let normalized = crow_diskdb_client::normalize_endpoint(endpoint);
        if let Some(ch) = self.channels.get(&normalized) {
            return Ok(ch.clone());
        }
        let ch = Channel::from_shared(normalized.clone())
            .map_err(|e| format!("invalid endpoint {normalized}: {e}"))?
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .connect_lazy();
        self.channels.insert(normalized, ch.clone());
        Ok(ch)
    }

    /// Warm the endpoint cache by reading all diskdb instances from the
    /// service registry. Maps each instance's `owned_dg_ids` to its
    /// gRPC endpoint so `channel_for_dg` can route by disk-group ID.
    ///
    /// # Errors
    /// Returns a String error if the service registry read fails.
    pub async fn refresh_endpoints(&self) -> Result<(), String> {
        let instances = self
            .svc
            .read_all_instances("diskdb")
            .await
            .map_err(|e| format!("read_all_instances: {e}"))?;
        for (_id, value) in instances {
            if let Some(ref extra) = value.extra {
                if let Some(ref diskdb) = extra.diskdb {
                    for dg_id in &diskdb.owned_dg_ids {
                        self.endpoints.insert(*dg_id, value.grpc_endpoint.clone());
                    }
                }
            }
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

    /// Commit blocks (mark as permanent) on the diskdb instances that
    /// own them. Broadcasts to all known channels — the owning instance
    /// accepts the commit, others reject.
    ///
    /// # Errors
    /// Returns a String error if all commit RPCs fail.
    pub async fn commit_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        let mut futures = Vec::new();
        for endpoint in &self.channels {
            let channel = endpoint.value().clone();
            let segs = segments.clone();
            futures.push(async move {
                let mut client =
                    crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient::new(channel);
                let req = CommitBlocksRequest { segments: segs };
                client.commit_blocks(req).await
            });
        }

        if futures.is_empty() {
            return Err("no channels available for commit_blocks".into());
        }

        let results = futures::future::join_all(futures).await;
        let mut failed = 0;
        for result in &results {
            if result.is_ok() {
                return Ok(());
            }
            failed += 1;
        }
        Err(format!("all commit_blocks RPCs failed ({failed} attempts)"))
    }

    /// Free blocks via the diskdb instances that own them.
    ///
    /// Segments are grouped by disk-group (via `disk_id → dg_id`
    /// reverse lookup) and freed in parallel to the owning instances.
    /// Falls back to broadcast when the reverse lookup misses (cache
    /// cold or unknown disk_id).
    ///
    /// # Errors
    /// Returns a String error if any free RPC fails.
    pub async fn free_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        // Group segments by disk-group ID via reverse lookup.
        let mut grouped: HashMap<u64, Vec<Segment>> = HashMap::new();
        let mut ungrouped: Vec<Segment> = Vec::new();
        for seg in segments {
            if let Some(disk_id) = &seg.disk_id {
                if let Some(dg_id) = self.dg_for_disk(disk_id) {
                    grouped.entry(dg_id).or_default().push(seg);
                    continue;
                }
            }
            ungrouped.push(seg);
        }

        let mut futures: Vec<FreeFut> = Vec::new();

        // Precise routing: one free RPC per disk-group.
        for (dg_id, segs) in grouped {
            match self.channel_for_dg(dg_id).await {
                Ok(channel) => {
                    futures.push(Box::pin(async move {
                        let mut client =
                            crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient::new(
                                channel,
                            );
                        let req = FreeBlocksRequest { segments: segs };
                        client.free_blocks(req).await
                    }));
                }
                Err(e) => {
                    warn!(error = %e, disk_group_id = dg_id, "free_blocks: no endpoint for dg, falling back to broadcast");
                    for seg in segs {
                        ungrouped.push(seg);
                    }
                }
            }
        }

        // Broadcast fallback for ungrouped segments (reverse lookup miss).
        if !ungrouped.is_empty() {
            for endpoint in &self.channels {
                let channel = endpoint.value().clone();
                let segs = ungrouped.clone();
                futures.push(Box::pin(async move {
                    let mut client =
                        crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient::new(channel);
                    let req = FreeBlocksRequest { segments: segs };
                    client.free_blocks(req).await
                }));
            }
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
