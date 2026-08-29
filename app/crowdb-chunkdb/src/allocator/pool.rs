// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb client pool — caches crowdb-rpc transports to diskdb instances.
//!
//! Routes `AllocateBlocks` / `FreeBlocks` to the correct diskdb
//! instance per disk-group, using `ServiceRegistryClient` for endpoint
//! discovery.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tracing::warn;

use crowdb_diskdb_client::DiskdbRpcTransport;
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CommitBlocksRequest, FreeBlocksRequest, FreeResponse, Segment,
};

/// Pool of diskdb crowdb-rpc transports, keyed by disk-group ID.
pub struct DiskdbClientPool {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> rpc_endpoint` cache.
    endpoints: DashMap<u64, String>,
    /// `disk_id -> disk_group_id` reverse lookup cache (GAP-4).
    /// Populated from the topology cache's `DiskGroupEntry` list.
    /// Used for precise `free_blocks` routing.
    disk_id_to_dg: DashMap<DiskId, u64>,
    /// Shared crowdb-rpc transport.
    transport: Arc<DiskdbRpcTransport>,
}

impl DiskdbClientPool {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoints: DashMap::new(),
            disk_id_to_dg: DashMap::new(),
            transport: Arc::new(DiskdbRpcTransport::new()),
        }
    }

    /// Update the `disk_id → disk_group_id` reverse lookup cache from
    /// a topology snapshot. Called by the topology refresh loop.
    pub fn update_disk_id_lookup(&self, entries: &[crowdb_protocol::sysdata::DiskGroupEntry]) {
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

    /// Resolve the endpoint for the diskdb instance owning
    /// `disk_group_id`.
    async fn endpoint_for_dg(&self, dg_id: u64) -> Result<String, String> {
        // Check endpoint cache.
        if let Some(endpoint) = self.endpoints.get(&dg_id) {
            return Ok(endpoint.value().clone());
        }

        // Cache miss — refresh from service registry and retry.
        self.refresh_endpoints().await?;
        if let Some(endpoint) = self.endpoints.get(&dg_id) {
            return Ok(endpoint.value().clone());
        }
        Err(format!("no endpoint cached for disk_group {dg_id}"))
    }

    /// Warm the endpoint cache by reading all diskdb instances from the
    /// service registry. Maps each instance's `owned_dg_ids` to its
    /// RPC endpoint so `endpoint_for_dg` can route by disk-group ID.
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
                        self.endpoints.insert(*dg_id, value.rpc_endpoint.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// All cached endpoints (for broadcast operations).
    fn all_endpoints(&self) -> Vec<String> {
        self.endpoints.iter().map(|r| r.value().clone()).collect()
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
        let endpoint = self.endpoint_for_dg(dg_id).await?;
        let req = AllocateBlocksRequest {
            disk_group_id: dg_id,
            unit_count,
            count,
            exclude_disk_ids: vec![],
            owner_chunk: Some(*owner_chunk),
        };
        self.transport
            .allocate_blocks(&endpoint, &req)
            .await
            .map_err(|e| format!("allocate_blocks RPC: {e}"))
    }

    /// Commit blocks (mark as permanent) on the diskdb instances that
    /// own them. Broadcasts to all known endpoints — the owning instance
    /// accepts the commit, others reject.
    ///
    /// # Errors
    /// Returns a String error if all commit RPCs fail.
    pub async fn commit_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        let endpoints = self.all_endpoints();
        if endpoints.is_empty() {
            return Err("no endpoints available for commit_blocks".into());
        }

        let mut futures = Vec::new();
        for endpoint in endpoints {
            let segs = segments.clone();
            let transport = Arc::clone(&self.transport);
            futures.push(async move {
                let req = CommitBlocksRequest { segments: segs };
                transport
                    .commit_blocks(&endpoint, &req)
                    .await
                    .map_err(|e| format!("commit_blocks RPC: {e}"))
            });
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
        type FreeFut = std::pin::Pin<
            Box<dyn std::future::Future<Output = std::result::Result<FreeResponse, String>> + Send>,
        >;

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
            match self.endpoint_for_dg(dg_id).await {
                Ok(endpoint) => {
                    let transport = Arc::clone(&self.transport);
                    futures.push(Box::pin(async move {
                        let req = FreeBlocksRequest { segments: segs };
                        transport
                            .free_blocks(&endpoint, &req)
                            .await
                            .map_err(|e| format!("free_blocks RPC: {e}"))
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
            for endpoint in self.all_endpoints() {
                let segs = ungrouped.clone();
                let transport = Arc::clone(&self.transport);
                futures.push(Box::pin(async move {
                    let req = FreeBlocksRequest { segments: segs };
                    transport
                        .free_blocks(&endpoint, &req)
                        .await
                        .map_err(|e| format!("free_blocks RPC: {e}"))
                }));
            }
        }

        if futures.is_empty() {
            return Err("no endpoints available for free_blocks".into());
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
