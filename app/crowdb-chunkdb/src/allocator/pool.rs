// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb client pool — caches crowdb-rpc transports to diskdb instances.
//!
//! Routes `AllocateBlocks` / `FreeBlocks` to the correct diskdb
//! instance per disk-group, using `ServiceRegistryClient` for endpoint
//! discovery.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use tracing::warn;

use crowdb_diskdb_client::{DiskdbClientError, DiskdbRpcTransport};
use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CommitBlocksRequest, FreeBlocksRequest, Segment,
};

/// Pool of diskdb crowdb-rpc transports, keyed by disk-group ID.
pub struct DiskdbClientPool {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> rpc_endpoint` cache.
    endpoints: DashMap<u64, String>,
    /// `disk_id -> disk_group_id` reverse lookup cache (GAP-4).
    /// Populated from the topology cache's `DiskGroupEntry` list.
    /// Used for precise `free_blocks` routing.
    disk_id_to_dg: ArcSwap<HashMap<DiskId, u64>>,
    /// Shared crowdb-rpc transport.
    transport: Arc<DiskdbRpcTransport>,
}

impl DiskdbClientPool {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoints: DashMap::new(),
            disk_id_to_dg: ArcSwap::from_pointee(HashMap::new()),
            transport: Arc::new(DiskdbRpcTransport::new()),
        }
    }

    /// Update the `disk_id → disk_group_id` reverse lookup cache from
    /// a topology snapshot. Called by the topology refresh loop.
    pub fn update_disk_id_lookup(&self, entries: &[crowdb_protocol::sysdata::DiskGroupEntry]) {
        let mut refreshed = HashMap::new();
        for entry in entries {
            for disk_id in &entry.value.disk_ids {
                refreshed.insert(*disk_id, entry.dg_id);
            }
        }
        self.disk_id_to_dg.store(Arc::new(refreshed));
    }

    /// Look up the disk-group ID for a disk_id (reverse lookup).
    pub(crate) fn dg_for_disk(&self, disk_id: &DiskId) -> Option<u64> {
        self.disk_id_to_dg.load().get(disk_id).copied()
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
        let mut refreshed = HashMap::new();
        for (_id, value) in instances {
            if let Some(ref extra) = value.extra {
                if let Some(ref diskdb) = extra.diskdb {
                    for dg_id in &diskdb.owned_dg_ids {
                        refreshed.insert(*dg_id, value.rpc_endpoint.clone());
                    }
                }
            }
        }
        self.endpoints.retain(|dg_id, _| refreshed.contains_key(dg_id));
        for (dg_id, endpoint) in refreshed {
            self.endpoints.insert(dg_id, endpoint);
        }
        Ok(())
    }

    /// Allocate blocks on the diskdb instance owning `disk_group_id`.
    ///
    /// Retries transient RPC failures (`DiskdbClientError::Unreachable`
    /// — timeout, connection reset, endpoint-not-yet-cached) with
    /// exponential backoff, mirroring `DiskdbClient::with_rpc_retry`.
    /// This rides out momentary overload (e.g. a Paxos round spiking
    /// past the client RPC reaper under concurrent load). Hard errors
    /// (`DiskdbClientError::Rpc` — NoSpace, NotOwner, etc.) are
    /// returned immediately. A timed-out attempt may leave orphaned
    /// tentative segments on the diskdb side; the orphan scanner
    /// reclaims them.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Unreachable` if the endpoint is not
    /// cached or the RPC fails with a transient (retryable) error after
    /// exhausting retries, or `DiskdbClientError::Rpc` for a hard RPC
    /// failure.
    pub async fn allocate_blocks(
        &self,
        dg_id: u64,
        count: u32,
        unit_count: u32,
        owner_chunk: &ChunkId,
    ) -> Result<AllocateResponse, DiskdbClientError> {
        const MAX_TRANSIENT_RETRIES: u32 = 2;
        const INITIAL_BACKOFF: Duration = Duration::from_millis(200);

        let req = AllocateBlocksRequest {
            disk_group_id: dg_id,
            unit_count,
            count,
            exclude_disk_ids: vec![],
            owner_chunk: Some(*owner_chunk),
        };

        let mut backoff = INITIAL_BACKOFF;
        let mut last_err: Option<DiskdbClientError> = None;
        for attempt in 0..=MAX_TRANSIENT_RETRIES {
            let endpoint = self.endpoint_for_dg(dg_id).await.map_err(|e| {
                DiskdbClientError::Unreachable(format!("no endpoint for disk_group {dg_id}: {e}"))
            })?;
            match self.transport.allocate_blocks(&endpoint, &req).await {
                Ok(resp) => return Ok(resp),
                Err(e @ DiskdbClientError::Unreachable(_)) => {
                    if attempt < MAX_TRANSIENT_RETRIES {
                        warn!(
                            disk_group_id = dg_id,
                            attempt = attempt + 1,
                            error = %e,
                            "transient allocate_blocks RPC, retrying after backoff"
                        );
                        last_err = Some(e);
                        let _ = self.refresh_endpoints().await;
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| DiskdbClientError::Unreachable("transient retries exhausted".into())))
    }

    /// Commit blocks on the DiskDB instances that own them.
    ///
    /// # Errors
    /// Returns a String error if routing is incomplete or any commit fails.
    pub async fn commit_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        let grouped = self.group_segments(segments, "commit_blocks")?;
        let mut futures = Vec::with_capacity(grouped.len());
        for (dg_id, segs) in grouped {
            let endpoint = self.endpoint_for_dg(dg_id).await?;
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
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Free blocks via the diskdb instances that own them.
    ///
    /// Segments are grouped by disk-group (via `disk_id → dg_id`
    /// reverse lookup) and freed in parallel to the owning instances.
    /// # Errors
    /// Returns a String error if routing is incomplete or any free RPC fails.
    pub async fn free_blocks(&self, segments: Vec<Segment>) -> Result<(), String> {
        if segments.is_empty() {
            return Ok(());
        }

        let grouped = self.group_segments(segments, "free_blocks")?;
        let mut futures = Vec::with_capacity(grouped.len());
        for (dg_id, segs) in grouped {
            let endpoint = self.endpoint_for_dg(dg_id).await?;
            let transport = Arc::clone(&self.transport);
            futures.push(async move {
                let req = FreeBlocksRequest { segments: segs };
                transport
                    .free_blocks(&endpoint, &req)
                    .await
                    .map_err(|e| format!("free_blocks RPC: {e}"))
            });
        }

        let results = futures::future::join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(Result::err).collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn group_segments(
        &self,
        segments: Vec<Segment>,
        operation: &str,
    ) -> Result<HashMap<u64, Vec<Segment>>, String> {
        let mut grouped = HashMap::new();
        for segment in segments {
            let disk_id = segment
                .disk_id
                .as_ref()
                .ok_or_else(|| format!("{operation}: segment has no disk_id"))?;
            let dg_id = self
                .dg_for_disk(disk_id)
                .ok_or_else(|| format!("{operation}: disk_id {disk_id:?} has no disk-group mapping"))?;
            grouped.entry(dg_id).or_insert_with(Vec::new).push(segment);
        }
        Ok(grouped)
    }
}
