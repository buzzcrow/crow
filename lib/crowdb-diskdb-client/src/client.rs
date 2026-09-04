// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskdbClient` — full client library for CROWDB diskdb operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all diskdb
//! instances from the service registry, populates a `DashMap` cache
//! (`disk_group_id -> rpc_endpoint`). On cache miss or
//! `Unavailable`, lazily refreshes and retries.
//!
//! All RPCs go through the crowdb-rpc flatbuffer transport.

use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, collections::HashSet};

use dashmap::DashMap;
use tracing::warn;

use crowdb_kv_client::ServiceRegistryClient;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CommitBlocksRequest, CommitBlocksResponse, CompactZoneRequest,
    CompactZoneResponse, FreeBlocksRequest, FreeResponse, GetDiskGroupInfoResponse, GetDiskInfoResponse,
    GetScanStatusResponse, QueryCapacityStatsRequest, QueryCapacityStatsResponse, RebuildZoneBitmapResponse,
    RecalcDiskUsageRequest, RecalcDiskUsageResponse, TriggerScanResponse,
};
use crowdb_protocol::DiskGroupId;

use crate::rpc_transport::DiskdbRpcTransport;
use crate::{DiskdbClientError, Result};

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

/// Client for CROWDB diskdb operations via crowdb-rpc.
#[derive(Clone)]
pub struct DiskdbClient {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> rpc_endpoint` cache.
    endpoint_cache: DashMap<DiskGroupId, String>,
    /// `disk_id -> disk_group_id` reverse routing map (for
    /// `free_blocks` across multiple disk-groups).
    disk_to_dg: DashMap<DiskId, DiskGroupId>,
    /// crowdb-rpc transport.
    rpc_transport: Arc<DiskdbRpcTransport>,
    retry: RetryConfig,
}

impl DiskdbClient {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient, rpc_transport: Arc<DiskdbRpcTransport>) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            disk_to_dg: DashMap::new(),
            rpc_transport,
            retry: RetryConfig::default(),
        }
    }

    /// Set a custom retry config.
    #[must_use]
    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Eager warm: read all diskdb instances, populate the endpoint
    /// cache + disk→dg reverse map.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Unreachable` if the service registry read fails.
    pub async fn refresh_endpoints(&self) -> Result<()> {
        let instances = self
            .svc
            .read_all_diskdb_instances()
            .await
            .map_err(|e| DiskdbClientError::Unreachable(format!("read_all_diskdb_instances: {e}")))?;
        let mut observed = HashMap::new();
        for (_id, value) in instances {
            if let Some(extra) = &value.extra {
                if let Some(diskdb) = &extra.diskdb {
                    for &dg_id in &diskdb.owned_dg_ids {
                        observed.insert(dg_id, value.rpc_endpoint.clone());
                    }
                }
            }
        }
        let observed_ids: HashSet<_> = observed.keys().copied().collect();
        self.endpoint_cache
            .retain(|dg_id, _| observed_ids.contains(dg_id));
        for (dg_id, endpoint) in observed {
            self.endpoint_cache.insert(dg_id, endpoint);
        }
        self.disk_to_dg.retain(|_, dg_id| observed_ids.contains(dg_id));
        Ok(())
    }

    /// Return the currently discovered disk-groups in stable order.
    #[must_use]
    pub fn disk_group_ids(&self) -> Vec<DiskGroupId> {
        let mut ids: Vec<_> = self.endpoint_cache.iter().map(|entry| *entry.key()).collect();
        ids.sort_unstable();
        ids
    }

    /// Look up the endpoint for `dg_id`, refreshing on cache miss.
    async fn endpoint_for(&self, dg_id: DiskGroupId) -> Result<String> {
        if let Some(endpoint) = self.endpoint_cache.get(&dg_id) {
            return Ok(endpoint.clone());
        }
        // Cache miss — refresh and retry.
        self.refresh_endpoints().await?;
        self.endpoint_cache
            .get(&dg_id)
            .map(|e| e.clone())
            .ok_or_else(|| DiskdbClientError::Unreachable(format!("no diskdb instance owns dg {dg_id}")))
    }

    /// Allocate blocks on a disk-group. Retries on transient errors
    /// (`Unavailable`, deadline-exceeded). `ResourceExhausted` (no
    /// space) is returned to the caller (not retryable).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn allocate_blocks(&self, req: AllocateBlocksRequest) -> Result<AllocateResponse> {
        let dg_id = req.disk_group_id;
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.allocate_blocks(&endpoint, &req).await }
        })
        .await
    }

    /// Free blocks. The request carries `Segment`s (each with
    /// `disk_id`); routes by looking up which diskdb instance owns
    /// each `disk_id`'s disk-group. If segments span multiple
    /// disk-groups, splits the request per-group and issues one
    /// `FreeBlocks` per group. v1: returns the first error on partial
    /// failure.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn free_blocks(&self, req: FreeBlocksRequest) -> Result<FreeResponse> {
        if req.segments.is_empty() {
            return Ok(FreeResponse { freed_count: 0 });
        }
        // Group segments by disk-group.
        let mut groups: Vec<(DiskGroupId, Vec<_>)> = Vec::new();
        for seg in &req.segments {
            let disk_id = seg
                .disk_id
                .ok_or_else(|| DiskdbClientError::Rpc("segment.disk_id required".into()))?;
            let dg_id = self.dg_for_disk(disk_id).await?;
            if let Some((_, segs)) = groups.iter_mut().find(|(g, _)| *g == dg_id) {
                segs.push(*seg);
            } else {
                groups.push((dg_id, vec![*seg]));
            }
        }
        let mut total_freed = 0u32;
        for (dg_id, segs) in groups {
            let sub_req = FreeBlocksRequest { segments: segs };
            let resp = self
                .with_rpc_retry(dg_id, |endpoint, rpc| {
                    let req = sub_req.clone();
                    async move { rpc.free_blocks(&endpoint, &req).await }
                })
                .await?;
            total_freed += resp.freed_count;
        }
        Ok(FreeResponse {
            freed_count: total_freed,
        })
    }

    /// Commit tentative blocks. Requests spanning disk-groups are split
    /// and routed to each current owner.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures and
    /// `Unreachable` for routing or connection errors.
    pub async fn commit_blocks(&self, req: CommitBlocksRequest) -> Result<CommitBlocksResponse> {
        if req.segments.is_empty() {
            return Ok(CommitBlocksResponse { committed_count: 0 });
        }
        let mut groups: Vec<(DiskGroupId, Vec<_>)> = Vec::new();
        for seg in &req.segments {
            let disk_id = seg
                .disk_id
                .ok_or_else(|| DiskdbClientError::Rpc("segment.disk_id required".into()))?;
            let dg_id = self.dg_for_disk(disk_id).await?;
            if let Some((_, segs)) = groups.iter_mut().find(|(group_id, _)| *group_id == dg_id) {
                segs.push(*seg);
            } else {
                groups.push((dg_id, vec![*seg]));
            }
        }
        let mut committed_count = 0u32;
        for (dg_id, segments) in groups {
            let sub_req = CommitBlocksRequest { segments };
            let response = self
                .with_rpc_retry(dg_id, |endpoint, rpc| {
                    let request = sub_req.clone();
                    async move { rpc.commit_blocks(&endpoint, &request).await }
                })
                .await?;
            committed_count += response.committed_count;
        }
        Ok(CommitBlocksResponse { committed_count })
    }

    /// Query capacity stats at the disk-group level (all owned groups
    /// if `dg_id == 0`).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_capacity_stats(
        &self,
        req: QueryCapacityStatsRequest,
    ) -> Result<QueryCapacityStatsResponse> {
        let dg_id = if req.disk_group_id != 0 {
            req.disk_group_id
        } else {
            // Use the first cached endpoint for an all-owned query.
            self.first_cached_dg()?
        };
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.query_capacity_stats(&endpoint, &req).await }
        })
        .await
    }

    /// Query one disk-group's capacity stats.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_disk_group(&self, dg_id: u64) -> Result<QueryCapacityStatsResponse> {
        self.query_capacity_stats(QueryCapacityStatsRequest {
            disk_group_id: dg_id,
            disk_id: None,
            zone_index: None,
        })
        .await
    }

    /// Query one disk's capacity stats (brief per-zone entries).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_disk(&self, dg_id: u64, disk_id: DiskId) -> Result<QueryCapacityStatsResponse> {
        self.query_capacity_stats(QueryCapacityStatsRequest {
            disk_group_id: dg_id,
            disk_id: Some(disk_id),
            zone_index: None,
        })
        .await
    }

    /// Query one zone's capacity stats (full `usage_bitmap`).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn query_zone(
        &self,
        dg_id: u64,
        disk_id: DiskId,
        zone_index: u32,
    ) -> Result<QueryCapacityStatsResponse> {
        self.query_capacity_stats(QueryCapacityStatsRequest {
            disk_group_id: dg_id,
            disk_id: Some(disk_id),
            zone_index: Some(zone_index),
        })
        .await
    }

    /// Get disk-group info.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_disk_group_info(&self, dg_id: u64) -> Result<GetDiskGroupInfoResponse> {
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.get_disk_group_info(&endpoint, dg_id).await
        })
        .await
    }

    /// Get disk info. `rack_id`/`node_id` are passed as 0 (the
    /// service handler resolves the disk by `disk_group_id` +
    /// `disk_id` only).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_disk_info(&self, dg_id: u64, disk_id: DiskId) -> Result<GetDiskInfoResponse> {
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.get_disk_info(&endpoint, dg_id, disk_id).await
        })
        .await
    }

    /// Recalc disk usage (admin RPC).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn recalc_disk_usage(&self, req: RecalcDiskUsageRequest) -> Result<RecalcDiskUsageResponse> {
        // Route to any cached endpoint (recalc covers all owned groups
        // when disk_group_id is None).
        let dg_id = if let Some(id) = req.disk_group_id {
            id
        } else {
            self.first_cached_dg()?
        };
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.recalc_disk_usage(&endpoint, &req).await }
        })
        .await
    }

    /// Compact one or more zones on a disk (admin RPC). Empty
    /// `zone_indices` = all zones on the disk. Routes by looking up
    /// which diskdb instance owns the disk's disk-group.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn compact_zone(&self, req: CompactZoneRequest) -> Result<CompactZoneResponse> {
        let disk_id = req
            .disk_id
            .ok_or_else(|| DiskdbClientError::Rpc("disk_id required".into()))?;
        let dg_id = self.dg_for_disk(disk_id).await?;
        self.with_rpc_retry(dg_id, |endpoint, rpc| {
            let req = req.clone();
            async move { rpc.compact_zone(&endpoint, &req).await }
        })
        .await
    }

    /// Trigger a scan on all owned groups (or one group if `dg_id` is
    /// set). Returns the last `ScanSummary` + `scan_in_progress`. If a
    /// scan is already running the server returns `scan_in_progress:
    /// true` (no error, no stacking). Admin/debug call; transient
    /// `Unavailable` is retried per `RetryConfig`.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn trigger_scan(&self, dg_id: Option<DiskGroupId>) -> Result<TriggerScanResponse> {
        let dg_id = match dg_id {
            Some(id) => id,
            None => self.first_cached_dg()?,
        };
        self.with_rpc_retry(
            dg_id,
            |endpoint, rpc| async move { rpc.trigger_scan(&endpoint).await },
        )
        .await
    }

    /// Get the last scan summary + `has_run` flag. `has_run` is false
    /// if no scan has completed yet (the summary is empty in that
    /// case). Admin/debug call; transient `Unavailable` is retried.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_scan_status(&self, dg_id: Option<DiskGroupId>) -> Result<GetScanStatusResponse> {
        let dg_id = match dg_id {
            Some(id) => id,
            None => self.first_cached_dg()?,
        };
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.get_scan_status(&endpoint).await
        })
        .await
    }

    /// Rebuild one zone's bitmap on a disk (admin/debug). Routes via
    /// `dg_for_disk` so the call lands on the owning diskdb instance.
    /// `zone_index = u32::MAX` means all zones on the disk.
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors (including unknown disk).
    pub async fn rebuild_zone_bitmap(
        &self,
        disk_id: DiskId,
        zone_index: u32,
    ) -> Result<RebuildZoneBitmapResponse> {
        let dg_id = self.dg_for_disk(disk_id).await?;
        self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
            rpc.rebuild_zone_bitmap(&endpoint, disk_id, zone_index).await
        })
        .await
    }

    /// Return the first cached disk-group id, or `Unreachable` if the
    /// cache is empty.
    fn first_cached_dg(&self) -> Result<DiskGroupId> {
        self.endpoint_cache
            .iter()
            .next()
            .map(|r| *r.key())
            .ok_or_else(|| {
                DiskdbClientError::Unreachable("no cached endpoints; call refresh_endpoints".into())
            })
    }

    /// Look up which disk-group owns a `disk_id`. Refreshes the
    /// disk→dg reverse map on miss.
    async fn dg_for_disk(&self, disk_id: DiskId) -> Result<DiskGroupId> {
        if let Some(dg_id) = self.disk_to_dg.get(&disk_id) {
            return Ok(*dg_id);
        }
        // Refresh the reverse map from the hardware hierarchy.
        self.refresh_endpoints().await?;
        // The reverse map is populated during refresh_endpoints from
        // the service registry's DiskdbExtra.owned_dg_ids — but that
        // only gives dg_id→endpoint, not disk_id→dg_id. For v1, we
        // try each cached endpoint's get_disk_group_info to find the
        // disk. This is O(groups) on first miss; subsequent calls hit
        // the cache.
        for entry in &self.endpoint_cache {
            let dg_id = *entry.key();
            let endpoint = entry.value().clone();
            drop(entry);
            let group_result = match self.rpc_transport.get_disk_group_info(&endpoint, dg_id).await {
                Ok(resp) => resp.group,
                Err(e) => {
                    warn!(disk_id = ?disk_id, dg_id, error = %e, "dg_for_disk: rpc get_disk_group_info failed");
                    continue;
                }
            };
            if let Some(group) = group_result {
                if group.disk_ids.contains(&disk_id) {
                    self.disk_to_dg.insert(disk_id, dg_id);
                    return Ok(dg_id);
                }
            }
        }
        Err(DiskdbClientError::Unreachable(format!(
            "no diskdb instance owns disk {disk_id:?}"
        )))
    }

    /// crowdb-rpc retry wrapper: calls `op` with the endpoint + transport,
    /// retries on transient errors (`Unreachable`).
    async fn with_rpc_retry<F, Fut, T>(&self, dg_id: DiskGroupId, op: F) -> Result<T>
    where
        F: Fn(String, Arc<DiskdbRpcTransport>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let rpc = Arc::clone(&self.rpc_transport);
        let mut backoff = self.retry.initial_backoff;
        let mut last_err = None;
        for attempt in 0..=self.retry.max_retries {
            let endpoint = match self.endpoint_for(dg_id).await {
                Ok(e) => e,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                    continue;
                }
            };
            match op(endpoint, Arc::clone(&rpc)).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if matches!(
                        e,
                        DiskdbClientError::Unreachable(_) | DiskdbClientError::NotOwner(_)
                    ) {
                        warn!(dg_id, attempt, error = %e, "rpc transient error, retrying");
                        last_err = Some(e);
                        self.endpoint_cache.remove(&dg_id);
                        self.disk_to_dg.retain(|_, cached_dg_id| *cached_dg_id != dg_id);
                        let _ = self.refresh_endpoints().await;
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| DiskdbClientError::Unreachable("max retries exhausted".into())))
    }
}

/// Normalize a service-registry endpoint: rewrite `0.0.0.0`
/// to `127.0.0.1` so the connection goes to a loopback address.
#[must_use]
pub fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}
