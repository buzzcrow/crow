// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskdbClient` — full client library for CROW diskdb gRPC operations.
//!
//! Endpoint discovery + cache: `refresh_endpoints` reads all diskdb
//! instances from the service registry, populates a `DashMap` cache
//! (`disk_group_id -> rpc_endpoint`). On cache miss or
//! `Unavailable`, lazily refreshes and retries.
//!
//! Channel pool: a `DashMap<String, tonic::transport::Channel>` per
//! endpoint; lazily created on first use.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tonic::transport::Channel;
use tracing::warn;

use crow_kv_client::ServiceRegistryClient;
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient;
use crow_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, CompactZoneRequest, CompactZoneResponse, FreeBlocksRequest,
    FreeResponse, GetDiskGroupInfoRequest, GetDiskGroupInfoResponse, GetDiskInfoRequest, GetDiskInfoResponse,
    GetScanStatusRequest, GetScanStatusResponse, QueryCapacityStatsRequest, QueryCapacityStatsResponse,
    RebuildZoneBitmapRequest, RebuildZoneBitmapResponse, RecalcDiskUsageRequest, RecalcDiskUsageResponse,
    TriggerScanRequest, TriggerScanResponse,
};
use crow_protocol::DiskGroupId;

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

/// Client for CROW diskdb gRPC operations.
#[derive(Clone)]
pub struct DiskdbClient {
    svc: ServiceRegistryClient,
    /// `disk_group_id -> rpc_endpoint` cache.
    endpoint_cache: DashMap<DiskGroupId, String>,
    /// `disk_id -> disk_group_id` reverse routing map (for
    /// `free_blocks` across multiple disk-groups).
    disk_to_dg: DashMap<DiskId, DiskGroupId>,
    /// `rpc_endpoint -> Channel` pool.
    channels: DashMap<String, Channel>,
    /// Optional crow-rpc transport (R115 migration). When set, the
    /// client uses crow-rpc instead of tonic gRPC.
    rpc_transport: Option<Arc<DiskdbRpcTransport>>,
    retry: RetryConfig,
}

impl DiskdbClient {
    #[must_use]
    pub fn new(svc: ServiceRegistryClient) -> Self {
        Self {
            svc,
            endpoint_cache: DashMap::new(),
            disk_to_dg: DashMap::new(),
            channels: DashMap::new(),
            rpc_transport: None,
            retry: RetryConfig::default(),
        }
    }

    /// Set a custom retry config.
    #[must_use]
    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Enable crow-rpc transport (R115 migration). When set, all RPCs
    /// use crow-rpc instead of tonic gRPC. The transport manages its
    /// own connection pool; the grpc channel pool is unused.
    #[must_use]
    pub fn with_rpc_transport(mut self, transport: Arc<DiskdbRpcTransport>) -> Self {
        self.rpc_transport = Some(transport);
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
        for (_id, value) in instances {
            if let Some(extra) = &value.extra {
                if let Some(diskdb) = &extra.diskdb {
                    for &dg_id in &diskdb.owned_dg_ids {
                        self.endpoint_cache.insert(dg_id, value.rpc_endpoint.clone());
                    }
                }
            }
        }
        Ok(())
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

    /// Get or create a gRPC channel for the given endpoint.
    fn channel_for(&self, endpoint: &str) -> Result<Channel> {
        let normalized = normalize_endpoint(endpoint);
        if let Some(ch) = self.channels.get(&normalized) {
            return Ok(ch.clone());
        }
        let ch = Channel::from_shared(normalized.clone())
            .map_err(|e| DiskdbClientError::Unreachable(format!("invalid endpoint {endpoint}: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .connect_lazy();
        self.channels.insert(normalized, ch.clone());
        Ok(ch)
    }

    /// Get or create a gRPC client for the given `dg_id`.
    async fn client_for(&self, dg_id: DiskGroupId) -> Result<DiskdbServiceClient<Channel>> {
        let endpoint = self.endpoint_for(dg_id).await?;
        let channel = self.channel_for(&endpoint)?;
        Ok(DiskdbServiceClient::new(channel))
    }

    /// Allocate blocks on a disk-group. Retries on transient errors
    /// (`Unavailable`, deadline-exceeded). `ResourceExhausted` (no
    /// space) is returned to the caller (not retryable).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn allocate_blocks(&self, req: AllocateBlocksRequest) -> Result<AllocateResponse> {
        let dg_id = req.disk_group_id;
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| {
                let req = req.clone();
                async move { rpc.allocate_blocks(&endpoint, &req).await }
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = req.clone();
                async move { client.allocate_blocks(req).await }
            })
            .await
        }
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
            if let Some(_rpc) = &self.rpc_transport {
                let resp = self
                    .with_rpc_retry(dg_id, |endpoint, rpc| {
                        let req = sub_req.clone();
                        async move { rpc.free_blocks(&endpoint, &req).await }
                    })
                    .await?;
                total_freed += resp.freed_count;
            } else {
                let resp = self
                    .with_retry(dg_id, |mut client| {
                        let req = sub_req.clone();
                        async move { client.free_blocks(req).await }
                    })
                    .await?;
                total_freed += resp.freed_count;
            }
        }
        Ok(FreeResponse {
            freed_count: total_freed,
        })
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
                rpc.query_capacity_stats(&endpoint, &req).await
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| async move {
                client.query_capacity_stats(req).await
            })
            .await
        }
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
                rpc.get_disk_group_info(&endpoint, dg_id).await
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = GetDiskGroupInfoRequest { disk_group_id: dg_id };
                async move { client.get_disk_group_info(req).await }
            })
            .await
        }
    }

    /// Get disk info. `rack_id`/`node_id` are passed as 0 (the
    /// service handler resolves the disk by `disk_group_id` +
    /// `disk_id` only).
    ///
    /// # Errors
    /// Returns `DiskdbClientError::Rpc` for RPC failures, `Unreachable` for connection errors.
    pub async fn get_disk_info(&self, dg_id: u64, disk_id: DiskId) -> Result<GetDiskInfoResponse> {
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
                rpc.get_disk_info(&endpoint, dg_id, disk_id).await
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = GetDiskInfoRequest {
                    rack_id: 0,
                    node_id: 0,
                    disk_group_id: dg_id,
                    disk_id: Some(disk_id),
                };
                async move { client.get_disk_info(req).await }
            })
            .await
        }
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
                rpc.recalc_disk_usage(&endpoint, &req).await
            })
            .await
        } else {
            self.with_retry(
                dg_id,
                |mut client| async move { client.recalc_disk_usage(req).await },
            )
            .await
        }
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| {
                let req = req.clone();
                async move { rpc.compact_zone(&endpoint, &req).await }
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = req.clone();
                async move { client.compact_zone(req).await }
            })
            .await
        }
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(
                dg_id,
                |endpoint, rpc| async move { rpc.trigger_scan(&endpoint).await },
            )
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = TriggerScanRequest {};
                async move { client.trigger_scan(req).await }
            })
            .await
        }
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
                rpc.get_scan_status(&endpoint).await
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = GetScanStatusRequest {};
                async move { client.get_scan_status(req).await }
            })
            .await
        }
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
        if let Some(_rpc) = &self.rpc_transport {
            self.with_rpc_retry(dg_id, |endpoint, rpc| async move {
                rpc.rebuild_zone_bitmap(&endpoint, disk_id, zone_index).await
            })
            .await
        } else {
            self.with_retry(dg_id, |mut client| {
                let req = RebuildZoneBitmapRequest {
                    disk_id: Some(disk_id),
                    zone_index,
                };
                async move { client.rebuild_zone_bitmap(req).await }
            })
            .await
        }
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
            let group_result = if let Some(rpc) = &self.rpc_transport {
                match rpc.get_disk_group_info(&endpoint, dg_id).await {
                    Ok(resp) => resp.group,
                    Err(e) => {
                        warn!(disk_id = ?disk_id, dg_id, error = %e, "dg_for_disk: rpc get_disk_group_info failed");
                        continue;
                    }
                }
            } else {
                let channel = self.channel_for(&endpoint)?;
                let mut client = DiskdbServiceClient::new(channel);
                match client
                    .get_disk_group_info(GetDiskGroupInfoRequest { disk_group_id: dg_id })
                    .await
                {
                    Ok(resp) => resp.into_inner().group,
                    Err(e) => {
                        warn!(disk_id = ?disk_id, dg_id, error = %e, "dg_for_disk: get_disk_group_info failed");
                        continue;
                    }
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

    /// Retry wrapper: calls `op` with a fresh client, retries on
    /// transient tonic errors (`Unavailable`, deadline-exceeded).
    async fn with_retry<F, Fut, T>(&self, dg_id: DiskGroupId, op: F) -> Result<T>
    where
        F: Fn(DiskdbServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
    {
        let mut backoff = self.retry.initial_backoff;
        let mut last_err = None;
        for attempt in 0..=self.retry.max_retries {
            let client = match self.client_for(dg_id).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                    continue;
                }
            };
            match op(client).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status) => {
                    let code = status.code();
                    if code == tonic::Code::Unavailable
                        || code == tonic::Code::DeadlineExceeded
                        || code == tonic::Code::Aborted
                    {
                        warn!(dg_id, attempt, code = ?code, "transient error, retrying");
                        last_err = Some(DiskdbClientError::Rpc(format!("{code}: {status}")));
                        // Refresh endpoints in case the instance moved.
                        let _ = self.refresh_endpoints().await;
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    }
                    // Non-retryable: map and return.
                    return Err(map_status(&status));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| DiskdbClientError::Unreachable("max retries exhausted".into())))
    }

    /// crow-rpc retry wrapper: calls `op` with the endpoint + transport,
    /// retries on transient errors (`Unreachable`).
    async fn with_rpc_retry<F, Fut, T>(&self, dg_id: DiskGroupId, op: F) -> Result<T>
    where
        F: Fn(String, Arc<DiskdbRpcTransport>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let rpc = Arc::clone(
            self.rpc_transport
                .as_ref()
                .expect("with_rpc_retry called without rpc_transport"),
        );
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
                    if matches!(e, DiskdbClientError::Unreachable(_)) {
                        warn!(dg_id, attempt, error = %e, "rpc transient error, retrying");
                        last_err = Some(e);
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

/// Map a tonic `Status` to a `DiskdbClientError`.
fn map_status(status: &tonic::Status) -> DiskdbClientError {
    match status.code() {
        tonic::Code::ResourceExhausted => DiskdbClientError::Rpc(format!("no space: {status}")),
        tonic::Code::NotFound => DiskdbClientError::Rpc(format!("not found: {status}")),
        tonic::Code::InvalidArgument => DiskdbClientError::Rpc(format!("invalid argument: {status}")),
        tonic::Code::PermissionDenied => DiskdbClientError::Rpc(format!("permission denied: {status}")),
        _ => DiskdbClientError::Rpc(format!("{status}")),
    }
}

/// Normalize a service-registry endpoint for tonic `Channel`:
/// prepend `http://` if no scheme is present, and rewrite `0.0.0.0`
/// to `127.0.0.1` so the channel connects to a loopback address.
#[must_use]
pub fn normalize_endpoint(endpoint: &str) -> String {
    let with_scheme = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    with_scheme.replacen("://0.0.0.0:", "://127.0.0.1:", 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_config_default() {
        let r = RetryConfig::default();
        assert_eq!(r.max_retries, 3);
        assert_eq!(r.initial_backoff, Duration::from_millis(50));
    }

    #[test]
    fn map_status_resource_exhausted() {
        let s = tonic::Status::resource_exhausted("no space");
        let e = map_status(&s);
        assert!(matches!(e, DiskdbClientError::Rpc(_)));
    }

    #[test]
    fn map_status_not_found() {
        let s = tonic::Status::not_found("missing");
        let e = map_status(&s);
        assert!(matches!(e, DiskdbClientError::Rpc(_)));
    }

    fn fresh_client() -> DiskdbClient {
        let kv = crow_kv_client::CrowkvClient::new(crow_kv_client::ClientConfig::new(Vec::new()));
        let svc = crow_kv_client::ServiceRegistryClient::new(kv);
        DiskdbClient::new(svc)
    }

    #[tokio::test]
    async fn trigger_scan_empty_cache_returns_unreachable() {
        let client = fresh_client();
        let err = client.trigger_scan(None).await.unwrap_err();
        assert!(matches!(err, DiskdbClientError::Unreachable(_)));
    }

    #[tokio::test]
    async fn get_scan_status_empty_cache_returns_unreachable() {
        let client = fresh_client();
        let err = client.get_scan_status(None).await.unwrap_err();
        assert!(matches!(err, DiskdbClientError::Unreachable(_)));
    }

    #[tokio::test]
    async fn rebuild_zone_bitmap_unknown_disk_returns_unreachable() {
        let client = fresh_client();
        let disk_id = DiskId { high: 0, low: 1 };
        let err = client.rebuild_zone_bitmap(disk_id, 0).await.unwrap_err();
        assert!(matches!(err, DiskdbClientError::Unreachable(_)));
    }
}
