// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Admin/management operations for [`CrowkvClient`]: snapshot lifecycle
//! (create, list, scan, release).

use bytes::Bytes;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::{
    CreateSnapshotRequest, CreateSnapshotResponse, ListSnapshotsRequest, ReadMode, ReleaseSnapshotRequest,
    ReleaseSnapshotResponse, SnapshotInfo, SnapshotScanRequest, SnapshotScanResponse,
};

use crate::client::CrowkvClient;
use crate::error::{Error, Result};

impl CrowkvClient {
    /// R59: Create a point-in-time-consistent snapshot. Flushes L0 → L1
    /// and pins the durable view at `last_applied_slot`. Returns a
    /// snapshot handle for use with `snapshot_scan`/`release_snapshot`.
    ///
    /// # Errors
    /// `Error::NotLeader` if this client is not connected to the leader
    /// (for linearizable mode). `Error::Transport` on transport failure.
    pub async fn create_snapshot(
        &self,
        store_id: u64,
        group_id: u64,
        read_mode: ReadMode,
        min_slot: Option<u64>,
    ) -> Result<CreateSnapshotResponse> {
        let min_slot = self.resolve_min_slot(store_id, group_id, read_mode, min_slot);
        let endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let req = CreateSnapshotRequest {
            group_id,
            read_mode: read_mode as i32,
            min_slot,
        };
        let channel = self.pool.get(&endpoint)?;
        let _in_flight = self.incr_in_flight(&endpoint);
        let resp = KvServiceClient::new(channel)
            .create_snapshot(req)
            .await
            .map_err(|e| Error::Transport {
                endpoint: endpoint.clone(),
                status: e.to_string(),
            })?
            .into_inner();
        Ok(resp)
    }

    /// R59: List active snapshot handles for a group.
    ///
    /// # Errors
    /// `Error::NoLeader` if no leader is known. `Error::Transport` on
    /// transport failure.
    pub async fn list_snapshots(&self, store_id: u64, group_id: u64) -> Result<Vec<SnapshotInfo>> {
        let endpoint = self
            .topology
            .leader(store_id, group_id)
            .ok_or(Error::NoLeader { store_id, group_id })?;
        let req = ListSnapshotsRequest { group_id };
        let channel = self.pool.get(&endpoint)?;
        let _in_flight = self.incr_in_flight(&endpoint);
        let resp = KvServiceClient::new(channel)
            .list_snapshots(req)
            .await
            .map_err(|e| Error::Transport {
                endpoint: endpoint.clone(),
                status: e.to_string(),
            })?
            .into_inner();
        Ok(resp.snapshots)
    }

    /// R59: Iterate a pinned snapshot with prefix/pagination. Returns
    /// one page of results; the caller advances `start_after` to the
    /// last returned key for the next page. The snapshot handle must
    /// have been created by `create_snapshot` and not yet released or
    /// expired.
    ///
    /// # Errors
    /// `Error::NoLeader` if no leader is known. `Error::Transport` on
    /// transport failure.
    pub async fn snapshot_scan(
        &self,
        store_id: u64,
        group_id: u64,
        snapshot_handle: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> Result<SnapshotScanResponse> {
        let endpoint = self
            .topology
            .leader(store_id, group_id)
            .ok_or(Error::NoLeader { store_id, group_id })?;
        let req = SnapshotScanRequest {
            snapshot_handle,
            prefix: Bytes::copy_from_slice(prefix),
            start_after: Bytes::copy_from_slice(start_after),
            limit,
            group_id,
        };
        let channel = self.pool.get(&endpoint)?;
        let _in_flight = self.incr_in_flight(&endpoint);
        let resp = KvServiceClient::new(channel)
            .snapshot_scan(req)
            .await
            .map_err(|e| Error::Transport {
                endpoint: endpoint.clone(),
                status: e.to_string(),
            })?
            .into_inner();
        Ok(resp)
    }

    /// R59: Release a snapshot handle, dropping the pinned view.
    ///
    /// # Errors
    /// `Error::NoLeader` if no leader is known. `Error::Transport` on
    /// transport failure.
    pub async fn release_snapshot(
        &self,
        store_id: u64,
        group_id: u64,
        snapshot_handle: u64,
    ) -> Result<ReleaseSnapshotResponse> {
        let endpoint = self
            .topology
            .leader(store_id, group_id)
            .ok_or(Error::NoLeader { store_id, group_id })?;
        let req = ReleaseSnapshotRequest {
            snapshot_handle,
            group_id,
        };
        let channel = self.pool.get(&endpoint)?;
        let _in_flight = self.incr_in_flight(&endpoint);
        let resp = KvServiceClient::new(channel)
            .release_snapshot(req)
            .await
            .map_err(|e| Error::Transport {
                endpoint: endpoint.clone(),
                status: e.to_string(),
            })?
            .into_inner();
        Ok(resp)
    }
}
