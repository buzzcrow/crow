// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Admin/management operations for [`CrowdbClient`]: snapshot lifecycle
//! (create, list, scan, release).

use crowdb_kv::rpc::{
    CreateSnapshotResponse, ReadMode, ReleaseSnapshotResponse, SnapshotInfo, SnapshotScanResponse,
};

use crate::client::CrowdbClient;
use crate::error::{Error, Result};

impl CrowdbClient {
    /// Create a point-in-time-consistent snapshot. Flushes L0 → L1
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
        let t = self.rpc_transport().ok_or_else(|| Error::Transport {
            endpoint: endpoint.clone(),
            status: "rpc transport not set".into(),
        })?;
        let _in_flight = self.incr_in_flight(&endpoint);
        t.send_create_snapshot(&endpoint, group_id, read_mode, min_slot)
            .await
    }

    /// List active snapshot handles for a group.
    ///
    /// # Errors
    /// `Error::NoLeader` if no leader is known. `Error::Transport` on
    /// transport failure.
    pub async fn list_snapshots(&self, store_id: u64, group_id: u64) -> Result<Vec<SnapshotInfo>> {
        let endpoint = self
            .topology
            .leader(store_id, group_id)
            .ok_or(Error::NoLeader { store_id, group_id })?;
        let t = self.rpc_transport().ok_or_else(|| Error::Transport {
            endpoint: endpoint.clone(),
            status: "rpc transport not set".into(),
        })?;
        let _in_flight = self.incr_in_flight(&endpoint);
        let resp = t.send_list_snapshots(&endpoint, group_id).await?;
        Ok(resp.snapshots)
    }

    /// Iterate a pinned snapshot with prefix/pagination. Returns
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
        let t = self.rpc_transport().ok_or_else(|| Error::Transport {
            endpoint: endpoint.clone(),
            status: "rpc transport not set".into(),
        })?;
        let _in_flight = self.incr_in_flight(&endpoint);
        t.send_snapshot_scan(&endpoint, snapshot_handle, prefix, start_after, limit, group_id)
            .await
    }

    /// Release a snapshot handle, dropping the pinned view.
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
        let t = self.rpc_transport().ok_or_else(|| Error::Transport {
            endpoint: endpoint.clone(),
            status: "rpc transport not set".into(),
        })?;
        let _in_flight = self.incr_in_flight(&endpoint);
        t.send_release_snapshot(&endpoint, snapshot_handle, group_id)
            .await
    }
}
