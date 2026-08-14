// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Console-side diskdb models and `ConsoleClient` extension methods.
//!
//! These types mirror the JSON shapes returned by `crow-web`'s
//! `/api/diskdb/*` REST proxy (see `app/crow-web/src/diskdb.rs`).
//! The CLI and other consumers use them to parse responses without
//! pulling in prost-generated proto types.

use serde::{Deserialize, Serialize};

use crate::clients::console::ConsoleClient;
use crate::error::Result;

// ── Response models (mirror the DTOs in crow-web/src/diskdb.rs) ────

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiskdbInstanceInfo {
    pub instance_id: u64,
    pub grpc_endpoint: String,
    pub last_heartbeat_ms: u64,
    pub owned_dg_ids: Vec<u64>,
    pub group_usages: Vec<DiskGroupUsageSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiskGroupUsageSummary {
    pub disk_group_id: u64,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub disk_count: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UsageResponse {
    pub disk_groups: Vec<DiskGroupInfoDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiskGroupInfoDto {
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub status: i32,
    pub disk_ids: Vec<String>,
    pub disks: Vec<DiskInfoDto>,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub allocatable_disk_count: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiskInfoDto {
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: String,
    pub disk_type: i32,
    pub capacity_units: u64,
    pub zone_size_units: u64,
    pub unit_size_bytes: u32,
    pub zone_count: u32,
    pub status: i32,
    pub busy_units: u64,
    pub free_units: u64,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub active_zone_count: u32,
    pub zone_usages: Vec<ZoneUsageDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ZoneUsageDto {
    pub zone_index: u32,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub busy_block_count: u32,
    pub free_block_count: u32,
    pub alloc_state: i32,
    pub usage_bitmap: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScanStatusResponse {
    pub summary: Option<ScanSummaryDto>,
    pub has_run: bool,
    pub scan_in_progress: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScanSummaryDto {
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub zones_scanned: u64,
    pub zones_skipped_active: u64,
    pub zones_skipped_compacting: u64,
    pub ghost_busy: u64,
    pub ghost_free: u64,
    pub uncompacted_lag: u64,
    pub corrupt_snapshots: u64,
    pub corrupt_records: u64,
    pub owner_mismatches: u64,
    pub leak_status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RecalcResultResponse {
    pub results: Vec<DiskGroupRecalcResultDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiskGroupRecalcResultDto {
    pub disk_group_id: u64,
    pub drift_detected: bool,
    pub zones: Vec<ZoneRecalcResultDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ZoneRecalcResultDto {
    pub disk_id: String,
    pub zone_index: u32,
    pub matches: bool,
    pub drift_detected: bool,
    pub live_busy_blocks: u32,
    pub replayed_busy_blocks: u32,
    pub live_snapshot_slot: u64,
    pub replayed_snapshot_slot: u64,
    pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompactResultResponse {
    pub compacted_zone_count: u32,
    pub total_free_records_deleted: u32,
    pub zones: Vec<ZoneCompactionResultDto>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ZoneCompactionResultDto {
    pub zone_index: u32,
    pub success: bool,
    pub free_records_deleted: u32,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RebuildResultResponse {
    pub rebuilt_zone_count: u32,
    pub total_busy_units: u64,
    pub total_free_units: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiskdbDeployResult {
    pub node_id: u64,
    pub mgmt_url: String,
    pub grpc_url: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StopResult {
    pub sent: bool,
}

// ── Request bodies ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct DgBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dg: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactBody {
    pub disk_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone_indices: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RebuildBody {
    pub disk_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone_index: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SetStatusBody {
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeployDiskdbBody {
    pub mgmt_port: u16,
    pub grpc_port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<String>,
}

// ── ConsoleClient extension methods ───────────────────────────────

impl ConsoleClient {
    /// `GET /api/diskdb/instances` — list all diskdb instances.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn list_diskdb_instances(&self) -> Result<Vec<DiskdbInstanceInfo>> {
        self.get_json("/api/diskdb/instances").await
    }

    /// `GET /api/diskdb/usage` — capacity usage drill-down. When
    /// `dg` is `None`, returns cluster-wide merged totals.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn diskdb_usage(
        &self,
        dg: Option<u64>,
        disk: Option<&str>,
        zone: Option<u32>,
    ) -> Result<UsageResponse> {
        let mut path = String::from("/api/diskdb/usage");
        let mut first = true;
        let mut sep = || {
            let s = if first { "?" } else { "&" };
            first = false;
            s
        };
        if let Some(d) = dg {
            path.push_str(sep());
            path.push_str("dg=");
            path.push_str(&d.to_string());
        }
        if let Some(d) = disk {
            path.push_str(sep());
            path.push_str("disk=");
            path.push_str(d);
        }
        if let Some(z) = zone {
            path.push_str(sep());
            path.push_str("zone=");
            path.push_str(&z.to_string());
        }
        self.get_json(&path).await
    }

    /// `GET /api/diskdb/scan-status` — get scan status.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn diskdb_scan_status(&self, dg: Option<u64>) -> Result<ScanStatusResponse> {
        let path = match dg {
            Some(d) => format!("/api/diskdb/scan-status?dg={d}"),
            None => String::from("/api/diskdb/scan-status"),
        };
        self.get_json(&path).await
    }

    /// `POST /api/diskdb/scan` — trigger a scan.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn diskdb_trigger_scan(&self, dg: Option<u64>) -> Result<ScanStatusResponse> {
        self.post_json("/api/diskdb/scan", &DgBody { dg }).await
    }

    /// `POST /api/diskdb/recalc` — recalc disk usage.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn diskdb_recalc(&self, dg: Option<u64>) -> Result<RecalcResultResponse> {
        self.post_json("/api/diskdb/recalc", &DgBody { dg }).await
    }

    /// `POST /api/diskdb/compact` — compact zones.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn diskdb_compact(
        &self,
        disk_id: &str,
        zone_indices: Option<Vec<u32>>,
    ) -> Result<CompactResultResponse> {
        self.post_json(
            "/api/diskdb/compact",
            &CompactBody {
                disk_id: disk_id.to_string(),
                zone_indices,
            },
        )
        .await
    }

    /// `POST /api/diskdb/rebuild` — rebuild zone bitmap.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn diskdb_rebuild(
        &self,
        disk_id: &str,
        zone_index: Option<u32>,
    ) -> Result<RebuildResultResponse> {
        self.post_json(
            "/api/diskdb/rebuild",
            &RebuildBody {
                disk_id: disk_id.to_string(),
                zone_index,
            },
        )
        .await
    }

    /// `PUT /api/disks/:disk_id/status` — set disk status.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn set_disk_status(&self, disk_id: &str, status: &str) -> Result<()> {
        let path = format!("/api/disks/{disk_id}/status");
        self.put_json_no_response(
            &path,
            &SetStatusBody {
                status: status.to_string(),
            },
        )
        .await
    }

    /// `POST /api/nodes/:id/diskdb/deploy` — deploy diskdb on a node.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn deploy_diskdb(&self, node_id: u64, body: &DeployDiskdbBody) -> Result<DiskdbDeployResult> {
        let path = format!("/api/nodes/{node_id}/diskdb/deploy");
        self.post_json(&path, body).await
    }

    /// `POST /api/nodes/:id/diskdb/restart` — restart diskdb on a node.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn restart_diskdb(&self, node_id: u64) -> Result<DiskdbDeployResult> {
        let path = format!("/api/nodes/{node_id}/diskdb/restart");
        self.post_json(&path, &serde_json::Value::Null).await
    }

    /// `POST /api/nodes/:id/diskdb/stop` — stop diskdb on a node.
    ///
    /// # Errors
    /// Surfaces HTTP / decode failures as `Error::UpstreamRpc`.
    pub async fn stop_diskdb(&self, node_id: u64) -> Result<StopResult> {
        let path = format!("/api/nodes/{node_id}/diskdb/stop");
        self.post_json(&path, &serde_json::Value::Null).await
    }

    /// `DELETE /api/nodes/:id/diskdb` — stop (best-effort) and remove
    /// the diskdb `ServerEntry`. Use when the PID is lost (console
    /// restarted) and `stop` returns 400.
    ///
    /// # Errors
    /// Surfaces HTTP failures as `Error::UpstreamRpc`.
    pub async fn delete_diskdb(&self, node_id: u64) -> Result<()> {
        let path = format!("/api/nodes/{node_id}/diskdb");
        self.delete_no_response(&path).await
    }
}
