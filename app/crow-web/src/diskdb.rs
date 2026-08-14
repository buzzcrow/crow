// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! REST proxy for diskdb runtime RPCs under `/api/diskdb/` plus
//! `PUT /api/disks/:disk_id/status`. The console web layer routes
//! CLI and web UI requests through here → `DiskdbClient` → gRPC →
//! `crow-diskdb`. No direct gRPC from the browser or CLI.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crow_diskdb_client::DiskdbClient;
use crow_kv_client::ServiceRegistryClient;
use crow_protocol::common::{DiskGroupUsageSummary, DiskdbExtra, HwStatus, InstanceValue};
use crow_protocol::common_type::InstanceId;
use crow_protocol::diskdb::rpc::{
    CompactZoneRequest, CompactZoneResponse, DiskGroupInfo, DiskInfo, QueryCapacityStatsRequest,
    QueryCapacityStatsResponse, RebuildZoneBitmapResponse, RecalcDiskUsageRequest, RecalcDiskUsageResponse,
    TriggerScanResponse, ZoneUsage,
};

use crate::error::{err_400, err_404, err_502, ErrorBody};
use crate::mgmt::{build_hardware_client, grpc_endpoint_for_node};
use crate::state::AppState;

// ── lazy DiskdbClient init ───────────────────────────────────────

/// Build a `DiskdbClient` from the group-0 endpoint (mirror
/// `build_hardware_client`). Returns `None` when no group-0 endpoint
/// is known.
pub(crate) async fn build_diskdb_client(state: &AppState) -> Option<DiskdbClient> {
    let snap = state.monitor_cache.snapshot().await;
    for node_id in snap.keys() {
        if let Some(ep) = grpc_endpoint_for_node(state, *node_id, 0).await {
            let kv = crow_kv_client::CrowkvClient::new(crow_kv_client::ClientConfig::new(Vec::new()));
            kv.seed_leader(0, 0, ep);
            let svc = ServiceRegistryClient::new(kv);
            let client = DiskdbClient::new(svc);
            if let Err(e) = client.refresh_endpoints().await {
                warn!(error = %e, "build_diskdb_client: refresh_endpoints failed");
            }
            return Some(client);
        }
    }
    warn!("build_diskdb_client: no group-0 endpoint found in monitor cache");
    None
}

/// Get or lazily init the `DiskdbClient` from `AppState`.
async fn get_diskdb_client(state: &AppState) -> Result<DiskdbClient, (StatusCode, Json<ErrorBody>)> {
    // Check the lazy slot first.
    if let Some(client) = state.diskdb_client.read().await.clone() {
        return Ok(client);
    }
    let client = build_diskdb_client(state)
        .await
        .ok_or_else(|| err_502("no group-0 endpoint; cluster not initialized"))?;
    // Store for reuse (best-effort; another request may race — fine).
    let mut slot = state.diskdb_client.write().await;
    *slot = Some(client.clone());
    Ok(client)
}

// ── response wrappers (serde-friendly mirrors of proto types) ─────

/// `QueryCapacityStatsResponse` is not `Serialize` (prost). This
/// wrapper serializes the nested `disk_groups` for the REST layer.
#[derive(Debug, Serialize)]
pub struct UsageResponse {
    pub disk_groups: Vec<DiskGroupInfoDto>,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
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

fn disk_id_to_string(d: &crow_protocol::common::DiskId) -> String {
    format!("{:016x}{:016x}", d.high, d.low)
}

impl From<&DiskGroupInfo> for DiskGroupInfoDto {
    fn from(g: &DiskGroupInfo) -> Self {
        Self {
            rack_id: g.rack_id,
            node_id: g.node_id,
            disk_group_id: g.disk_group_id,
            status: g.status,
            disk_ids: g.disk_ids.iter().map(disk_id_to_string).collect(),
            disks: g.disks.iter().map(DiskInfoDto::from).collect(),
            capacity_bytes: g.capacity_bytes,
            busy_bytes: g.busy_bytes,
            free_bytes: g.free_bytes,
            allocatable_disk_count: g.allocatable_disk_count,
        }
    }
}

impl From<&DiskInfo> for DiskInfoDto {
    fn from(d: &DiskInfo) -> Self {
        Self {
            rack_id: d.rack_id,
            node_id: d.node_id,
            disk_group_id: d.disk_group_id,
            disk_id: d.disk_id.as_ref().map(disk_id_to_string).unwrap_or_default(),
            disk_type: d.disk_type,
            capacity_units: d.capacity_units,
            zone_size_units: d.zone_size_units,
            unit_size_bytes: d.unit_size_bytes,
            zone_count: d.zone_count,
            status: d.status,
            busy_units: d.busy_units,
            free_units: d.free_units,
            capacity_bytes: d.capacity_bytes,
            busy_bytes: d.busy_bytes,
            free_bytes: d.free_bytes,
            active_zone_count: d.active_zone_count,
            zone_usages: d.zone_usages.iter().map(ZoneUsageDto::from).collect(),
        }
    }
}

impl From<&ZoneUsage> for ZoneUsageDto {
    fn from(z: &ZoneUsage) -> Self {
        Self {
            zone_index: z.zone_index,
            capacity_bytes: z.capacity_bytes,
            busy_bytes: z.busy_bytes,
            free_bytes: z.free_bytes,
            busy_block_count: z.busy_block_count,
            free_block_count: z.free_block_count,
            alloc_state: z.alloc_state,
            usage_bitmap: z.usage_bitmap.as_ref().map(hex::encode),
        }
    }
}

/// Merge two `QueryCapacityStatsResponse`s by `disk_group_id`,
/// summing capacity/busy/free. Per-disk details from the first
/// occurrence are retained (a merge is only done at the cluster
/// overview level where per-disk detail is not needed).
fn merge_capacity_responses(responses: Vec<QueryCapacityStatsResponse>) -> UsageResponse {
    let mut merged: Vec<DiskGroupInfo> = Vec::new();
    for resp in responses {
        for g in resp.disk_groups {
            if let Some(existing) = merged.iter_mut().find(|m| m.disk_group_id == g.disk_group_id) {
                existing.capacity_bytes += g.capacity_bytes;
                existing.busy_bytes += g.busy_bytes;
                existing.free_bytes += g.free_bytes;
                existing.allocatable_disk_count += g.allocatable_disk_count;
                existing.disk_ids.extend(g.disk_ids);
                existing.disks.extend(g.disks);
            } else {
                merged.push(g);
            }
        }
    }
    UsageResponse {
        disk_groups: merged.iter().map(DiskGroupInfoDto::from).collect(),
    }
}

// ── instance info ────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct DiskdbInstanceInfo {
    pub instance_id: u64,
    pub grpc_endpoint: String,
    pub last_heartbeat_ms: u64,
    pub owned_dg_ids: Vec<u64>,
    pub group_usages: Vec<DiskGroupUsageSummary>,
}

impl From<(InstanceId, InstanceValue)> for DiskdbInstanceInfo {
    fn from((id, val): (InstanceId, InstanceValue)) -> Self {
        let (owned_dg_ids, group_usages) = val
            .extra
            .as_ref()
            .and_then(|e| e.diskdb.as_ref())
            .map(|d: &DiskdbExtra| (d.owned_dg_ids.clone(), d.group_usages.clone()))
            .unwrap_or_default();
        Self {
            instance_id: id,
            grpc_endpoint: val.grpc_endpoint,
            last_heartbeat_ms: val.last_heartbeat_ms,
            owned_dg_ids,
            group_usages,
        }
    }
}

// ── handlers ─────────────────────────────────────────────────────

/// `GET /api/diskdb/instances` — all diskdb instances from the
/// service registry (no gRPC fan-out).
///
/// # Errors
/// Returns `502` if the service registry read fails.
pub async fn http_list_diskdb_instances(
    State(state): State<AppState>,
) -> Result<Json<Vec<DiskdbInstanceInfo>>, (StatusCode, Json<ErrorBody>)> {
    let hw = build_hardware_client(&state)
        .await
        .ok_or_else(|| err_502("no group-0 endpoint; cluster not initialized"))?;
    let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
    let instances = svc
        .read_all_diskdb_instances()
        .await
        .map_err(|e| err_502(format!("read_all_diskdb_instances: {e}")))?;
    Ok(Json(
        instances.into_iter().map(DiskdbInstanceInfo::from).collect(),
    ))
}

/// `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` —
/// `QueryCapacityStats` drill-down. When `dg` is omitted, iterates
/// all registered instances and merges for cluster-wide totals.
///
/// # Errors
/// Returns `502` on gRPC or registry errors, `400` on invalid params.
pub async fn http_diskdb_usage(
    State(state): State<AppState>,
    Query(q): Query<UsageQuery>,
) -> Result<Json<UsageResponse>, (StatusCode, Json<ErrorBody>)> {
    if let Some(dg) = q.dg {
        // Single-instance drill-down.
        let client = get_diskdb_client(&state).await?;
        let req = QueryCapacityStatsRequest {
            disk_group_id: dg,
            disk_id: q.disk.as_deref().and_then(parse_disk_id),
            zone_index: q.zone,
        };
        let resp = client
            .query_capacity_stats(req)
            .await
            .map_err(|e| err_502(format!("query_capacity_stats: {e}")))?;
        Ok(Json(UsageResponse {
            disk_groups: resp.disk_groups.iter().map(DiskGroupInfoDto::from).collect(),
        }))
    } else {
        // Cluster-wide merge: iterate all instances.
        let hw = build_hardware_client(&state)
            .await
            .ok_or_else(|| err_502("no group-0 endpoint"))?;
        let svc = ServiceRegistryClient::from_shared(hw.shared_kv());
        let instances = svc
            .read_all_diskdb_instances()
            .await
            .map_err(|e| err_502(format!("read_all_diskdb_instances: {e}")))?;
        let mut responses = Vec::new();
        for (_id, val) in instances {
            let endpoint = val.grpc_endpoint.clone();
            let req = QueryCapacityStatsRequest {
                disk_group_id: 0,
                disk_id: None,
                zone_index: None,
            };
            match query_instance_direct(&endpoint, req).await {
                Ok(resp) => responses.push(resp),
                Err(e) => {
                    warn!(endpoint = %endpoint, error = %e, "cluster merge: instance query failed; skipping");
                }
            }
        }
        Ok(Json(merge_capacity_responses(responses)))
    }
}

/// Query a specific instance directly (bypass the dg cache).
async fn query_instance_direct(
    endpoint: &str,
    req: QueryCapacityStatsRequest,
) -> Result<QueryCapacityStatsResponse, String> {
    use crow_protocol::diskdb::rpc::diskdb_service_client::DiskdbServiceClient;
    let channel = tonic::transport::Channel::from_shared(endpoint.to_string())
        .map_err(|e| format!("invalid endpoint: {e}"))?
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .connect_lazy();
    let mut grpc = DiskdbServiceClient::new(channel);
    grpc.query_capacity_stats(req)
        .await
        .map(tonic::Response::into_inner)
        .map_err(|e| format!("gRPC: {e}"))
}

#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    #[serde(default)]
    dg: Option<u64>,
    #[serde(default)]
    disk: Option<String>,
    #[serde(default)]
    zone: Option<u32>,
}

fn parse_disk_id(s: &str) -> Option<crow_protocol::common::DiskId> {
    if s.len() == 32 {
        let high = u64::from_str_radix(&s[..16], 16).ok()?;
        let low = u64::from_str_radix(&s[16..], 16).ok()?;
        Some(crow_protocol::common::DiskId { high, low })
    } else {
        None
    }
}

/// `GET /api/diskdb/scan-status?dg=<id>` — `GetScanStatus`.
///
/// # Errors
/// Returns `502` on gRPC errors.
pub async fn http_diskdb_scan_status(
    State(state): State<AppState>,
    Query(q): Query<DgQuery>,
) -> Result<Json<ScanStatusResponse>, (StatusCode, Json<ErrorBody>)> {
    let client = get_diskdb_client(&state).await?;
    let resp = client
        .get_scan_status(q.dg)
        .await
        .map_err(|e| err_502(format!("get_scan_status: {e}")))?;
    Ok(Json(ScanStatusResponse::from(resp)))
}

/// `POST /api/diskdb/scan` — `TriggerScan`.
///
/// # Errors
/// Returns `502` on gRPC errors.
pub async fn http_diskdb_scan(
    State(state): State<AppState>,
    body: Json<DgBody>,
) -> Result<Json<ScanStatusResponse>, (StatusCode, Json<ErrorBody>)> {
    let client = get_diskdb_client(&state).await?;
    let resp = client
        .trigger_scan(body.dg)
        .await
        .map_err(|e| err_502(format!("trigger_scan: {e}")))?;
    Ok(Json(ScanStatusResponse::from(resp)))
}

/// `POST /api/diskdb/recalc` — `RecalcDiskUsage`.
///
/// # Errors
/// Returns `502` on gRPC errors.
pub async fn http_diskdb_recalc(
    State(state): State<AppState>,
    body: Json<DgBody>,
) -> Result<Json<RecalcResultResponse>, (StatusCode, Json<ErrorBody>)> {
    let client = get_diskdb_client(&state).await?;
    let req = RecalcDiskUsageRequest {
        disk_group_id: body.dg,
    };
    let resp = client
        .recalc_disk_usage(req)
        .await
        .map_err(|e| err_502(format!("recalc_disk_usage: {e}")))?;
    Ok(Json(RecalcResultResponse::from(resp)))
}

/// `POST /api/diskdb/compact` — `CompactZone`.
///
/// # Errors
/// Returns `400` if `disk_id` is missing, `502` on gRPC errors.
pub async fn http_diskdb_compact(
    State(state): State<AppState>,
    Json(body): Json<CompactBody>,
) -> Result<Json<CompactResultResponse>, (StatusCode, Json<ErrorBody>)> {
    let disk_id = parse_disk_id(&body.disk_id).ok_or_else(|| err_400("invalid or missing disk_id"))?;
    let client = get_diskdb_client(&state).await?;
    let req = CompactZoneRequest {
        disk_id: Some(disk_id),
        zone_indices: body.zone_indices.unwrap_or_default(),
    };
    let resp = client
        .compact_zone(req)
        .await
        .map_err(|e| err_502(format!("compact_zone: {e}")))?;
    Ok(Json(CompactResultResponse::from(resp)))
}

/// `POST /api/diskdb/rebuild` — `RebuildZoneBitmap`.
///
/// # Errors
/// Returns `400` if `disk_id` is missing, `502` on gRPC errors.
pub async fn http_diskdb_rebuild(
    State(state): State<AppState>,
    Json(body): Json<RebuildBody>,
) -> Result<Json<RebuildResultResponse>, (StatusCode, Json<ErrorBody>)> {
    let disk_id = parse_disk_id(&body.disk_id).ok_or_else(|| err_400("invalid or missing disk_id"))?;
    let client = get_diskdb_client(&state).await?;
    let zone_index = body.zone_index.unwrap_or(u32::MAX);
    let resp = client
        .rebuild_zone_bitmap(disk_id, zone_index)
        .await
        .map_err(|e| err_502(format!("rebuild_zone_bitmap: {e}")))?;
    Ok(Json(RebuildResultResponse::from(resp)))
}

/// `PUT /api/disks/:disk_id/status` — set a disk's `HwStatus` via
/// `HardwareClient.set_disk_status`. Resolves the disk's
/// rack/node/dg from config.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the disk is not in config, `400` on invalid
/// status, `502` on group-0 write failure.
pub async fn http_set_disk_status(
    State(state): State<AppState>,
    Path(disk_id): Path<String>,
    Json(body): Json<SetStatusBody>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let (rack_id, node_id, dg_id, disk_id_proto) = {
        let cfg = state.config.read().unwrap();
        let disk = cfg
            .disks
            .iter()
            .find(|d| d.disk_id == disk_id)
            .cloned()
            .ok_or_else(|| err_404(format!("disk {disk_id} not found")))?;
        let did = parse_disk_id(&disk.disk_id)
            .ok_or_else(|| err_400(format!("invalid disk_id format: {}", disk.disk_id)))?;
        (disk.rack_id, disk.node_id, disk.disk_group_id, did)
    };
    let status =
        parse_hw_status(&body.status).ok_or_else(|| err_400(format!("invalid status: {}", body.status)))?;
    let hw = build_hardware_client(&state)
        .await
        .ok_or_else(|| err_502("no group-0 endpoint; cluster not initialized"))?;
    hw.set_disk_status(rack_id, node_id, dg_id, &disk_id_proto, status)
        .await
        .map_err(|e| err_502(format!("set_disk_status: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}

// ── request/response DTOs ────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct DgQuery {
    #[serde(default)]
    dg: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DgBody {
    #[serde(default)]
    pub dg: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CompactBody {
    pub disk_id: String,
    #[serde(default)]
    pub zone_indices: Option<Vec<u32>>,
}

#[derive(Debug, Deserialize)]
pub struct RebuildBody {
    pub disk_id: String,
    #[serde(default)]
    pub zone_index: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SetStatusBody {
    pub status: String,
}

#[derive(Debug, Serialize)]
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

impl From<crow_protocol::diskdb::rpc::ScanSummary> for ScanSummaryDto {
    fn from(s: crow_protocol::diskdb::rpc::ScanSummary) -> Self {
        Self {
            started_at_ms: s.started_at_ms,
            duration_ms: s.duration_ms,
            zones_scanned: s.zones_scanned,
            zones_skipped_active: s.zones_skipped_active,
            zones_skipped_compacting: s.zones_skipped_compacting,
            ghost_busy: s.ghost_busy,
            ghost_free: s.ghost_free,
            uncompacted_lag: s.uncompacted_lag,
            corrupt_snapshots: s.corrupt_snapshots,
            corrupt_records: s.corrupt_records,
            owner_mismatches: s.owner_mismatches,
            leak_status: s.leak_status,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ScanStatusResponse {
    pub summary: Option<ScanSummaryDto>,
    pub has_run: bool,
    pub scan_in_progress: bool,
}

impl From<TriggerScanResponse> for ScanStatusResponse {
    fn from(r: TriggerScanResponse) -> Self {
        let has_run = r.summary.is_some();
        Self {
            summary: r.summary.map(ScanSummaryDto::from),
            has_run,
            scan_in_progress: r.scan_in_progress,
        }
    }
}

impl From<crow_protocol::diskdb::rpc::GetScanStatusResponse> for ScanStatusResponse {
    fn from(r: crow_protocol::diskdb::rpc::GetScanStatusResponse) -> Self {
        Self {
            summary: r.summary.map(ScanSummaryDto::from),
            has_run: r.has_run,
            scan_in_progress: false,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RecalcResultResponse {
    pub results: Vec<DiskGroupRecalcResultDto>,
}

#[derive(Debug, Serialize)]
pub struct DiskGroupRecalcResultDto {
    pub disk_group_id: u64,
    pub drift_detected: bool,
    pub zones: Vec<ZoneRecalcResultDto>,
}

#[derive(Debug, Serialize)]
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

impl From<RecalcDiskUsageResponse> for RecalcResultResponse {
    fn from(r: RecalcDiskUsageResponse) -> Self {
        Self {
            results: r
                .results
                .into_iter()
                .map(|g| DiskGroupRecalcResultDto {
                    disk_group_id: g.disk_group_id,
                    drift_detected: g.drift_detected,
                    zones: g
                        .zones
                        .into_iter()
                        .map(|z| ZoneRecalcResultDto {
                            disk_id: z.disk_id.as_ref().map(disk_id_to_string).unwrap_or_default(),
                            zone_index: z.zone_index,
                            matches: z.matches,
                            drift_detected: z.drift_detected,
                            live_busy_blocks: z.live_busy_blocks,
                            replayed_busy_blocks: z.replayed_busy_blocks,
                            live_snapshot_slot: z.live_snapshot_slot,
                            replayed_snapshot_slot: z.replayed_snapshot_slot,
                            fallback_reason: z.fallback_reason,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CompactResultResponse {
    pub compacted_zone_count: u32,
    pub total_free_records_deleted: u32,
    pub zones: Vec<ZoneCompactionResultDto>,
}

#[derive(Debug, Serialize)]
pub struct ZoneCompactionResultDto {
    pub zone_index: u32,
    pub success: bool,
    pub free_records_deleted: u32,
    pub error: Option<String>,
}

impl From<CompactZoneResponse> for CompactResultResponse {
    fn from(r: CompactZoneResponse) -> Self {
        Self {
            compacted_zone_count: r.compacted_zone_count,
            total_free_records_deleted: r.total_free_records_deleted,
            zones: r
                .zones
                .into_iter()
                .map(|z| ZoneCompactionResultDto {
                    zone_index: z.zone_index,
                    success: z.success,
                    free_records_deleted: z.free_records_deleted,
                    error: z.error,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RebuildResultResponse {
    pub rebuilt_zone_count: u32,
    pub total_busy_units: u64,
    pub total_free_units: u64,
}

impl From<RebuildZoneBitmapResponse> for RebuildResultResponse {
    fn from(r: RebuildZoneBitmapResponse) -> Self {
        Self {
            rebuilt_zone_count: r.rebuilt_zone_count,
            total_busy_units: r.total_busy_units,
            total_free_units: r.total_free_units,
        }
    }
}

fn parse_hw_status(s: &str) -> Option<HwStatus> {
    match s.to_ascii_uppercase().as_str() {
        "INIT" | "HW_STATUS_INIT" => Some(HwStatus::Init),
        "UP" | "HW_STATUS_UP" => Some(HwStatus::Up),
        "MAINTENANCE" | "HW_STATUS_MAINTENANCE" => Some(HwStatus::Maintenance),
        "SUSPECT" | "HW_STATUS_SUSPECT" => Some(HwStatus::Suspect),
        "MISSING" | "HW_STATUS_MISSING" => Some(HwStatus::Missing),
        "BAD" | "HW_STATUS_BAD" => Some(HwStatus::Bad),
        "OFFLINE" | "HW_STATUS_OFFLINE" => Some(HwStatus::Offline),
        _ => None,
    }
}
