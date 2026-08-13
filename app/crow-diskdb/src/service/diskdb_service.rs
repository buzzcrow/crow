// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb gRPC service wiring.
//!
//! `AllocateBlocks` / `FreeBlocks` call the two-phase persistence
//! functions. `QueryCapacityStats` returns `Unimplemented` (R74).
//! `GetDiskGroupInfo` / `GetDiskInfo` read from in-memory state.

use std::sync::Arc;

use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::diskdb_service_server::{
    DiskdbService as DiskdbServiceTrait, DiskdbServiceServer,
};
use crow_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, DiskGroupInfo, DiskGroupRecalcResult, DiskInfo,
    FreeBlocksRequest, FreeResponse, GetDiskGroupInfoRequest, GetDiskGroupInfoResponse, GetDiskInfoRequest,
    GetDiskInfoResponse, QueryCapacityStatsRequest, QueryCapacityStatsResponse, RebuildZoneBitmapRequest,
    RebuildZoneBitmapResponse, RecalcDiskUsageRequest, RecalcDiskUsageResponse, ZoneRecalcResult,
    ZoneUsage as ProtoZoneUsage,
};
use crow_protocol::diskdb_type_util::DiskIdExt;
use tonic::{Request, Response, Status};

use crate::ddb_config::StorageDefaults;
use crate::ddb_kv_client::DdbKvClient;
use crate::metrics::RecalcEngine;
use crate::model::alloc;
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::ZoneUsage;
use crate::recovery::RecoveryEngine;

/// Maximum number of blocks per `AllocateBlocks` request.
const MAX_ALLOCATE_COUNT: u32 = 1024;

/// `u32::MAX` sentinel for "all zones on the disk".
const ALL_ZONES: u32 = u32::MAX;

pub struct DiskdbService {
    container: Arc<DdbDiskGroupContainer>,
    kv: Arc<DdbKvClient>,
    storage: StorageDefaults,
    recovery: Arc<RecoveryEngine>,
    recalc: Arc<RecalcEngine>,
}

impl DiskdbService {
    pub fn new(
        container: Arc<DdbDiskGroupContainer>,
        kv: Arc<DdbKvClient>,
        storage: StorageDefaults,
        recovery: Arc<RecoveryEngine>,
        recalc: Arc<RecalcEngine>,
    ) -> Self {
        Self {
            container,
            kv,
            storage,
            recovery,
            recalc,
        }
    }

    pub fn into_server(self) -> DiskdbServiceServer<Self> {
        DiskdbServiceServer::new(self)
    }

    /// Build a `DiskInfo` proto from a live `DdbDisk` with usage fields
    /// populated. When `include_zones` is true, attaches brief
    /// per-zone `ZoneUsage` entries (disk-level query).
    fn build_disk_info(disk: &Arc<crate::model::disk::DdbDisk>, include_zones: bool) -> DiskInfo {
        let dv = disk.disk_value.read().unwrap();
        let usage = disk.usage();
        let zone_usages = if include_zones {
            disk.zone_usages()
                .into_iter()
                .map(|z| Self::zone_usage_to_proto(&z))
                .collect()
        } else {
            Vec::new()
        };
        DiskInfo {
            rack_id: disk.rack_id,
            node_id: disk.node_id,
            disk_group_id: disk.disk_group_id,
            disk_id: Some(disk.disk_id),
            disk_type: dv.disk_type,
            capacity_units: dv.capacity_units,
            zone_size_units: dv.zone_size_units,
            unit_size_bytes: dv.unit_size_bytes,
            zone_count: dv.zone_count,
            status: dv.status,
            busy_units: usage.busy_bytes / u64::from(dv.unit_size_bytes).max(1),
            free_units: usage.free_bytes / u64::from(dv.unit_size_bytes).max(1),
            capacity_bytes: usage.capacity_bytes,
            busy_bytes: usage.busy_bytes,
            free_bytes: usage.free_bytes,
            active_zone_count: usage.active_zone_count,
            zone_usages,
        }
    }

    /// Convert a domain `ZoneUsage` to the proto `ZoneUsage`.
    fn zone_usage_to_proto(zu: &ZoneUsage) -> ProtoZoneUsage {
        ProtoZoneUsage {
            zone_index: zu.zone_index,
            capacity_bytes: zu.capacity_bytes,
            busy_bytes: zu.busy_bytes,
            free_bytes: zu.free_bytes,
            busy_block_count: zu.busy_block_count,
            free_block_count: zu.free_block_count,
            alloc_state: zu.alloc_state as i32,
            usage_bitmap: None,
        }
    }
}

#[tonic::async_trait]
impl DiskdbServiceTrait for DiskdbService {
    async fn allocate_blocks(
        &self,
        req: Request<AllocateBlocksRequest>,
    ) -> Result<Response<AllocateResponse>, Status> {
        let req = req.into_inner();

        // Validate unit_count.
        if req.unit_count == 0 {
            return Err(Status::invalid_argument("unit_count must be non-zero"));
        }
        let unit_size = self.storage.block_size_bytes;
        if req.unit_count * unit_size % unit_size != 0 {
            return Err(Status::invalid_argument(
                "unit_count must be aligned to block size",
            ));
        }

        // Validate count.
        if req.count == 0 {
            return Err(Status::invalid_argument("count must be non-zero"));
        }
        if req.count > MAX_ALLOCATE_COUNT {
            return Err(Status::invalid_argument(format!(
                "count must be <= {MAX_ALLOCATE_COUNT}"
            )));
        }

        // Check lifecycle phase — mutating RPCs require Up.
        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            return Err(Status::unavailable(format!(
                "diskdb not ready: phase={}",
                phase.as_str()
            )));
        }

        // Check not degraded.
        if self.container.is_degraded() {
            return Err(Status::unavailable("diskdb in degraded mode"));
        }

        // Get node.
        let dg = self.container.get_disk_group(req.disk_group_id).ok_or_else(|| {
            Status::permission_denied(format!(
                "disk-group {} not owned by this instance",
                req.disk_group_id
            ))
        })?;

        // Get owner_chunk.
        let owner_chunk = req
            .owner_chunk
            .ok_or_else(|| Status::invalid_argument("owner_chunk required"))?;

        // Convert exclude_disk_ids.
        let exclude_disks = req.exclude_disk_ids.clone();

        // Two-phase allocate.
        let segments = alloc::allocate_blocks(
            &dg,
            req.unit_count,
            req.count,
            &exclude_disks,
            &owner_chunk,
            unit_size,
            &self.kv,
            self.storage.cas_retry_limit,
            self.storage.zone_rotate_count,
        )
        .await
        .map_err(|e| match e {
            crate::model::disk_group::AllocError::NoSpace => Status::resource_exhausted("no space available"),
        })?;

        Ok(Response::new(AllocateResponse { segments }))
    }

    async fn free_blocks(&self, req: Request<FreeBlocksRequest>) -> Result<Response<FreeResponse>, Status> {
        let req = req.into_inner();

        if req.segments.is_empty() {
            return Ok(Response::new(FreeResponse { freed_count: 0 }));
        }

        // Check lifecycle phase — mutating RPCs require Up.
        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            return Err(Status::unavailable(format!(
                "diskdb not ready: phase={}",
                phase.as_str()
            )));
        }

        // Check not degraded.
        if self.container.is_degraded() {
            return Err(Status::unavailable("diskdb in degraded mode"));
        }

        // Get the node from the first segment's disk_group_id. All
        // segments should belong to the same disk-group (the caller
        // groups them); we use the first to look up the node.
        let first_disk_id = req.segments[0]
            .disk_id
            .ok_or_else(|| Status::invalid_argument("segment.disk_id required"))?;

        // Find the node that owns this disk.
        let node = {
            let dg_ids = self.container.disk_group_ids();
            let mut found = None;
            for dg_id in dg_ids {
                if let Some(n) = self.container.get_disk_group(dg_id) {
                    let owns = {
                        let disks = n.disks.read().unwrap();
                        disks.iter().any(|d| d.disk_id == first_disk_id)
                    };
                    if owns {
                        found = Some(n);
                        break;
                    }
                }
            }
            found
        }
        .ok_or_else(|| {
            Status::permission_denied(format!(
                "disk {} not owned by this instance",
                first_disk_id.to_display_string()
            ))
        })?;

        // Immediate free (v1).
        alloc::free_blocks(
            &node,
            &req.segments,
            &self.kv,
            self.storage.validate_owner_on_free,
        )
        .await
        .map_err(|e| match e {
            crate::model::alloc::FreeError::NotBusy { .. } => Status::not_found(format!("free failed: {e}")),
            crate::model::alloc::FreeError::OwnerMismatch { .. } => {
                Status::permission_denied(format!("free failed: {e}"))
            }
            crate::model::alloc::FreeError::Kv(_) => Status::internal(format!("free persist failed: {e}")),
        })?;

        #[allow(clippy::cast_possible_truncation)]
        let freed_count = req.segments.len() as u32;
        Ok(Response::new(FreeResponse { freed_count }))
    }

    #[allow(clippy::too_many_lines)]
    async fn query_capacity_stats(
        &self,
        req: Request<QueryCapacityStatsRequest>,
    ) -> Result<Response<QueryCapacityStatsResponse>, Status> {
        let req = req.into_inner();
        // Read-only — allowed in any lifecycle phase (like
        // GetDiskGroupInfo); does not check allows_mutating_rpcs.

        // Zone-level shape: disk_group_id + disk_id + zone_index all set.
        if let (Some(disk_id), Some(zone_index)) = (req.disk_id, req.zone_index) {
            let dg = self
                .container
                .get_disk_group(req.disk_group_id)
                .ok_or_else(|| Status::not_found(format!("disk-group {} not owned", req.disk_group_id)))?;
            // Verify the disk exists in this group.
            let disk_exists = {
                let disks = dg.disks.read().unwrap();
                disks.iter().any(|d| d.disk_id == disk_id)
            };
            if !disk_exists {
                return Err(Status::not_found(format!(
                    "disk {} not found in group {}",
                    disk_id.to_display_string(),
                    req.disk_group_id
                )));
            }
            let zu = dg.zone_usage(disk_id, zone_index).ok_or_else(|| {
                Status::not_found(format!(
                    "zone {zone_index} out of range on disk {}",
                    disk_id.to_display_string()
                ))
            })?;
            // Attach the full usage_bitmap for the zone-level shape.
            let bitmap_bytes = {
                let disk = dg.get_disk(disk_id);
                let bitmap = disk.and_then(|d| {
                    let zones = d.zones.read().unwrap();
                    let zi = zone_index as usize;
                    (zi < zones.len()).then(|| zones[zi].usage_bits.snapshot())
                });
                bitmap
            };
            let mut proto_zu = Self::zone_usage_to_proto(&zu);
            proto_zu.usage_bitmap = bitmap_bytes;
            // Return a single-disk-group response with one disk + one zone.
            let disk_info = DiskInfo {
                zone_usages: vec![proto_zu],
                ..Self::build_disk_info(&dg.get_disk(disk_id).expect("disk exists"), false)
            };
            let group_info = DiskGroupInfo {
                rack_id: dg.rack_id,
                node_id: dg.node_id,
                disk_group_id: dg.disk_group_id,
                status: *dg.status.read().unwrap() as i32,
                disk_ids: vec![disk_id],
                disks: vec![disk_info],
                capacity_bytes: 0,
                busy_bytes: 0,
                free_bytes: 0,
                allocatable_disk_count: 0,
            };
            return Ok(Response::new(QueryCapacityStatsResponse {
                disk_groups: vec![group_info],
            }));
        }

        // Disk-level shape: disk_group_id + disk_id set, zone_index absent.
        if let Some(disk_id) = req.disk_id {
            let dg = self
                .container
                .get_disk_group(req.disk_group_id)
                .ok_or_else(|| Status::not_found(format!("disk-group {} not owned", req.disk_group_id)))?;
            let disk = {
                let disks = dg.disks.read().unwrap();
                disks.iter().find(|d| d.disk_id == disk_id).cloned()
            }
            .ok_or_else(|| {
                Status::not_found(format!(
                    "disk {} not found in group {}",
                    disk_id.to_display_string(),
                    req.disk_group_id
                ))
            })?;
            let disk_info = Self::build_disk_info(&disk, true);
            let group_info = DiskGroupInfo {
                rack_id: dg.rack_id,
                node_id: dg.node_id,
                disk_group_id: dg.disk_group_id,
                status: *dg.status.read().unwrap() as i32,
                disk_ids: vec![disk_id],
                disks: vec![disk_info],
                capacity_bytes: 0,
                busy_bytes: 0,
                free_bytes: 0,
                allocatable_disk_count: 0,
            };
            return Ok(Response::new(QueryCapacityStatsResponse {
                disk_groups: vec![group_info],
            }));
        }

        // Disk-group level shape: disk_group_id only (or 0 = all owned).
        let dg_ids: Vec<u64> = if req.disk_group_id == 0 {
            self.container.disk_group_ids()
        } else {
            // Verify the specific group is owned.
            if self.container.get_disk_group(req.disk_group_id).is_none() {
                return Err(Status::not_found(format!(
                    "disk-group {} not owned",
                    req.disk_group_id
                )));
            }
            vec![req.disk_group_id]
        };

        let mut disk_groups = Vec::with_capacity(dg_ids.len());
        for dg_id in dg_ids {
            let Some(dg) = self.container.get_disk_group(dg_id) else {
                continue;
            };
            let usage = dg.aggregate_usage();
            let disk_ids: Vec<DiskId> = {
                let disks = dg.disks.read().unwrap();
                disks.iter().map(|d| d.disk_id).collect()
            };
            // Disk-group level: no per-zone entries (brief only).
            let disks: Vec<DiskInfo> = {
                let disks_guard = dg.disks.read().unwrap();
                disks_guard
                    .iter()
                    .map(|d| Self::build_disk_info(d, false))
                    .collect()
            };
            disk_groups.push(DiskGroupInfo {
                rack_id: dg.rack_id,
                node_id: dg.node_id,
                disk_group_id: dg.disk_group_id,
                status: *dg.status.read().unwrap() as i32,
                disk_ids,
                disks,
                capacity_bytes: usage.capacity_bytes,
                busy_bytes: usage.busy_bytes,
                free_bytes: usage.free_bytes,
                allocatable_disk_count: usage.allocatable_disk_count,
            });
        }
        Ok(Response::new(QueryCapacityStatsResponse { disk_groups }))
    }

    async fn get_disk_group_info(
        &self,
        req: Request<GetDiskGroupInfoRequest>,
    ) -> Result<Response<GetDiskGroupInfoResponse>, Status> {
        let req = req.into_inner();
        let node = self
            .container
            .get_disk_group(req.disk_group_id)
            .ok_or_else(|| Status::not_found(format!("disk-group {} not owned", req.disk_group_id)))?;
        let status = *node.status.read().unwrap();
        let usage = node.aggregate_usage();
        let disk_ids: Vec<_> = node.disks.read().unwrap().iter().map(|d| d.disk_id).collect();
        let group = DiskGroupInfo {
            rack_id: node.rack_id,
            node_id: node.node_id,
            disk_group_id: node.disk_group_id,
            status: status as i32,
            disk_ids,
            disks: Vec::new(),
            capacity_bytes: usage.capacity_bytes,
            busy_bytes: usage.busy_bytes,
            free_bytes: usage.free_bytes,
            allocatable_disk_count: usage.allocatable_disk_count,
        };
        Ok(Response::new(GetDiskGroupInfoResponse { group: Some(group) }))
    }

    async fn get_disk_info(
        &self,
        req: Request<GetDiskInfoRequest>,
    ) -> Result<Response<GetDiskInfoResponse>, Status> {
        let req = req.into_inner();
        let node = self
            .container
            .get_disk_group(req.disk_group_id)
            .ok_or_else(|| Status::not_found(format!("disk-group {} not owned", req.disk_group_id)))?;
        let disks = node.disks.read().unwrap();
        let req_disk_id = req
            .disk_id
            .ok_or_else(|| Status::invalid_argument("disk_id required"))?;
        let disk = disks.iter().find(|d| d.disk_id == req_disk_id).ok_or_else(|| {
            Status::not_found(format!("disk {} not found", req_disk_id.to_display_string()))
        })?;
        let info = Self::build_disk_info(disk, false);
        Ok(Response::new(GetDiskInfoResponse { disk: Some(info) }))
    }

    async fn rebuild_zone_bitmap(
        &self,
        req: Request<RebuildZoneBitmapRequest>,
    ) -> Result<Response<RebuildZoneBitmapResponse>, Status> {
        let req = req.into_inner();

        // Check lifecycle phase — mutating RPCs require Up.
        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            return Err(Status::unavailable(format!(
                "diskdb not ready: phase={}",
                phase.as_str()
            )));
        }

        let disk_id = req
            .disk_id
            .ok_or_else(|| Status::invalid_argument("disk_id required"))?;

        // Find the node that owns this disk + the disk's zone_count.
        let (node, disk_value, disk_value_disk_id) = {
            let dg_ids = self.container.disk_group_ids();
            let mut found = None;
            for dg_id in dg_ids {
                if let Some(n) = self.container.get_disk_group(dg_id) {
                    let dv_clone = {
                        let disks = n.disks.read().unwrap();
                        disks
                            .iter()
                            .find(|d| d.disk_id == disk_id)
                            .map(|d| (*d.disk_value.read().unwrap(), d.disk_id))
                    };
                    if let Some((dv, did)) = dv_clone {
                        found = Some((n, dv, did));
                        break;
                    }
                }
            }
            found
        }
        .ok_or_else(|| {
            Status::permission_denied(format!(
                "disk {} not owned by this instance",
                disk_id.to_display_string()
            ))
        })?;

        let bind = *node.bind.read().unwrap();
        let zone_count = disk_value.zone_count;
        let zone_size_units = disk_value.zone_size_units;

        // Determine which zones to rebuild: all or one.
        let zones_to_rebuild: Vec<u32> = if req.zone_index == ALL_ZONES {
            (0..zone_count).collect()
        } else {
            if req.zone_index >= zone_count {
                return Err(Status::invalid_argument(format!(
                    "zone_index {} out of range (zone_count={zone_count})",
                    req.zone_index,
                )));
            }
            vec![req.zone_index]
        };

        let mut rebuilt_zone_count = 0u32;
        let mut total_busy_units = 0u64;
        let mut total_free_units = 0u64;
        for zi in zones_to_rebuild {
            #[allow(clippy::cast_possible_truncation)]
            let unit_capacity = if zi == zone_count - 1 {
                let remaining = disk_value.capacity_units - (u64::from(zi) * zone_size_units);
                let rounded = (remaining / 64) * 64;
                rounded as u32
            } else {
                zone_size_units as u32
            };
            match self
                .recovery
                .rebuild_zone_bitmap_full_scan(bind, disk_value_disk_id, zi, unit_capacity)
                .await
            {
                Ok((_zone, stats)) => {
                    rebuilt_zone_count += 1;
                    total_busy_units += stats.used_units;
                    total_free_units += stats.free_units;
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "rebuild_zone_bitmap failed for zone {zi}: {e}"
                    )));
                }
            }
        }

        Ok(Response::new(RebuildZoneBitmapResponse {
            rebuilt_zone_count,
            total_busy_units,
            total_free_units,
        }))
    }

    async fn recalc_disk_usage(
        &self,
        req: Request<RecalcDiskUsageRequest>,
    ) -> Result<Response<RecalcDiskUsageResponse>, Status> {
        let req = req.into_inner();
        // Mutating RPC — requires allows_mutating_rpcs (admin operation;
        // does KV reads/journal scans).
        let phase = self.container.lifecycle_phase();
        if !phase.allows_mutating_rpcs() {
            return Err(Status::unavailable(format!(
                "diskdb not ready: phase={}",
                phase.as_str()
            )));
        }

        let results = if let Some(dg_id) = req.disk_group_id {
            // Recalc one disk-group.
            match self.recalc.recalc_disk_group(dg_id).await {
                Some(r) => vec![r],
                None => return Err(Status::not_found(format!("disk-group {dg_id} not owned"))),
            }
        } else {
            // Recalc all owned disk-groups.
            self.recalc.recalc_all().await
        };

        let proto_results: Vec<DiskGroupRecalcResult> = results
            .into_iter()
            .map(|dg_r| DiskGroupRecalcResult {
                disk_group_id: dg_r.disk_group_id,
                drift_detected: dg_r.drift_detected,
                zones: dg_r
                    .zone_results
                    .into_iter()
                    .map(|zr| ZoneRecalcResult {
                        disk_id: Some(zr.disk_id),
                        zone_index: zr.zone_index,
                        matches: zr.matches,
                        drift_detected: zr.drift_detected,
                        live_busy_blocks: zr.live_busy_blocks,
                        replayed_busy_blocks: zr.replayed_busy_blocks,
                        live_snapshot_slot: zr.live_snapshot_slot,
                        replayed_snapshot_slot: zr.replayed_snapshot_slot,
                        fallback_reason: zr.fallback_reason_str().map(String::from),
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(RecalcDiskUsageResponse {
            results: proto_results,
        }))
    }
}
