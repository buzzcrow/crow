// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb gRPC service wiring.
//!
//! `AllocateBlocks` / `FreeBlocks` call the two-phase persistence
//! functions. `QueryCapacityStats` returns `Unimplemented` (R74).
//! `GetDiskGroupInfo` / `GetDiskInfo` read from in-memory state.

use std::sync::Arc;

use crow_protocol::diskdb::rpc::diskdb_service_server::{
    DiskdbService as DiskdbServiceTrait, DiskdbServiceServer,
};
use crow_protocol::diskdb::rpc::{
    AllocateBlocksRequest, AllocateResponse, FreeBlocksRequest, FreeResponse, GetDiskGroupInfoRequest,
    GetDiskGroupInfoResponse, GetDiskInfoRequest, GetDiskInfoResponse, QueryCapacityStatsRequest,
    QueryCapacityStatsResponse, RebuildZoneBitmapRequest, RebuildZoneBitmapResponse,
};
use crow_protocol::diskdb_type_util::DiskIdExt;
use tonic::{Request, Response, Status};

use crate::config::StorageDefaults;
use crate::domain::disk_group_container::DdbDiskGroupContainer;
use crate::persistence::{self, DataGroupClient};
use crate::recovery::RecoveryEngine;

/// Maximum number of blocks per `AllocateBlocks` request.
const MAX_ALLOCATE_COUNT: u32 = 1024;

/// `u32::MAX` sentinel for "all zones on the disk".
const ALL_ZONES: u32 = u32::MAX;

pub struct DiskdbService {
    container: Arc<DdbDiskGroupContainer>,
    kv: Arc<DataGroupClient>,
    storage: StorageDefaults,
    recovery: Arc<RecoveryEngine>,
}

impl DiskdbService {
    pub fn new(
        container: Arc<DdbDiskGroupContainer>,
        kv: Arc<DataGroupClient>,
        storage: StorageDefaults,
        recovery: Arc<RecoveryEngine>,
    ) -> Self {
        Self {
            container,
            kv,
            storage,
            recovery,
        }
    }

    pub fn into_server(self) -> DiskdbServiceServer<Self> {
        DiskdbServiceServer::new(self)
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
        let segments = persistence::allocate_blocks(
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
            crate::domain::disk_group::AllocError::NoSpace => {
                Status::resource_exhausted("no space available")
            }
        })?;

        Ok(Response::new(AllocateResponse { segments }))
    }

    async fn free_blocks(&self, req: Request<FreeBlocksRequest>) -> Result<Response<FreeResponse>, Status> {
        let req = req.into_inner();

        if req.segments.is_empty() {
            return Ok(Response::new(FreeResponse { freed_count: 0 }));
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
        persistence::free_blocks(
            &node,
            &req.segments,
            &self.kv,
            self.storage.validate_owner_on_free,
        )
        .await
        .map_err(|e| match e {
            crate::persistence::FreeError::NotBusy { .. } => Status::not_found(format!("free failed: {e}")),
            crate::persistence::FreeError::OwnerMismatch { .. } => {
                Status::permission_denied(format!("free failed: {e}"))
            }
            crate::persistence::FreeError::Kv(_) => Status::internal(format!("free persist failed: {e}")),
        })?;

        #[allow(clippy::cast_possible_truncation)]
        let freed_count = req.segments.len() as u32;
        Ok(Response::new(FreeResponse { freed_count }))
    }

    async fn query_capacity_stats(
        &self,
        _req: Request<QueryCapacityStatsRequest>,
    ) -> Result<Response<QueryCapacityStatsResponse>, Status> {
        Err(Status::unimplemented(
            "query_capacity_stats not implemented until R74",
        ))
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
        let disk_ids: Vec<_> = node.disks.read().unwrap().iter().map(|d| d.disk_id).collect();
        let group = crow_protocol::diskdb::rpc::DiskGroupInfo {
            rack_id: node.rack_id,
            node_id: node.node_id,
            disk_group_id: node.disk_group_id,
            status: status as i32,
            disk_ids,
            disks: Vec::new(),
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
        let dv = disk.disk_value.read().unwrap();
        let info = crow_protocol::diskdb::rpc::DiskInfo {
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
        };
        Ok(Response::new(GetDiskInfoResponse { disk: Some(info) }))
    }

    async fn rebuild_zone_bitmap(
        &self,
        req: Request<RebuildZoneBitmapRequest>,
    ) -> Result<Response<RebuildZoneBitmapResponse>, Status> {
        let req = req.into_inner();
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
}
