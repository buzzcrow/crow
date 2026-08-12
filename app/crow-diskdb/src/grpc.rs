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
    QueryCapacityStatsResponse,
};
use crow_protocol::diskdb_type_util::DiskIdExt;
use tonic::{Request, Response, Status};

use crate::config::StorageDefaults;
use crate::node::NodeContainer;
use crate::persistence::{self, DataGroupClient};

/// Maximum number of blocks per `AllocateBlocks` request.
const MAX_ALLOCATE_COUNT: u32 = 1024;

pub struct DiskdbService {
    container: Arc<NodeContainer>,
    kv: Arc<DataGroupClient>,
    storage: StorageDefaults,
}

impl DiskdbService {
    pub fn new(container: Arc<NodeContainer>, kv: Arc<DataGroupClient>, storage: StorageDefaults) -> Self {
        Self {
            container,
            kv,
            storage,
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
        let node = self.container.get_node(req.disk_group_id).ok_or_else(|| {
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
            &node,
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
            crate::node::AllocError::NoSpace => Status::resource_exhausted("no space available"),
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
            let node_ids = self.container.node_ids();
            let mut found = None;
            for dg_id in node_ids {
                if let Some(n) = self.container.get_node(dg_id) {
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
        persistence::free_blocks(&node, &req.segments, &self.kv)
            .await
            .map_err(|e| Status::internal(format!("free persist failed: {e}")))?;

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
            .get_node(req.disk_group_id)
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
            .get_node(req.disk_group_id)
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
}
