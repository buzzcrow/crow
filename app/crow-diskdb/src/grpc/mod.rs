// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb gRPC service wiring.
//!
//! `AllocateBlocks` / `FreeBlocks` / `QueryCapacityStats` return
//! `Unimplemented` — R72 fills them in. `GetDiskGroupInfo` /
//! `GetDiskInfo` read from in-memory `NodeContainer` state.

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

use crate::node::NodeContainer;

pub struct DiskdbService {
    container: Arc<NodeContainer>,
}

impl DiskdbService {
    pub fn new(container: Arc<NodeContainer>) -> Self {
        Self { container }
    }

    pub fn into_server(self) -> DiskdbServiceServer<Self> {
        DiskdbServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl DiskdbServiceTrait for DiskdbService {
    async fn allocate_blocks(
        &self,
        _req: Request<AllocateBlocksRequest>,
    ) -> Result<Response<AllocateResponse>, Status> {
        Err(Status::unimplemented("allocate_blocks not implemented until R72"))
    }

    async fn free_blocks(&self, _req: Request<FreeBlocksRequest>) -> Result<Response<FreeResponse>, Status> {
        Err(Status::unimplemented("free_blocks not implemented until R72"))
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
