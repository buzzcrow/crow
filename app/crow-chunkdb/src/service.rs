// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb gRPC service — delegates to lifecycle handlers.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crow_protocol::chunkdb::rpc::chunkdb_service_server::{
    ChunkdbService as ChunkdbServiceTrait, ChunkdbServiceServer,
};
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, ChunkType,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    ListChunksRequest, ListChunksResponse, NotMyRangeHint, QueryChunkRequest, QueryChunkResponse,
    SealChunkRequest, SealChunkResponse, StripType as ProtoStripType, UpdateChunkStripRequest,
    UpdateChunkStripResponse,
};

use crate::lifecycle::{LifecycleError, LifecycleHandler};

/// chunkdb gRPC service.
pub struct ChunkdbService {
    handler: Arc<LifecycleHandler>,
}

impl ChunkdbService {
    #[must_use]
    pub fn new(handler: Arc<LifecycleHandler>) -> Self {
        Self { handler }
    }

    pub fn into_server(self) -> ChunkdbServiceServer<Self> {
        ChunkdbServiceServer::new(self)
    }

    /// Build a `NotMyRange` gRPC status. The server does not track
    /// other instances' bindings, so the hint carries only the bucket
    /// (in `range_start`) as a diagnostic signal — the client refreshes
    /// its binding cache from group-0 and re-routes. See
    /// `RangeBindingClient::refresh_and_route`.
    fn not_my_range_status(bucket: u16) -> Status {
        let hint = NotMyRangeHint {
            range_start: u32::from(bucket),
            range_end: u32::from(bucket),
            instance_id: 0,
            grpc_endpoint: String::new(),
            sub_range_index: 0,
        };
        let details = prost::Message::encode_to_vec(&hint);
        Status::with_details(
            tonic::Code::FailedPrecondition,
            format!("chunk bucket {bucket} not in owned ranges"),
            details.into(),
        )
    }
}

/// Map a `LifecycleError` to a gRPC `Status`.
fn map_error(e: &LifecycleError) -> Status {
    match e {
        LifecycleError::InvalidStateTransition(_) => Status::failed_precondition(e.to_string()),
        LifecycleError::ChunkNotFound => Status::not_found(e.to_string()),
        LifecycleError::ChunkAlreadyExists => Status::already_exists(e.to_string()),
        LifecycleError::StateConflict => Status::aborted(e.to_string()),
        LifecycleError::Allocation(_) => Status::internal(e.to_string()),
        LifecycleError::Storage(_) => Status::internal(e.to_string()),
        LifecycleError::InvalidRequest(_) => Status::invalid_argument(e.to_string()),
        LifecycleError::NotMyRange { bucket: _ } => Status::failed_precondition(e.to_string()),
        LifecycleError::LockBusy => Status::unavailable(e.to_string()),
        LifecycleError::LockTimeout => Status::unavailable(e.to_string()),
        LifecycleError::StripIndexOutOfRange { .. } => Status::invalid_argument(e.to_string()),
    }
}

#[tonic::async_trait]
impl ChunkdbServiceTrait for ChunkdbService {
    async fn allocate_chunk(
        &self,
        req: Request<AllocateChunkRequest>,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        let req = req.into_inner();
        let strip_type = ProtoStripType::try_from(req.strip_type)
            .map_err(|_| Status::invalid_argument("invalid strip_type"))?;
        let chunk_type = ChunkType::try_from(req.chunk_type)
            .map_err(|_| Status::invalid_argument("invalid chunk_type"))?;
        let chunk = self
            .handler
            .allocate_chunk(
                req.chunk_id,
                req.write_granularity,
                req.strip_count,
                strip_type,
                req.data_num,
                req.code_num,
                req.copy_count,
                chunk_type,
            )
            .await
            .map_err(|e| match &e {
                LifecycleError::NotMyRange { bucket } => Self::not_my_range_status(*bucket),
                _ => map_error(&e),
            })?;
        Ok(Response::new(AllocateChunkResponse { chunk: Some(chunk) }))
    }

    async fn append_chunk(
        &self,
        req: Request<AppendChunkRequest>,
    ) -> Result<Response<AppendChunkResponse>, Status> {
        let req = req.into_inner();
        let chunk_id = req
            .chunk_id
            .ok_or_else(|| Status::invalid_argument("missing chunk_id"))?;
        let strip_type = ProtoStripType::try_from(req.strip_type)
            .map_err(|_| Status::invalid_argument("invalid strip_type"))?;
        let chunk = self
            .handler
            .append_chunk(
                &chunk_id,
                req.strip_count,
                strip_type,
                req.data_num,
                req.code_num,
                req.copy_count,
                req.strip_size,
            )
            .await
            .map_err(|e| match &e {
                LifecycleError::NotMyRange { bucket } => Self::not_my_range_status(*bucket),
                _ => map_error(&e),
            })?;
        Ok(Response::new(AppendChunkResponse { chunk: Some(chunk) }))
    }

    async fn query_chunk(
        &self,
        req: Request<QueryChunkRequest>,
    ) -> Result<Response<QueryChunkResponse>, Status> {
        let req = req.into_inner();
        let chunk_id = req
            .chunk_id
            .ok_or_else(|| Status::invalid_argument("missing chunk_id"))?;
        let chunk = self
            .handler
            .query_chunk(&chunk_id)
            .await
            .map_err(|e| map_error(&e))?;
        Ok(Response::new(QueryChunkResponse { chunk: Some(chunk) }))
    }

    async fn seal_chunk(
        &self,
        req: Request<SealChunkRequest>,
    ) -> Result<Response<SealChunkResponse>, Status> {
        let req = req.into_inner();
        let chunk_id = req
            .chunk_id
            .ok_or_else(|| Status::invalid_argument("missing chunk_id"))?;
        let chunk = self
            .handler
            .seal_chunk(&chunk_id, req.seal_length)
            .await
            .map_err(|e| match &e {
                LifecycleError::NotMyRange { bucket } => Self::not_my_range_status(*bucket),
                _ => map_error(&e),
            })?;
        Ok(Response::new(SealChunkResponse { chunk: Some(chunk) }))
    }

    async fn delete_chunk(
        &self,
        req: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let req = req.into_inner();
        let chunk_id = req
            .chunk_id
            .ok_or_else(|| Status::invalid_argument("missing chunk_id"))?;
        let chunk = self.handler.delete_chunk(&chunk_id).await.map_err(|e| match &e {
            LifecycleError::NotMyRange { bucket } => Self::not_my_range_status(*bucket),
            _ => map_error(&e),
        })?;
        Ok(Response::new(DeleteChunkResponse { chunk: Some(chunk) }))
    }

    async fn delete_chunk_range(
        &self,
        req: Request<DeleteChunkRangeRequest>,
    ) -> Result<Response<DeleteChunkRangeResponse>, Status> {
        let req = req.into_inner();
        let chunk_id = req
            .chunk_id
            .ok_or_else(|| Status::invalid_argument("missing chunk_id"))?;
        self.handler
            .delete_chunk_range(&chunk_id, req.chunk_offset, req.chunk_size)
            .await
            .map_err(|e| match &e {
                LifecycleError::NotMyRange { bucket } => Self::not_my_range_status(*bucket),
                _ => map_error(&e),
            })?;
        Ok(Response::new(DeleteChunkRangeResponse {}))
    }

    async fn update_chunk_strip(
        &self,
        req: Request<UpdateChunkStripRequest>,
    ) -> Result<Response<UpdateChunkStripResponse>, Status> {
        let req = req.into_inner();
        let chunk_id = req
            .chunk_id
            .ok_or_else(|| Status::invalid_argument("missing chunk_id"))?;
        let strip = req
            .strip
            .ok_or_else(|| Status::invalid_argument("missing strip"))?;
        let chunk = self
            .handler
            .update_chunk_strip(&chunk_id, req.strip_index, strip)
            .await
            .map_err(|e| match &e {
                LifecycleError::NotMyRange { bucket } => Self::not_my_range_status(*bucket),
                _ => map_error(&e),
            })?;
        Ok(Response::new(UpdateChunkStripResponse { chunk: Some(chunk) }))
    }

    async fn list_chunks(
        &self,
        req: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let req = req.into_inner();
        let chunks = self
            .handler
            .list_chunks(req.start_token.as_ref(), req.max_keys)
            .await
            .map_err(|e| map_error(&e))?;
        let next_token = chunks.last().and_then(|c| c.id);
        Ok(Response::new(ListChunksResponse { chunks, next_token }))
    }
}
