// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! chunkdb gRPC service stub.
//!
//! All RPCs return `Unimplemented` — real handlers land in R89.

use tonic::{Request, Response, Status};

use crow_protocol::chunkdb::rpc::chunkdb_service_server::{
    ChunkdbService as ChunkdbServiceTrait, ChunkdbServiceServer,
};
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse,
    ListChunksRequest, ListChunksResponse, QueryChunkRequest, QueryChunkResponse, SealChunkRequest,
    SealChunkResponse, UpdateChunkStripRequest, UpdateChunkStripResponse,
};

/// chunkdb gRPC service (stub — all RPCs return Unimplemented).
pub struct ChunkdbService;

impl ChunkdbService {
    pub fn new() -> Self {
        Self
    }

    pub fn into_server(self) -> ChunkdbServiceServer<Self> {
        ChunkdbServiceServer::new(self)
    }
}

impl Default for ChunkdbService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl ChunkdbServiceTrait for ChunkdbService {
    async fn allocate_chunk(
        &self,
        _req: Request<AllocateChunkRequest>,
    ) -> Result<Response<AllocateChunkResponse>, Status> {
        Err(Status::unimplemented("allocate_chunk not yet implemented (R89)"))
    }

    async fn append_chunk(
        &self,
        _req: Request<AppendChunkRequest>,
    ) -> Result<Response<AppendChunkResponse>, Status> {
        Err(Status::unimplemented("append_chunk not yet implemented (R89)"))
    }

    async fn query_chunk(
        &self,
        _req: Request<QueryChunkRequest>,
    ) -> Result<Response<QueryChunkResponse>, Status> {
        Err(Status::unimplemented("query_chunk not yet implemented (R89)"))
    }

    async fn seal_chunk(
        &self,
        _req: Request<SealChunkRequest>,
    ) -> Result<Response<SealChunkResponse>, Status> {
        Err(Status::unimplemented("seal_chunk not yet implemented (R89)"))
    }

    async fn delete_chunk(
        &self,
        _req: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        Err(Status::unimplemented("delete_chunk not yet implemented (R89)"))
    }

    async fn delete_chunk_range(
        &self,
        _req: Request<DeleteChunkRangeRequest>,
    ) -> Result<Response<DeleteChunkRangeResponse>, Status> {
        Err(Status::unimplemented(
            "delete_chunk_range not yet implemented (R89)",
        ))
    }

    async fn update_chunk_strip(
        &self,
        _req: Request<UpdateChunkStripRequest>,
    ) -> Result<Response<UpdateChunkStripResponse>, Status> {
        Err(Status::unimplemented(
            "update_chunk_strip not yet implemented (R89)",
        ))
    }

    async fn list_chunks(
        &self,
        _req: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        Err(Status::unimplemented("list_chunks not yet implemented (R89)"))
    }
}
