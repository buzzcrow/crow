// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkAllocator` — chunk-metadata RPC seam for testability.
//!
//! The writer is generic over this trait so integration tests can
//! inject mock impls (record calls, inject delays/errors) without
//! running real servers. `ChunkdbClient` implements `ChunkAllocator`.
//! Block-level IO is now the `DiskWriter` trait in `disk_io/`.

use async_trait::async_trait;
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, DeleteChunkRequest,
    DeleteChunkResponse, QueryChunkRequest, QueryChunkResponse, SealChunkRequest, SealChunkResponse,
    UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use std::sync::Arc;

use crate::Result;

/// Chunk lifecycle operations the writer needs. Mirrors the subset of
/// `ChunkdbClient` methods used by the data path.
#[async_trait]
pub trait ChunkAllocator: Send + Sync {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse>;
    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse>;
    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse>;
    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse>;
    async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse>;
    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse>;
}

// Blanket impl so the pipeline can hold `Arc<dyn ChunkAllocator>` and
// still call trait methods through the Arc.
#[async_trait]
impl<T: ChunkAllocator + ?Sized> ChunkAllocator for Arc<T> {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        (**self).allocate_chunk(req).await
    }
    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        (**self).append_chunk(req).await
    }
    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        (**self).seal_chunk(req).await
    }
    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        (**self).delete_chunk(req).await
    }
    async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        (**self).update_chunk_strip(req).await
    }
    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        (**self).query_chunk(req).await
    }
}

// ── Concrete impl for ChunkdbClient ──────────────────────────────

#[async_trait]
impl ChunkAllocator for crowdb_chunkdb_client::ChunkdbClient {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        Ok(crowdb_chunkdb_client::ChunkdbClient::allocate_chunk(self, req).await?)
    }
    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        Ok(crowdb_chunkdb_client::ChunkdbClient::append_chunk(self, req).await?)
    }
    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        Ok(crowdb_chunkdb_client::ChunkdbClient::seal_chunk(self, req).await?)
    }
    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        Ok(crowdb_chunkdb_client::ChunkdbClient::delete_chunk(self, req).await?)
    }
    async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        Ok(crowdb_chunkdb_client::ChunkdbClient::update_chunk_strip(self, req).await?)
    }
    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(crowdb_chunkdb_client::ChunkdbClient::query_chunk(self, req).await?)
    }
}
