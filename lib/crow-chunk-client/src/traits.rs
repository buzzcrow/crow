// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Trait seams for testability — `ChunkAllocator` and `BlockWriter`.
//!
//! The writer is generic over these traits so integration tests can
//! inject mock impls (record calls, inject delays/errors) without
//! running real servers. `ChunkdbClient` implements `ChunkAllocator`;
//! `DiskioBlockWriter` (wrapping `DiskioClient`) implements
//! `BlockWriter`.

use async_trait::async_trait;
use bytes::Bytes;
use crow_diskio_client::DiskId;
use crow_protocol::chunkdb::rpc::{
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

/// Block-level IO the writer needs. Mirrors the subset of
/// `DiskioClient` methods used by the data path.
#[async_trait]
pub trait BlockWriter: Send + Sync {
    /// Write `data` to `disk_id` at the given zone/offset.
    async fn write(&self, disk_id: DiskId, zone_index: u32, zone_offset: u64, data: Bytes) -> Result<()>;
    /// Flush all pending writes on `disk_id` to durable storage.
    async fn fsync(&self, disk_id: DiskId) -> Result<()>;
}

// Blanket impls so the pipeline can hold `Arc<A>` / `Arc<W>` and still
// call trait methods through the Arc.
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

#[async_trait]
impl<T: BlockWriter + ?Sized> BlockWriter for Arc<T> {
    async fn write(&self, disk_id: DiskId, zone_index: u32, zone_offset: u64, data: Bytes) -> Result<()> {
        (**self).write(disk_id, zone_index, zone_offset, data).await
    }
    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        (**self).fsync(disk_id).await
    }
}

// ── Concrete impl for ChunkdbClient ──────────────────────────────

#[async_trait]
impl ChunkAllocator for crow_chunkdb_client::ChunkdbClient {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        Ok(crow_chunkdb_client::ChunkdbClient::allocate_chunk(self, req).await?)
    }
    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        Ok(crow_chunkdb_client::ChunkdbClient::append_chunk(self, req).await?)
    }
    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        Ok(crow_chunkdb_client::ChunkdbClient::seal_chunk(self, req).await?)
    }
    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        Ok(crow_chunkdb_client::ChunkdbClient::delete_chunk(self, req).await?)
    }
    async fn update_chunk_strip(&self, req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        Ok(crow_chunkdb_client::ChunkdbClient::update_chunk_strip(self, req).await?)
    }
    async fn query_chunk(&self, req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(crow_chunkdb_client::ChunkdbClient::query_chunk(self, req).await?)
    }
}
