// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `LargeObjectWriter` + `WriterConfig`.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::traits::{BlockWriter, ChunkAllocator};
use crate::writer::pipeline::{run_fetch_stage, run_main_write_task};
use crate::{Location, Result};
use crow_common::ec::EcScheme;
use crow_protocol::chunk_id::CHUNK_TYPE_REPO;

/// Writer configuration.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Max chunk size before rotation (bytes). Default 1 GB.
    pub max_chunk_size: u64,
    /// Strips allocated ahead of the write cursor. Default 2.
    pub prealloc_depth: usize,
    /// Parity tasks in flight. Default 2.
    pub parity_depth: usize,
    /// Chunks allocated ahead. Default 1.
    pub chunk_prefetch_depth: usize,
    /// Fetch read granularity / block size (bytes). Default 1 MB.
    pub read_buffer_size: usize,
    /// Max un-written data in fetch cache (bytes). Default 4 MB.
    pub max_cached_buffer: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        const MB: usize = 1024 * 1024;
        const GB: usize = 1024 * 1024 * 1024;
        Self {
            max_chunk_size: GB as u64,
            prealloc_depth: 2,
            parity_depth: 2,
            chunk_prefetch_depth: 1,
            read_buffer_size: MB,
            max_cached_buffer: 4 * MB,
        }
    }
}

/// Large-object writer — writes one object to one or more dedicated
/// chunks using EC strips.
///
/// Generic over `A: ChunkAllocator` (chunk lifecycle) and `W:
/// BlockWriter` (disk IO) for testability.
pub struct LargeObjectWriter<A: ChunkAllocator, W: BlockWriter> {
    chunkdb: Arc<A>,
    diskio: Arc<W>,
    ec_scheme: EcScheme,
    config: WriterConfig,
}

impl<A: ChunkAllocator + 'static, W: BlockWriter + 'static> LargeObjectWriter<A, W> {
    /// Construct a new writer.
    pub fn new(chunkdb: A, diskio: W, ec_scheme: EcScheme, config: WriterConfig) -> Self {
        Self {
            chunkdb: Arc::new(chunkdb),
            diskio: Arc::new(diskio),
            ec_scheme,
            config,
        }
    }

    /// Per-writer memory footprint for `WriterPool` budgeting.
    pub fn per_writer_memory(&self) -> usize {
        let block = self.config.read_buffer_size;
        self.config.max_cached_buffer
            + block
            + self.config.parity_depth * self.ec_scheme.total_blocks() * block
    }

    /// Stream-driven write. Drives fetch + main write + parity
    /// pipeline internally.
    ///
    /// `object_size` known → pre-calculate strip/chunk count for
    /// planning but still only pre-allocate `prealloc_depth` ahead;
    /// `None` → streaming, on-demand strips.
    pub async fn write_stream(
        &mut self,
        reader: impl tokio::io::AsyncRead + Unpin + Send,
        object_size: Option<u64>,
    ) -> Result<Vec<Location>> {
        // Empty object → no chunks.
        if object_size == Some(0) {
            return Ok(Vec::new());
        }

        // Write granularity = block size = read_buffer_size. The unit
        // size is the strip's block size; each data shard is one block.
        let write_granularity_kb: u32 = (self.config.read_buffer_size / 1024) as u32;
        let unit_bytes = u64::from(write_granularity_kb) * 1024;

        // Spawn prealloc task. The sender is dropped when the task
        // exits, closing the channel → receiver gets None.
        let (mut prealloc_rx, prealloc_handle) = crate::prefetch::spawn_prealloc_task(
            self.chunkdb.clone(),
            self.ec_scheme,
            self.config.max_chunk_size,
            self.config.prealloc_depth,
            write_granularity_kb,
            object_size,
            CHUNK_TYPE_REPO,
        );

        // Create block channel (fetch → main write).
        let block_channel_capacity = (self.config.max_cached_buffer / self.config.read_buffer_size).max(1);
        let (block_tx, block_rx) = mpsc::channel(block_channel_capacity);

        // Run fetch and main write concurrently. fetch reads from
        // `reader` and sends blocks; main_write receives blocks and
        // drives the pipeline. When fetch finishes (EOF), it drops
        // `block_tx`, and main_write sees channel closure → finishes.
        let fetch_fut = run_fetch_stage(reader, block_tx, self.config.read_buffer_size);
        let main_write_fut = run_main_write_task(
            self.chunkdb.clone(),
            self.diskio.clone(),
            self.ec_scheme,
            self.config.max_chunk_size,
            self.config.parity_depth,
            unit_bytes,
            &mut prealloc_rx,
            block_rx,
        );

        let (_fetch_result, locations) = tokio::join!(fetch_fut, main_write_fut);

        // Abort prealloc task if still running (e.g. on error).
        prealloc_handle.abort();

        locations
    }
}
