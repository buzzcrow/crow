// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `LargeObjectWriter` + `WriterConfig`.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::io::{ChunkIoWriter, FeedStatus};
use crate::traits::{BlockWriter, ChunkAllocator};
use crate::writer::pipeline::{run_fetch_stage, run_main_write_task};
use crate::{IoError, Location, Result};
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
/// `BlockWriter` (disk IO) for testability.
///
/// Two APIs: `write_stream` (stream-driven, recommended) and the
/// `ChunkIoWriter` trait (push-driven, for callers that feed bytes
/// incrementally). Both drive the same fetch→write→parity pipeline;
/// `write_stream` runs a fetch stage that pulls from `AsyncRead`, while
/// `on_data` pushes `Bytes` directly to the block channel.
pub struct LargeObjectWriter<A: ChunkAllocator, W: BlockWriter> {
    chunkdb: Arc<A>,
    diskio: Arc<W>,
    ec_scheme: EcScheme,
    config: WriterConfig,
    /// Push-mode pipeline state — `None` until the first `on_data`,
    /// consumed (set to `None`) by `on_finish`/`on_error`. Unused by
    /// `write_stream` (which runs the pipeline inline).
    pipeline: Option<PipelineHandle>,
    /// True after `write_stream`, `on_finish`, or `on_error` has run.
    /// Further `on_data`/`on_finish`/`on_error` return `IoError::Finished`.
    finished: bool,
}

/// Handles for the push-mode pipeline tasks + channels.
struct PipelineHandle {
    block_tx: mpsc::Sender<Bytes>,
    prealloc_handle: JoinHandle<()>,
    main_write: JoinHandle<Result<Vec<Location>>>,
    cancel_tx: watch::Sender<bool>,
}

/// Output of `start_pipeline` — the block channel sender + task
/// handles + cancel signal, bundled to avoid a complex tuple type.
struct PipelineStart {
    block_tx: mpsc::Sender<Bytes>,
    prealloc_handle: JoinHandle<()>,
    main_write: JoinHandle<Result<Vec<Location>>>,
    cancel_tx: watch::Sender<bool>,
}

impl<A: ChunkAllocator + 'static, W: BlockWriter + 'static> LargeObjectWriter<A, W> {
    /// Construct a new writer.
    pub fn new(chunkdb: A, diskio: W, ec_scheme: EcScheme, config: WriterConfig) -> Self {
        Self {
            chunkdb: Arc::new(chunkdb),
            diskio: Arc::new(diskio),
            ec_scheme,
            config,
            pipeline: None,
            finished: false,
        }
    }

    /// Per-writer memory footprint for `WriterPool` budgeting.
    pub fn per_writer_memory(&self) -> usize {
        let block = self.config.read_buffer_size;
        self.config.max_cached_buffer
            + block
            + self.config.parity_depth * self.ec_scheme.total_blocks() * block
    }

    /// Spawn the prealloc task + main write task and return the block
    /// channel sender + task handles + cancel signal. Shared by
    /// `write_stream` (which adds a fetch stage) and push mode (which
    /// feeds `block_tx` via `on_data`).
    fn start_pipeline(&self, object_size: Option<u64>) -> PipelineStart {
        let write_granularity_kb: u32 = (self.config.read_buffer_size / 1024) as u32;
        let unit_bytes = u64::from(write_granularity_kb) * 1024;

        let (prealloc_rx, prealloc_handle) = crate::prefetch::spawn_prealloc_task(
            self.chunkdb.clone(),
            self.ec_scheme,
            self.config.max_chunk_size,
            self.config.prealloc_depth,
            write_granularity_kb,
            object_size,
            CHUNK_TYPE_REPO,
        );

        let block_channel_capacity = (self.config.max_cached_buffer / self.config.read_buffer_size).max(1);
        let (block_tx, block_rx) = mpsc::channel(block_channel_capacity);
        let (cancel_tx, cancel_rx) = watch::channel(false);

        let main_write = tokio::spawn(run_main_write_task(
            self.chunkdb.clone(),
            self.diskio.clone(),
            self.ec_scheme,
            self.config.max_chunk_size,
            self.config.parity_depth,
            unit_bytes,
            prealloc_rx,
            block_rx,
            cancel_rx,
        ));

        PipelineStart {
            block_tx,
            prealloc_handle,
            main_write,
            cancel_tx,
        }
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
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        // Empty object → no chunks.
        if object_size == Some(0) {
            return Ok(Vec::new());
        }

        // Start the pipeline (prealloc + main write). The fetch stage
        // owns `block_tx`; when it hits EOF it drops the sender → main
        // write sees channel closure → drains + seals.
        let PipelineStart {
            block_tx,
            prealloc_handle,
            main_write,
            cancel_tx: _,
        } = self.start_pipeline(object_size);

        let fetch_fut = run_fetch_stage(reader, block_tx, self.config.read_buffer_size);
        let (_fetch_result, main_result) = tokio::join!(fetch_fut, main_write);

        // Abort prealloc task if still running (e.g. on error).
        prealloc_handle.abort();

        main_result
            .map_err(|e| IoError::Internal(format!("main write task panicked: {e}")))
            .and_then(std::convert::identity)
    }
}

#[async_trait::async_trait]
impl<A: ChunkAllocator + 'static, W: BlockWriter + 'static> ChunkIoWriter for LargeObjectWriter<A, W> {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        // Lazily start the pipeline on the first push.
        if self.pipeline.is_none() {
            let PipelineStart {
                block_tx,
                prealloc_handle,
                main_write,
                cancel_tx,
            } = self.start_pipeline(None);
            self.pipeline = Some(PipelineHandle {
                block_tx,
                prealloc_handle,
                main_write,
                cancel_tx,
            });
        }
        let pipe = self.pipeline.as_ref().expect("pipeline just initialized");
        // Always stores: awaits if the block channel is full
        // (backpressure = max_cached_buffer).
        pipe.block_tx
            .send(buffer)
            .await
            .map_err(|_| IoError::Internal("main write task exited".into()))?;
        // FeedStatus: Continue if the channel still has capacity,
        // Pause if it is now full.
        let status = if pipe.block_tx.capacity() > 0 {
            FeedStatus::Continue
        } else {
            FeedStatus::Pause
        };
        Ok(status)
    }

    async fn on_finish(&mut self) -> Result<Vec<Location>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;
        let Some(pipe) = self.pipeline.take() else {
            // No data was pushed — empty object.
            return Ok(Vec::new());
        };
        // Drop block_tx → main write sees EOF → drains + seals.
        drop(pipe.block_tx);
        let locations = pipe
            .main_write
            .await
            .map_err(|e| IoError::Internal(format!("main write task panicked: {e}")))
            .and_then(std::convert::identity)?;
        pipe.prealloc_handle.abort();
        Ok(locations)
    }

    async fn on_error(&mut self) -> Result<Vec<Location>> {
        self.finished = true;
        let Some(pipe) = self.pipeline.take() else {
            return Ok(Vec::new());
        };
        // Signal cancel + drop block_tx → main write aborts: drops
        // in-flight parity, deletes the partial chunk, returns the
        // already-sealed Locations.
        let _ = pipe.cancel_tx.send(true);
        drop(pipe.block_tx);
        let locations = pipe
            .main_write
            .await
            .map_err(|e| IoError::Internal(format!("main write task panicked: {e}")))
            .and_then(std::convert::identity)?;
        pipe.prealloc_handle.abort();
        Ok(locations)
    }

    fn require_data(&self) -> bool {
        if self.finished {
            return false;
        }
        match &self.pipeline {
            Some(pipe) => pipe.block_tx.capacity() > 0,
            None => true, // pipeline not started → on_data would not block
        }
    }
}

impl<A: ChunkAllocator, W: BlockWriter> Drop for LargeObjectWriter<A, W> {
    fn drop(&mut self) {
        if let Some(pipe) = self.pipeline.take() {
            // Safety net: signal cancel + drop block_tx so the main
            // write task drains, sees cancel, and deletes the partial
            // chunk. The task is detached (JoinHandle dropped) —
            // best-effort cleanup if the runtime is still alive. The
            // prealloc task is aborted (no cleanup needed).
            let _ = pipe.cancel_tx.send(true);
            drop(pipe.block_tx);
            pipe.prealloc_handle.abort();
        }
    }
}
