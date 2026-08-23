// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `LargeObjectWriter` (non-blocking stream) — owns the drive loop.
//!
//! Accepts a non-blocking stream (data already in buffer). Owns the
//! drive loop: gets placements from `ChunkPrefetch`, rotates chunks
//! when the placement's chunk_id changes, pushes data_num blocks per
//! strip, finishes strips, seals chunks at EOF. Implements
//! `ChunkIoWriter` for push mode.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::chunk::chunk_prefetch::ChunkPrefetch;
use crate::chunk::chunk_writer::ChunkWriter;
use crate::chunk::strip::StripPlacement;
use crate::config::ChunkClientConfig;
use crate::disk_io::DiskWriter;
use crate::io::{ChunkIoWriter, FeedStatus};
use crate::traits::ChunkAllocator;
use crate::{IoError, Location, Result};
use crow_common::ec::EcScheme;

/// Large-object writer — non-blocking stream. Owns the drive loop.
pub struct LargeObjectWriter {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk_writer: Option<ChunkWriter>,
    pub(crate) prefetch_rx: Option<mpsc::Receiver<Result<StripPlacement>>>,
    pub(crate) prefetch_handle: Option<JoinHandle<()>>,
    pub(crate) locations: Vec<Location>,
    pub(crate) logical_offset: u64,
    pub(crate) finished: bool,
}

impl LargeObjectWriter {
    /// Construct a new writer.
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
    ) -> Self {
        Self {
            allocator,
            disk_writer,
            ec_scheme,
            config,
            chunk_writer: None,
            prefetch_rx: None,
            prefetch_handle: None,
            locations: Vec::new(),
            logical_offset: 0,
            finished: false,
        }
    }

    /// Per-writer memory footprint for `WriterPool` budgeting.
    pub fn per_writer_memory(&self) -> usize {
        self.config.per_writer_memory(&self.ec_scheme)
    }

    /// Start the pipeline: spawn prefetch.
    pub(crate) fn start_pipeline(&mut self, object_size: Option<u64>) {
        let prefetch = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crow_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let (rx, handle) = prefetch.spawn(object_size);
        self.prefetch_rx = Some(rx);
        self.prefetch_handle = Some(handle);
    }

    /// Get the next placement (from prefetch or on-demand).
    pub(crate) async fn next_placement(&mut self) -> Result<Option<StripPlacement>> {
        if let Some(rx) = self.prefetch_rx.as_mut() {
            match rx.recv().await {
                Some(Ok(p)) => return Ok(Some(p)),
                Some(Err(e)) => return Err(e),
                None => {
                    self.prefetch_rx = None;
                }
            }
        }
        // On-demand allocation.
        let chunk_id = self.chunk_writer.as_ref().and_then(ChunkWriter::current_chunk_id);
        let strips = self.chunk_writer.as_ref().map_or(0, ChunkWriter::strips_in_chunk);
        let pf = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crow_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let placement = pf.on_demand(chunk_id, strips).await?;
        Ok(Some(placement))
    }

    /// Ensure an open strip: get placement, rotate chunk if needed.
    pub(crate) async fn ensure_open_strip(&mut self) -> Result<()> {
        let need_open = self
            .chunk_writer
            .as_ref()
            .map_or(true, |cw| cw.is_strip_full() || cw.current_strip_is_none());

        if !need_open {
            return Ok(());
        }

        let placement = self
            .next_placement()
            .await?
            .ok_or_else(|| IoError::Internal("no placement available".into()))?;

        let need_rotate = match &self.chunk_writer {
            None => true,
            Some(cw) => cw.current_chunk_id() != Some(placement.chunk_id),
        };
        if need_rotate {
            if let Some(mut cw) = self.chunk_writer.take() {
                let location = cw.seal().await?;
                let bytes = location.length;
                if bytes > 0 {
                    self.locations.push(Location {
                        logical_offset: self.logical_offset,
                        logical_length: bytes,
                        ..location
                    });
                    self.logical_offset += bytes;
                }
            }
            let mut cw = ChunkWriter::new(
                self.allocator.clone(),
                self.disk_writer.clone(),
                self.ec_scheme,
                self.config.clone(),
            );
            cw.open(placement)?;
            self.chunk_writer = Some(cw);
        } else {
            let cw = self.chunk_writer.as_mut().unwrap();
            cw.continue_strip(placement)?;
        }
        Ok(())
    }

    /// Finish: seal the current chunk, return all Locations.
    pub(crate) async fn finish_pipeline(&mut self) -> Result<Vec<Location>> {
        // Finish current strip if has data.
        if let Some(cw) = self.chunk_writer.as_mut() {
            if !cw.is_strip_full() && cw.ready() {
                let _ = cw.finish_strip().await?;
            }
        }

        if let Some(mut cw) = self.chunk_writer.take() {
            let location = cw.seal().await?;
            let bytes = location.length;
            if bytes > 0 {
                self.locations.push(Location {
                    logical_offset: self.logical_offset,
                    logical_length: bytes,
                    ..location
                });
                self.logical_offset += bytes;
            }
        }
        if let Some(handle) = self.prefetch_handle.take() {
            handle.abort();
        }
        Ok(std::mem::take(&mut self.locations))
    }

    /// Abort: cancel in-flight, return already-sealed Locations.
    pub(crate) async fn abort_pipeline(&mut self) -> Result<Vec<Location>> {
        if let Some(mut cw) = self.chunk_writer.take() {
            let _ = cw.abort().await;
        }
        if let Some(handle) = self.prefetch_handle.take() {
            handle.abort();
        }
        Ok(std::mem::take(&mut self.locations))
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for LargeObjectWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        if self.prefetch_rx.is_none() && self.chunk_writer.is_none() {
            self.start_pipeline(None);
        }

        // Ensure we have an open strip.
        self.ensure_open_strip().await?;

        // Push the block.
        let cw = self
            .chunk_writer
            .as_mut()
            .ok_or_else(|| IoError::Internal("no chunk writer".into()))?;
        let status = cw.push(buffer).await?;

        // If strip is now full, finish it.
        if cw.is_strip_full() {
            let _ = cw.finish_strip().await?;
        }

        Ok(status)
    }

    async fn on_finish(&mut self) -> Result<Vec<Location>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;
        self.finish_pipeline().await
    }

    async fn on_error(&mut self) -> Result<Vec<Location>> {
        self.finished = true;
        self.abort_pipeline().await
    }

    fn require_data(&self) -> bool {
        if self.finished {
            return false;
        }
        self.chunk_writer.as_ref().map_or(true, ChunkWriter::ready)
    }
}
