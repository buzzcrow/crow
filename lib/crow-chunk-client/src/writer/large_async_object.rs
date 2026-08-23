// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `LargeAsyncObjectWriter` — async stream variant, owns the drive
//! loop.
//!
//! Accepts an `AsyncRead` stream — a more complex flow with a fetch
//! stage + backpressure. Owns the drive loop: gets placements from
//! `ChunkPrefetch`, rotates chunks when the placement's chunk_id
//! changes, pushes data_num blocks per strip, finishes strips, seals
//! chunks at EOF. Implements `ChunkIoWriter` for push mode.

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
use crate::writer::fetch::run_fetch_stage;
use crate::{IoError, Location, Result};
use crow_common::ec::EcScheme;

/// Large-object writer — async stream. Owns the drive loop + fetch
/// stage.
pub struct LargeAsyncObjectWriter {
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

impl LargeAsyncObjectWriter {
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

    /// Per-writer memory footprint.
    pub fn per_writer_memory(&self) -> usize {
        self.config.per_writer_memory(&self.ec_scheme)
    }

    /// Seal the current chunk (if any) and record its Location.
    async fn seal_current(&mut self) -> Result<()> {
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
        Ok(())
    }

    /// Apply a placement: rotate chunk if needed, then open/continue
    /// the strip.
    async fn apply_placement(&mut self, placement: StripPlacement) -> Result<()> {
        let need_rotate = match &self.chunk_writer {
            None => true,
            Some(cw) => cw.current_chunk_id() != Some(placement.chunk_id),
        };
        if need_rotate {
            self.seal_current().await?;
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

    /// On-demand placement when prefetch is exhausted.
    async fn on_demand_placement(&self) -> Result<StripPlacement> {
        let chunk_id = self.chunk_writer.as_ref().and_then(ChunkWriter::current_chunk_id);
        let strips = self.chunk_writer.as_ref().map_or(0, ChunkWriter::strips_in_chunk);
        let pf = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crow_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        pf.on_demand(chunk_id, strips).await
    }

    /// Receive and push `data_num` blocks for the current strip.
    /// Returns the number of blocks received (0 = EOF before any
    /// block).
    async fn receive_and_push(
        &mut self,
        block_rx: &mut mpsc::Receiver<Bytes>,
        data_num: usize,
    ) -> Result<usize> {
        let mut received = 0usize;
        for _ in 0..data_num {
            match block_rx.recv().await {
                Some(buffer) => {
                    let cw = self.chunk_writer.as_mut().unwrap();
                    cw.push(buffer).await?;
                    received += 1;
                }
                None => break,
            }
        }
        Ok(received)
    }

    /// Async stream write. Runs fetch stage + drive loop concurrently.
    ///
    /// # Panics
    ///
    /// Does not panic directly, but calls `unwrap` on
    /// `chunk_writer.as_mut()` after `apply_placement` guarantees it
    /// is `Some`.
    pub async fn write_stream(
        &mut self,
        reader: impl tokio::io::AsyncRead + Unpin + Send,
        object_size: Option<u64>,
    ) -> Result<Vec<Location>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        if object_size == Some(0) {
            return Ok(Vec::new());
        }

        let prefetch = ChunkPrefetch::new(
            self.allocator.clone(),
            self.ec_scheme,
            self.config.clone(),
            crow_protocol::chunk_id::CHUNK_TYPE_REPO,
        );
        let (mut placement_rx, prefetch_handle) = prefetch.spawn(object_size);

        let channel_cap = (self.config.max_cached_buffer / self.config.read_buffer_size).max(1);
        let (block_tx, mut block_rx) = mpsc::channel::<Bytes>(channel_cap);
        let fetch_fut = run_fetch_stage(reader, block_tx, self.config.read_buffer_size);

        let data_num = self.ec_scheme.data_num;
        let mut got_eof = false;

        let drive_fut = async {
            loop {
                let placement = match placement_rx.recv().await {
                    Some(Ok(p)) => p,
                    Some(Err(e)) => return Err(e),
                    None => {
                        // Prefetch done — check if more data remains.
                        let Ok(Some(first_block)) =
                            tokio::time::timeout(std::time::Duration::from_millis(10), block_rx.recv()).await
                        else {
                            break;
                        };
                        // On-demand allocation for the extra strip.
                        let placement = self.on_demand_placement().await?;
                        self.apply_placement(placement).await?;
                        // Push the first block we already consumed.
                        let cw = self.chunk_writer.as_mut().unwrap();
                        cw.push(first_block).await?;
                        // Receive remaining blocks.
                        let extra = self.receive_and_push(&mut block_rx, data_num - 1).await?;
                        let total = 1 + extra;
                        if total > 0 {
                            let cw = self.chunk_writer.as_mut().unwrap();
                            cw.finish_strip().await?;
                        }
                        if extra < data_num - 1 {
                            got_eof = true;
                            break;
                        }
                        continue;
                    }
                };

                self.apply_placement(placement).await?;
                let received = self.receive_and_push(&mut block_rx, data_num).await?;
                if received > 0 {
                    let cw = self.chunk_writer.as_mut().unwrap();
                    cw.finish_strip().await?;
                }
                if received < data_num {
                    got_eof = true;
                    break;
                }
            }
            Ok::<(), IoError>(())
        };

        let (_fetch_result, drive_result) = tokio::join!(fetch_fut, drive_fut);
        drive_result?;

        self.seal_current().await?;
        prefetch_handle.abort();

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
impl ChunkIoWriter for LargeAsyncObjectWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        // Lazy-start prefetch on first push.
        if self.prefetch_rx.is_none() && self.chunk_writer.is_none() {
            let prefetch = ChunkPrefetch::new(
                self.allocator.clone(),
                self.ec_scheme,
                self.config.clone(),
                crow_protocol::chunk_id::CHUNK_TYPE_REPO,
            );
            let (rx, handle) = prefetch.spawn(None);
            self.prefetch_rx = Some(rx);
            self.prefetch_handle = Some(handle);
        }

        // Ensure we have an open chunk + strip.
        let need_open = self
            .chunk_writer
            .as_ref()
            .map_or(true, |cw| cw.is_strip_full() || cw.current_strip_is_none());

        if need_open {
            let placement = if let Some(rx) = self.prefetch_rx.as_mut() {
                match rx.recv().await {
                    Some(Ok(p)) => Some(p),
                    Some(Err(e)) => return Err(e),
                    None => {
                        self.prefetch_rx = None;
                        Some(self.on_demand_placement().await?)
                    }
                }
            } else {
                None
            };

            if let Some(placement) = placement {
                self.apply_placement(placement).await?;
            }
        }

        let cw = self
            .chunk_writer
            .as_mut()
            .ok_or_else(|| IoError::Internal("no chunk writer".into()))?;
        let status = cw.push(buffer).await?;

        if cw.is_strip_full() {
            cw.finish_strip().await?;
        }

        Ok(status)
    }

    async fn on_finish(&mut self) -> Result<Vec<Location>> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        // Finish current strip if has data.
        if let Some(cw) = self.chunk_writer.as_mut() {
            if !cw.is_strip_full() && cw.ready() {
                cw.finish_strip().await?;
            }
        }

        self.seal_current().await?;

        if let Some(handle) = self.prefetch_handle.take() {
            handle.abort();
        }

        Ok(std::mem::take(&mut self.locations))
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
