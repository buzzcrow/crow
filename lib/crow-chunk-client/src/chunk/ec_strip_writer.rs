// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `EcStripWriter` — owns one EC strip's data + parity write.
//!
//! `push` writes a data block to disk via `DiskWriter` and feeds the
//! buffer to `EcWorker` for streaming compute. `finish` writes the
//! pre-computed parity shards in parallel via `ParityBatch` + fsyncs.
//! Each disk block (data + parity) can be written in parallel.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use tokio::task::JoinHandle;

use crate::chunk::parity_batch::ParityBatch;
use crate::chunk::strip::{StripPlacement, StripResult};
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::worker::EcWorker;
use crate::{IoError, Result};
use crow_common::ec::EcScheme;

/// EC strip writer — owns one strip's data + parity write.
pub struct EcStripWriter {
    pub(crate) placement: StripPlacement,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_worker: EcWorker,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) next_block: usize,
    pub(crate) data_blocks_written: u32,
    pub(crate) bytes_written: u64,
    pub(crate) partial: bool,
    pub(crate) parity_batch: ParityBatch,
    pub(crate) finished: bool,
}

impl EcStripWriter {
    /// Construct a new EC strip writer.
    pub fn new(placement: StripPlacement, disk_writer: Arc<dyn DiskWriter>, ec_scheme: EcScheme) -> Self {
        let ec_worker = EcWorker::new(ec_scheme);
        Self {
            placement,
            disk_writer,
            ec_worker,
            ec_scheme,
            next_block: 0,
            data_blocks_written: 0,
            bytes_written: 0,
            partial: false,
            parity_batch: ParityBatch::new(),
            finished: false,
        }
    }

    /// The EC scheme for this strip.
    pub fn ec_scheme(&self) -> EcScheme {
        self.ec_scheme
    }

    /// Number of data blocks written so far.
    pub fn data_blocks_written(&self) -> u32 {
        self.data_blocks_written
    }

    /// True if the strip is full (all data_num blocks written).
    pub fn is_full(&self) -> bool {
        self.next_block >= self.ec_scheme.data_num
    }

    /// Push a data block to the strip. Writes the data block to disk
    /// + feeds the buffer to `EcWorker` for streaming compute.
    ///
    /// Returns `Continue` if the strip has room for more blocks,
    /// `Pause` if the strip is now full.
    pub async fn push(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        if self.finished {
            return Err(IoError::Finished);
        }
        if self.is_full() {
            return Err(IoError::Internal(
                "push called on full strip — call finish first".into(),
            ));
        }

        let unit_bytes = self.placement.unit_bytes();
        let block_len = u64::try_from(buffer.len()).unwrap_or(0);
        let is_partial = block_len < unit_bytes;

        // Write the data block to disk.
        let seg = self.placement.segment(self.next_block)?;
        self.disk_writer.write(seg, unit_bytes, buffer.clone()).await?;

        // Feed to EcWorker for streaming compute.
        self.ec_worker.push(&buffer)?;

        self.next_block += 1;
        self.data_blocks_written += 1;
        self.bytes_written += block_len;
        if is_partial {
            self.partial = true;
        }

        let status = if self.is_full() {
            FeedStatus::Pause
        } else {
            FeedStatus::Continue
        };
        Ok(status)
    }

    /// End of strip: write the pre-computed parity shards in parallel
    /// + fsync, return the strip result.
    pub async fn finish(&mut self) -> Result<StripResult> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        // Finalize EC compute — get parity shards.
        let parity = self.ec_worker.finish()?;
        let unit_bytes = self.placement.unit_bytes();
        let data_num = self.ec_scheme.data_num;

        // Spawn parallel parity write tasks.
        for (i, parity_block) in parity.iter().enumerate() {
            let seg_index = data_num + i;
            let seg = *self.placement.segment(seg_index)?;
            let data = Bytes::from(parity_block.clone());
            let disk_writer = self.disk_writer.clone();
            let seg_clone = seg;
            let ub = unit_bytes;
            let handle: JoinHandle<Result<()>> =
                tokio::spawn(async move { disk_writer.write(&seg_clone, ub, data).await });
            self.parity_batch.spawn(handle);
        }

        // Spawn parallel fsync tasks (deduplicated by disk_id).
        let mut fsynced: HashSet<(u64, u64)> = HashSet::new();
        for seg in &self.placement.segments {
            if let Some(did) = seg.disk_id.as_ref() {
                let key = (did.high, did.low);
                if fsynced.insert(key) {
                    let did_clone = *did;
                    let disk_writer = self.disk_writer.clone();
                    let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
                        let id = crow_diskio_client::DiskId::new(did_clone.high, did_clone.low);
                        disk_writer.fsync(id).await
                    });
                    self.parity_batch.spawn(handle);
                }
            }
        }

        // Join all writes.
        self.parity_batch.join_all().await?;

        // Reset the EC worker for reuse.
        self.ec_worker.reset();

        Ok(StripResult {
            chunk_id: self.placement.chunk_id,
            strip_index_in_chunk: self.placement.strip_index_in_chunk,
            data_blocks_written: self.data_blocks_written,
            bytes_written: self.bytes_written,
            partial: self.partial,
            parity_handles: Vec::new(), // already joined
        })
    }

    /// Abort: drop in-flight writes, return already-durable state.
    pub fn abort(&mut self) -> Result<StripResult> {
        self.finished = true;
        self.parity_batch.abort_all();
        self.ec_worker.reset();
        Ok(StripResult {
            chunk_id: self.placement.chunk_id,
            strip_index_in_chunk: self.placement.strip_index_in_chunk,
            data_blocks_written: self.data_blocks_written,
            bytes_written: self.bytes_written,
            partial: self.partial,
            parity_handles: Vec::new(),
        })
    }

    /// Non-async capacity hint. True if the strip has room for more
    /// data blocks.
    pub fn ready(&self) -> bool {
        !self.finished && !self.is_full()
    }
}
