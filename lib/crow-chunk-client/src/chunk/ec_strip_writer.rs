// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `EcStripWriter` — owns one EC strip's data + parity write.
//!
//! `push` writes a data block to disk via `DiskWriter` and feeds the
//! buffer to `EcWorker` for streaming compute. `finish` spawns the
//! pre-computed parity shards + deduplicated fsyncs in parallel via
//! `parity_writer::spawn_parity_writes` and returns the handles
//! **without joining** — `ChunkWriter` collects them and joins at
//! `seal()` time. Each disk block (data + parity) can be written in
//! parallel.
//!
//! Holds `Arc<Chunk>` + `strip_index` — shares the protobuf with
//! `ChunkWriter` by ref count. Accessor methods (`unit_bytes`,
//! `segment`, `disk_id`, `zone_offset`) read directly from
//! `self.chunk.strips[self.strip_index]`.

use std::sync::Arc;

use bytes::Bytes;

use crate::chunk::parity_writer::spawn_parity_writes;
use crate::chunk::strip::StripResult;
use crate::disk_io::DiskWriter;
use crate::io::FeedStatus;
use crate::worker::EcWorker;
use crate::{IoError, Result};
use crow_common::ec::EcScheme;
use crow_diskio_client::DiskId;
use crow_protocol::chunkdb::rpc::Strip as StripOneof;
use crow_protocol::chunkdb::rpc::{Chunk, EcStrip};
use crow_protocol::diskdb::rpc::Segment;

/// EC strip writer — owns one strip's data + parity write.
pub struct EcStripWriter {
    pub(crate) chunk: Arc<Chunk>,
    pub(crate) strip_index: u32,
    pub(crate) disk_writer: Arc<dyn DiskWriter>,
    pub(crate) ec_worker: EcWorker,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) next_block: usize,
    pub(crate) data_blocks_written: u32,
    pub(crate) bytes_written: u64,
    pub(crate) partial: bool,
    pub(crate) finished: bool,
}

impl EcStripWriter {
    /// Construct a new EC strip writer sharing `chunk` at `strip_index`.
    pub fn new(
        chunk: Arc<Chunk>,
        strip_index: u32,
        disk_writer: Arc<dyn DiskWriter>,
        ec_scheme: EcScheme,
    ) -> Self {
        let ec_worker = EcWorker::new(ec_scheme);
        Self {
            chunk,
            strip_index,
            disk_writer,
            ec_worker,
            ec_scheme,
            next_block: 0,
            data_blocks_written: 0,
            bytes_written: 0,
            partial: false,
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

    /// The current `ChunkStrip` protobuf (the strip being written).
    fn strip(&self) -> Result<&crow_protocol::chunkdb::rpc::ChunkStrip> {
        self.chunk
            .strips
            .get(self.strip_index as usize)
            .ok_or_else(|| IoError::Internal(format!("strip {} missing from chunk", self.strip_index)))
    }

    /// The `EcStrip` oneof of the current strip.
    fn ec_strip(&self) -> Result<&EcStrip> {
        match self.strip()?.strip.as_ref() {
            Some(StripOneof::EcStrip(ec)) => Ok(ec),
            Some(StripOneof::MirrorStrip(_)) => {
                Err(IoError::Internal("expected EC strip, got mirror".into()))
            }
            None => Err(IoError::Internal("chunk strip missing oneof".into())),
        }
    }

    /// Unit size in bytes = unit_kb * 1024.
    fn unit_bytes(&self) -> u64 {
        u64::from(self.strip().map_or(0, |s| s.unit_kb)) * 1024
    }

    /// Get segment `i` (bounds-checked) of the current strip.
    fn segment(&self, i: usize) -> Result<&Segment> {
        self.ec_strip()?
            .segments
            .get(i)
            .ok_or_else(|| IoError::Internal(format!("segment {i} missing")))
    }

    /// Get the disk_id for segment `i`.
    #[allow(dead_code)] // used by the read path (R107) + accessor tests
    fn disk_id(&self, i: usize) -> Result<DiskId> {
        let seg = self.segment(i)?;
        let did = seg
            .disk_id
            .as_ref()
            .ok_or_else(|| IoError::Internal(format!("segment {i} missing disk_id")))?;
        Ok(DiskId::new(did.high, did.low))
    }

    /// Get the byte zone offset for segment `i` = unit_offset * unit_bytes.
    #[allow(dead_code)] // used by the read path (R107) + accessor tests
    fn zone_offset(&self, i: usize) -> Result<u64> {
        let seg = self.segment(i)?;
        Ok(seg.unit_offset * self.unit_bytes())
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

        let unit_bytes = self.unit_bytes();
        let block_len = u64::try_from(buffer.len()).unwrap_or(0);
        let is_partial = block_len < unit_bytes;

        // Write the data block to disk. Clone is required: disk_writer
        // takes ownership, but buffer is also borrowed by ec_worker below.
        // Bytes::clone is an Arc bump — no data copy.
        let seg = self.segment(self.next_block)?;
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

    /// End of strip: spawn parity writes + fsyncs in parallel (no
    /// join) and return the strip result with parity handles. The
    /// caller (`ChunkWriter`) collects the handles and joins them at
    /// `seal()` time — strip N+1's data writes overlap with strip N's
    /// parity writes + fsyncs.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)] // signature matches MirrorStripWriter for enum dispatch
    pub async fn finish(&mut self) -> Result<StripResult> {
        if self.finished {
            return Err(IoError::Finished);
        }
        self.finished = true;

        // Finalize EC compute — get parity shards.
        let parity = self.ec_worker.finish()?;

        // Spawn parallel parity write + fsync tasks (no join).
        let parity_handles = spawn_parity_writes(
            &self.chunk,
            self.strip_index,
            parity,
            &self.disk_writer,
            &self.ec_scheme,
        )?;

        // Reset the EC worker for reuse.
        self.ec_worker.reset();

        Ok(StripResult {
            chunk_id: self.chunk.id.unwrap_or_default(),
            strip_index_in_chunk: self.strip_index,
            data_blocks_written: self.data_blocks_written,
            bytes_written: self.bytes_written,
            partial: self.partial,
            parity_handles,
        })
    }

    /// Abort: drop in-flight writes, return already-durable state.
    /// No parity tasks are spawned (abort is called instead of
    /// `finish`), so no handles to abort.
    pub fn abort(&mut self) -> Result<StripResult> {
        self.finished = true;
        self.ec_worker.reset();
        Ok(StripResult {
            chunk_id: self.chunk.id.unwrap_or_default(),
            strip_index_in_chunk: self.strip_index,
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

    // ── test-util accessors ──────────────────────────────────────
    // Private accessors exposed via `#[cfg(feature = "test-util")]`
    // for integration tests. Not part of the public API.

    #[cfg(feature = "test-util")]
    pub fn unit_bytes_for_tests(&self) -> u64 {
        self.unit_bytes()
    }

    #[cfg(feature = "test-util")]
    pub fn segment_for_tests(&self, i: usize) -> Result<&Segment> {
        self.segment(i)
    }

    #[cfg(feature = "test-util")]
    pub fn disk_id_for_tests(&self, i: usize) -> Result<DiskId> {
        self.disk_id(i)
    }

    #[cfg(feature = "test-util")]
    pub fn zone_offset_for_tests(&self, i: usize) -> Result<u64> {
        self.zone_offset(i)
    }
}
