// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `LargeObjectWriter::write_stream` using
//! mock `ChunkAllocator` + `BlockWriter` implementations.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::similar_names
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use crow_chunk_client::{
    BlockWriter, ChunkAllocator, ChunkIoWriter, FeedStatus, IoError, LargeObjectWriter, Result, WriterConfig,
};
use crow_common::ec::EcScheme;
use crow_diskio_client::DiskId;
use crow_protocol::chunkdb::rpc::chunk_strip::Strip as StripOneof;
use crow_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkStrip,
    ChunkType, DeleteChunkRequest, DeleteChunkResponse, EcStrip, QueryChunkRequest, QueryChunkResponse,
    SealChunkRequest, SealChunkResponse, StripType, UpdateChunkStripRequest, UpdateChunkStripResponse,
};
use crow_protocol::common::DiskId as ProtoDiskId;
use crow_protocol::diskdb::rpc::Segment;

// ── Mock ChunkAllocator ──────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct MockChunkAllocator {
    state: Arc<Mutex<MockChunkState>>,
}

#[derive(Debug, Default)]
struct MockChunkState {
    /// chunks by id → (strip_count, sealed_length_units, deleted)
    chunks: HashMap<(u64, u64), (u32, u32, bool)>,
    /// segment counter — each allocate/append gives fresh segments
    next_segment_offset: u64,
    /// calls recorded
    allocate_calls: usize,
    append_calls: usize,
    seal_calls: usize,
    delete_calls: usize,
}

impl MockChunkAllocator {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> std::sync::MutexGuard<'_, MockChunkState> {
        self.state.lock().unwrap()
    }
}

#[async_trait]
impl ChunkAllocator for MockChunkAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.allocate_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = data_num + code_num;

        // Build segments for the first strip.
        let mut segments = Vec::with_capacity(total);
        for i in 0..total {
            segments.push(Segment {
                disk_id: Some(ProtoDiskId {
                    high: 1000 + i as u64,
                    low: i as u64,
                }),
                zone_index: 0,
                unit_offset: st.next_segment_offset,
                unit_count: 1,
                owner_chunk: Some(chunk_id),
            });
            st.next_segment_offset += 1;
        }

        let strip = ChunkStrip {
            chunk_offset: 0,
            strip_sequence: 0,
            unit_kb: 4, // 4 KB units = 4096 bytes = read_buffer_size
            capacity: data_num as u32,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: StripType::Ec as i32,
            strip: Some(StripOneof::EcStrip(EcStrip {
                data_num: req.data_num,
                code_num: req.code_num,
                ec_state: 0,
                segments,
            })),
            usage_bitmap: Vec::new(),
        };

        let chunk = Chunk {
            id: Some(chunk_id),
            state: 1, // ACTIVE
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: data_num as u32,
            sealed_length: 0,
            strips: vec![strip],
            chunk_type: ChunkType::Repo as i32,
        };

        st.chunks.insert((chunk_id.high, chunk_id.low), (1, 0, false));

        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.append_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = data_num + code_num;

        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("append to unknown chunk");
        let strip_seq = entry.0;
        entry.0 += 1;

        let mut segments = Vec::with_capacity(total);
        for i in 0..total {
            segments.push(Segment {
                disk_id: Some(ProtoDiskId {
                    high: 1000 + i as u64,
                    low: i as u64,
                }),
                zone_index: 0,
                unit_offset: st.next_segment_offset,
                unit_count: 1,
                owner_chunk: Some(chunk_id),
            });
            st.next_segment_offset += 1;
        }

        let strip = ChunkStrip {
            chunk_offset: strip_seq,
            strip_sequence: strip_seq,
            unit_kb: 4, // 4 KB units = 4096 bytes = read_buffer_size
            capacity: data_num as u32,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: StripType::Ec as i32,
            strip: Some(StripOneof::EcStrip(EcStrip {
                data_num: req.data_num,
                code_num: req.code_num,
                ec_state: 0,
                segments,
            })),
            usage_bitmap: Vec::new(),
        };

        // Re-query the chunk to return full state with all strips.
        // For the mock, we only return the latest strip in the
        // response — the writer only reads the last strip.
        let chunk = Chunk {
            id: Some(chunk_id),
            state: 1,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity: data_num as u32 * (strip_seq + 1),
            sealed_length: 0,
            strips: vec![strip],
            chunk_type: ChunkType::Repo as i32,
        };

        Ok(AppendChunkResponse { chunk: Some(chunk) })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.seal_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("seal unknown chunk");
        entry.1 = req.seal_length;
        Ok(SealChunkResponse { chunk: None })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.delete_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("delete unknown chunk");
        entry.2 = true;
        Ok(DeleteChunkResponse { chunk: None })
    }

    async fn update_chunk_strip(&self, _req: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        Ok(UpdateChunkStripResponse { chunk: None })
    }

    async fn query_chunk(&self, _req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(QueryChunkResponse { chunk: None })
    }
}

// ── Mock BlockWriter ─────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct MockBlockWriter {
    writes: Arc<Mutex<Vec<MockWrite>>>,
    fsyncs: Arc<Mutex<Vec<(u64, u64)>>>,
    /// When set, the first `write` whose `zone_offset` matches fails
    /// once (then the slot is cleared). Used to inject a strip write
    /// failure for whole-strip retry tests.
    fail_zone_offset: Arc<Mutex<Option<u64>>>,
}

impl MockBlockWriter {
    fn new() -> Self {
        Self::default()
    }

    fn with_fail_once(zone_offset: u64) -> Self {
        Self {
            fail_zone_offset: Arc::new(Mutex::new(Some(zone_offset))),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MockWrite {
    disk_high: u64,
    disk_low: u64,
    zone_index: u32,
    zone_offset: u64,
    data: Vec<u8>,
}

#[async_trait]
impl BlockWriter for MockBlockWriter {
    async fn write(&self, disk_id: DiskId, zone_index: u32, zone_offset: u64, data: Bytes) -> Result<()> {
        {
            let mut fail = self.fail_zone_offset.lock().unwrap();
            if let Some(zo) = *fail {
                if zo == zone_offset {
                    *fail = None;
                    return Err(IoError::WriteFailed(format!(
                        "injected failure at zone_offset {zone_offset}"
                    )));
                }
            }
        }
        self.writes.lock().unwrap().push(MockWrite {
            disk_high: disk_id.high,
            disk_low: disk_id.low,
            zone_index,
            zone_offset,
            data: data.to_vec(),
        });
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        self.fsyncs.lock().unwrap().push((disk_id.high, disk_id.low));
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────

/// Config with small chunks for testing rotation.
fn test_config(max_chunk_size: u64) -> WriterConfig {
    WriterConfig {
        max_chunk_size,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 4096,
        max_cached_buffer: 8 * 4096,
    }
}

fn ec_4_1() -> EcScheme {
    EcScheme::new(4, 1)
}

// ── Tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn write_stream_empty_object() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio, ec, test_config(1024 * 1024));
    let data: Vec<u8> = Vec::new();
    let locs = writer.write_stream(data.as_slice(), Some(0)).await.unwrap();
    assert!(locs.is_empty());
}

#[tokio::test]
async fn write_stream_single_block_4mb() {
    // 4 MB = exactly 1 strip (4 data blocks × 1 MB). But our
    // read_buffer_size is 4096, so 4 MB = 1024 blocks of 4 KB. With
    // data_num=4, that's 256 strips. Use a smaller size: 16 KB = 4
    // blocks of 4 KB = 1 strip.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0xABu8; 4 * 4096]; // 4 blocks = 1 strip
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    let loc = &locs[0];
    assert_eq!(loc.offset, 0);
    assert_eq!(loc.length, 4 * 4096);
    assert_eq!(loc.logical_offset, 0);
    assert_eq!(loc.logical_length, 4 * 4096);

    // 1 strip → 1 allocate_chunk call, 0 append calls.
    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 0);
    assert_eq!(st.seal_calls, 1);
    assert_eq!(st.delete_calls, 0);

    // 4 data writes + 1 parity write = 5 writes.
    let writes = diskio.writes.lock().unwrap();
    assert_eq!(writes.len(), 5);
    // Each data write is 4096 bytes.
    for w in writes.iter() {
        assert_eq!(w.data.len(), 4096);
    }
}

#[tokio::test]
async fn write_stream_partial_strip_3_blocks() {
    // 3 blocks of 4 KB = 3 of 4 data shards (partial strip).
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0xCDu8; 3 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 3 * 4096);

    let writes = diskio.writes.lock().unwrap();
    // 3 data writes + 1 parity write (partial EC) = 4 writes.
    assert_eq!(writes.len(), 4);
    // Data writes are 4096 bytes each.
    for w in writes.iter().take(3) {
        assert_eq!(w.data.len(), 4096);
    }
    // Parity is also 4096 bytes (matches shard size).
    assert_eq!(writes[3].data.len(), 4096);
}

#[tokio::test]
async fn write_stream_multi_strip_same_chunk() {
    // 8 blocks = 2 strips, both in the same chunk (max_chunk_size large).
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x11u8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 1);
    assert_eq!(st.seal_calls, 1);

    // 8 data + 2 parity = 10 writes.
    let writes = diskio.writes.lock().unwrap();
    assert_eq!(writes.len(), 10);
}

#[tokio::test]
async fn write_stream_chunk_rotation() {
    // max_chunk_size = 4 * 4096 = 1 strip per chunk.
    // 8 blocks = 2 strips → 2 chunks, 1 Location each.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(4 * 4096));

    let data = vec![0x22u8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 2);
    assert_eq!(locs[0].length, 4 * 4096);
    assert_eq!(locs[0].logical_offset, 0);
    assert_eq!(locs[1].length, 4 * 4096);
    assert_eq!(locs[1].logical_offset, 4 * 4096);

    // 2 chunks → 2 allocate calls, 0 appends (each chunk gets 1 strip).
    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 2);
    assert_eq!(st.seal_calls, 2);

    // 8 data + 2 parity = 10 writes.
    let writes = diskio.writes.lock().unwrap();
    assert_eq!(writes.len(), 10);
}

#[tokio::test]
async fn write_stream_unknown_size_streaming() {
    // Unknown size — prealloc allocates on demand.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x33u8; 4 * 4096];
    let locs = writer.write_stream(data.as_slice(), None).await.unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 4 * 4096);
}

#[tokio::test]
async fn write_stream_data_integrity() {
    // Verify that the data written to disk blocks reconstructs the
    // original input (data blocks only, ignoring parity).
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let mut data = Vec::new();
    for i in 0..4 * 4096u32 {
        data.push((i % 251) as u8);
    }
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();
    assert_eq!(locs.len(), 1);

    // Reconstruct: data writes are the first 4 (data_num) writes.
    let writes = diskio.writes.lock().unwrap();
    let mut reconstructed = Vec::new();
    for w in writes.iter().take(4) {
        reconstructed.extend_from_slice(&w.data);
    }
    assert_eq!(reconstructed, data);
}

#[tokio::test]
async fn write_stream_parity_correctness() {
    // Verify parity: encode parity from the 4 data shards using
    // crow-common::ec and compare with the parity block written.
    use crow_common::ec::{decode, encode_parity_from_shards};

    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x55u8; 4 * 4096];
    writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    let writes = diskio.writes.lock().unwrap();
    let data_shards: Vec<&[u8]> = writes.iter().take(4).map(|w| w.data.as_slice()).collect();
    let expected_parity = encode_parity_from_shards(ec, &data_shards).unwrap();
    assert_eq!(writes[4].data, expected_parity[0]);

    // Full decode round-trip: 4 data + 1 parity → lose 1 data, reconstruct.
    let mut blocks: Vec<Option<Vec<u8>>> = writes.iter().take(5).map(|w| Some(w.data.clone())).collect();
    blocks[0] = None; // lose data shard 0
    let recovered = decode(ec, blocks).unwrap();
    // Reconstruct original data from recovered data shards.
    let mut reconstructed = Vec::new();
    for shard in recovered.iter().take(4) {
        reconstructed.extend_from_slice(shard);
    }
    assert_eq!(reconstructed, data);
}

#[tokio::test]
async fn write_stream_fsync_per_strip() {
    // Each strip's parity task fsyncs all disks in the strip.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x77u8; 4 * 4096];
    writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    // 1 strip → 5 unique disks (4 data + 1 parity) → 5 fsyncs.
    let fsyncs = diskio.fsyncs.lock().unwrap();
    assert_eq!(fsyncs.len(), 5);
}

#[tokio::test]
async fn write_stream_whole_strip_retry() {
    // 2 strips (8 blocks). The mock allocator assigns each segment a
    // fresh unit_offset: strip 0 → offsets 0-4, strip 1 → 5-9. Inject
    // a failure on strip 1's first data block (unit_offset 5 →
    // zone_offset 5*4096 = 20480). The writer retries the whole strip
    // via append_chunk (new placement with fresh offsets) and succeeds.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::with_fail_once(5 * 4096);
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    let data = vec![0x88u8; 8 * 4096];
    let locs = writer
        .write_stream(data.as_slice(), Some(data.len() as u64))
        .await
        .unwrap();

    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);

    // 1 allocate (chunk) + 1 append (prealloc strip 1) + 1 append
    // (retry replacement strip) = 2 appends.
    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.append_calls, 2);
    assert_eq!(st.seal_calls, 1);

    // 8 data writes + 2 parity writes = 10. The failed attempt's
    // write is not recorded (the mock returns Err before pushing).
    let writes = diskio.writes.lock().unwrap();
    assert_eq!(writes.len(), 10);

    // Data integrity: collect data writes (disks 0-3 = data, disk 4 =
    // parity), sort by zone_offset, concatenate → original bytes.
    let mut data_writes: Vec<&MockWrite> = writes
        .iter()
        .filter(|w| w.disk_high == 1000 || w.disk_high == 1001 || w.disk_high == 1002 || w.disk_high == 1003)
        .collect();
    data_writes.sort_by_key(|w| w.zone_offset);
    let mut reconstructed = Vec::new();
    for w in data_writes {
        reconstructed.extend_from_slice(&w.data);
    }
    assert_eq!(reconstructed, data);
}

// ── Push mode (ChunkIoWriter) tests ──────────────────────────────

fn block(value: u8, size: usize) -> Bytes {
    Bytes::from(vec![value; size])
}

#[tokio::test]
async fn push_mode_basic_one_strip() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio.clone(), ec, test_config(1024 * 1024));

    assert!(writer.require_data());
    for i in 0..4u8 {
        let status = writer.on_data(block(i, 4096)).await.unwrap();
        assert_eq!(status, FeedStatus::Continue);
    }
    let locs = writer.on_finish().await.unwrap();
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 4 * 4096);
    assert!(!writer.require_data());

    let st = chunkdb.snapshot();
    assert_eq!(st.allocate_calls, 1);
    assert_eq!(st.seal_calls, 1);
}

#[tokio::test]
async fn push_mode_empty_object() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio, ec, test_config(1024 * 1024));

    let locs = writer.on_finish().await.unwrap();
    assert!(locs.is_empty());
}

#[tokio::test]
async fn push_mode_on_data_after_finish() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio, ec, test_config(1024 * 1024));

    writer.on_data(block(0, 4096)).await.unwrap();
    writer.on_finish().await.unwrap();
    let result = writer.on_data(block(1, 4096)).await;
    assert!(matches!(result, Err(IoError::Finished)));
}

#[tokio::test]
async fn push_mode_on_finish_twice() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio, ec, test_config(1024 * 1024));

    writer.on_data(block(0, 4096)).await.unwrap();
    writer.on_finish().await.unwrap();
    let result = writer.on_finish().await;
    assert!(matches!(result, Err(IoError::Finished)));
}

#[tokio::test]
async fn push_mode_on_error_no_sealed() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio, ec, test_config(1024 * 1024));

    let locs = writer.on_error().await.unwrap();
    assert!(locs.is_empty());
}

#[tokio::test]
async fn push_mode_on_error_after_sealed_chunk() {
    // max_chunk_size = 4*4096 (1 strip/chunk). Push 10 blocks (2 full
    // strips + 1 partial 2-block strip): chunk 1 + chunk 2 sealed via
    // rotation, chunk 3 partial. on_error → returns 2 Locations
    // (chunks 1-2), deletes the partial chunk 3.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb.clone(), diskio, ec, test_config(4 * 4096));

    for i in 0..10u8 {
        writer.on_data(block(i, 4096)).await.unwrap();
    }
    let locs = writer.on_error().await.unwrap();
    assert_eq!(locs.len(), 2);
    assert_eq!(locs[0].length, 4 * 4096);
    assert_eq!(locs[1].length, 4 * 4096);

    let st = chunkdb.snapshot();
    assert_eq!(st.seal_calls, 2); // chunks 1-2 sealed at rotation
    assert_eq!(st.delete_calls, 1); // partial chunk 3 deleted
}

#[tokio::test]
async fn push_mode_data_integrity() {
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let mut writer = LargeObjectWriter::new(chunkdb, diskio.clone(), ec, test_config(1024 * 1024));

    let mut data = Vec::new();
    for i in 0..8u8 {
        let block_data = vec![i; 4096];
        data.extend_from_slice(&block_data);
        writer.on_data(Bytes::from(block_data)).await.unwrap();
    }
    let locs = writer.on_finish().await.unwrap();
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 8 * 4096);

    // Reconstruct from data writes (disks 1000-1003), sorted by offset.
    let writes = diskio.writes.lock().unwrap();
    let mut data_writes: Vec<&MockWrite> = writes
        .iter()
        .filter(|w| (1000..=1003).contains(&w.disk_high))
        .collect();
    data_writes.sort_by_key(|w| w.zone_offset);
    let mut reconstructed = Vec::new();
    for w in data_writes {
        reconstructed.extend_from_slice(&w.data);
    }
    assert_eq!(reconstructed, data);
}

#[tokio::test]
async fn push_mode_backpressure() {
    // max_cached_buffer = 2*4096 → channel capacity = 2. With an
    // instant mock BlockWriter the channel drains concurrently, so
    // Pause is not guaranteed. Verify the API is sound: on_data always
    // stores, returns a FeedStatus, and on_finish seals correctly.
    let chunkdb = MockChunkAllocator::new();
    let diskio = MockBlockWriter::new();
    let ec = ec_4_1();
    let config = WriterConfig {
        max_chunk_size: 1024 * 1024,
        prealloc_depth: 2,
        parity_depth: 2,
        chunk_prefetch_depth: 1,
        read_buffer_size: 4096,
        max_cached_buffer: 2 * 4096,
    };
    let mut writer = LargeObjectWriter::new(chunkdb, diskio, ec, config);

    for i in 0..6u8 {
        let _status = writer.on_data(block(i, 4096)).await.unwrap();
    }
    let locs = writer.on_finish().await.unwrap();
    assert_eq!(locs.len(), 1);
    assert_eq!(locs[0].length, 6 * 4096);
}
