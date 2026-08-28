// Copyright 2026-present buzzcrow <buzzcrow@126.com>

//! Zero-copy wrapper round-trip tests for chunkdb response types
//! (R116). Build each response flatbuffer → parse via the `Ref`
//! wrapper → verify every accessor. Malformed buffer → `valid() ==
//! false`.

use crow_protocol::chunkdb_fb::{
    FBAllocateChunkResponse, FBAllocateChunkResponseArgs, FBChunk, FBChunkArgs, FBChunkState, FBChunkStrip,
    FBChunkStripArgs, FBChunkType, FBChunkdbRetCode, FBDeleteChunkRangeResponse,
    FBDeleteChunkRangeResponseArgs, FBEcState, FBEcStrip, FBEcStripArgs, FBInt128, FBListChunksResponse,
    FBListChunksResponseArgs, FBMirrorStrip, FBMirrorStripArgs, FBSegment, FBStripBody, FBStripType,
};
use crow_protocol::fb_wrappers::chunkdb::{
    FBAllocateChunkResponseRef, FBDeleteChunkRangeResponseRef, FBListChunksResponseRef,
};
use flatbuffers::FlatBufferBuilder;

fn make_chunk_id(high: u64, low: u64) -> FBInt128 {
    FBInt128::new(high, low)
}

/// Build a mirror strip with one segment, wrapped in a chunk.
/// Returns the chunk offset. Inline to avoid `FlatBufferBuilder`
/// lifetime issues with helper functions.
macro_rules! build_mirror_chunk {
    ($fbb:expr, $chunk_id:expr, $state:expr, $ctype:expr) => {{
        let disk_id = make_chunk_id(0, 100);
        let owner_chunk = make_chunk_id(0, 200);
        let seg = FBSegment::new(&disk_id, &owner_chunk, 0, 0, 8);
        let segs = $fbb.create_vector(&[seg]);
        let mirror = FBMirrorStrip::create($fbb, &FBMirrorStripArgs { segments: Some(segs) });
        let strip = FBChunkStrip::create(
            $fbb,
            &FBChunkStripArgs {
                chunk_offset: 0,
                strip_sequence: 0,
                unit_kb: 4,
                capacity: 8,
                create_ts_ms: 1000,
                sealed_ts_ms: 0,
                sealed_length: 0,
                strip_type: FBStripType::Mirror,
                strip_body_type: FBStripBody::FBMirrorStrip,
                strip_body: Some(mirror.as_union_value()),
                usage_bitmap: None,
            },
        );
        let strips = $fbb.create_vector(&[strip]);
        FBChunk::create(
            $fbb,
            &FBChunkArgs {
                id: Some(&$chunk_id),
                state: $state,
                create_ts_ms: 1000,
                sealed_ts_ms: 0,
                capacity: 8,
                sealed_length: 0,
                strips: Some(strips),
                chunk_type: $ctype,
            },
        )
    }};
}

#[test]
fn allocate_chunk_response_success() {
    let mut fbb = FlatBufferBuilder::new();
    let chunk_id = make_chunk_id(1, 42);
    let chunk = build_mirror_chunk!(&mut fbb, chunk_id, FBChunkState::Active, FBChunkType::Repo);
    let resp = FBAllocateChunkResponse::create(
        &mut fbb,
        &FBAllocateChunkResponseArgs {
            id: 1234,
            rpc_create_nano: 999,
            ret_code: FBChunkdbRetCode::Success,
            error_msg: None,
            range_start: 0,
            range_end: 0,
            chunk: Some(chunk),
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBAllocateChunkResponseRef::new(&buf);
    assert!(view.valid());
    assert_eq!(view.request_id(), Some(1234));
    assert_eq!(view.ret_code(), FBChunkdbRetCode::Success);
    assert!(view.ok());
    assert_eq!(view.range_start(), 0);
    assert_eq!(view.range_end(), 0);
    let chunk = view.chunk().expect("chunk present");
    let id = chunk.id().expect("chunk id");
    assert_eq!(id.high(), 1);
    assert_eq!(id.low(), 42);
    assert_eq!(chunk.state(), FBChunkState::Active);
    assert_eq!(chunk.chunk_type(), FBChunkType::Repo);
    let strips = chunk.strips().expect("strips present");
    assert_eq!(strips.len(), 1);
    let strip = strips.get(0);
    assert_eq!(strip.strip_type(), FBStripType::Mirror);
    assert_eq!(strip.strip_body_type(), FBStripBody::FBMirrorStrip);
    let mirror = strip.strip_body_as_fbmirror_strip().expect("mirror strip");
    let segs = mirror.segments().expect("segments present");
    assert_eq!(segs.len(), 1);
    let seg = segs.get(0);
    assert_eq!(seg.unit_count(), 8);
}

#[test]
fn allocate_chunk_response_not_my_range() {
    let mut fbb = FlatBufferBuilder::new();
    let resp = FBAllocateChunkResponse::create(
        &mut fbb,
        &FBAllocateChunkResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBChunkdbRetCode::NotMyRange,
            error_msg: None,
            range_start: 42,
            range_end: 42,
            chunk: None,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBAllocateChunkResponseRef::new(&buf);
    assert!(view.valid());
    assert!(!view.ok());
    assert_eq!(view.ret_code(), FBChunkdbRetCode::NotMyRange);
    assert_eq!(view.range_start(), 42);
    assert_eq!(view.range_end(), 42);
    assert!(view.chunk().is_none());
}

#[test]
fn allocate_chunk_response_internal_with_error_msg() {
    let mut fbb = FlatBufferBuilder::new();
    let msg = fbb.create_string("disk write failed");
    let resp = FBAllocateChunkResponse::create(
        &mut fbb,
        &FBAllocateChunkResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBChunkdbRetCode::Internal,
            error_msg: Some(msg),
            range_start: 0,
            range_end: 0,
            chunk: None,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBAllocateChunkResponseRef::new(&buf);
    assert!(view.valid());
    assert!(!view.ok());
    assert_eq!(view.ret_code(), FBChunkdbRetCode::Internal);
    assert_eq!(view.error_msg(), Some("disk write failed"));
}

#[test]
fn malformed_buffer_invalid() {
    let view = FBAllocateChunkResponseRef::new(b"not a flatbuffer");
    assert!(!view.valid());
    assert_eq!(view.ret_code(), FBChunkdbRetCode::Internal);
    assert!(!view.ok());
    assert!(view.chunk().is_none());
}

#[test]
fn malformed_buffer_too_short() {
    let view = FBAllocateChunkResponseRef::new(b"ab");
    assert!(!view.valid());
}

#[test]
fn ec_strip_union_variant() {
    let mut fbb = FlatBufferBuilder::new();
    let disk_id = make_chunk_id(0, 10);
    let owner_chunk = make_chunk_id(0, 20);
    let seg = FBSegment::new(&disk_id, &owner_chunk, 0, 0, 4);
    let segs = fbb.create_vector(&[seg, seg]);
    let ec = FBEcStrip::create(
        &mut fbb,
        &FBEcStripArgs {
            data_num: 2,
            code_num: 1,
            ec_state: FBEcState::Parity,
            segments: Some(segs),
        },
    );
    let strip = FBChunkStrip::create(
        &mut fbb,
        &FBChunkStripArgs {
            chunk_offset: 0,
            strip_sequence: 1,
            unit_kb: 4,
            capacity: 4,
            create_ts_ms: 2000,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: FBStripType::Ec,
            strip_body_type: FBStripBody::FBEcStrip,
            strip_body: Some(ec.as_union_value()),
            usage_bitmap: None,
        },
    );
    let strips = fbb.create_vector(&[strip]);
    let chunk_id = make_chunk_id(2, 99);
    let chunk = FBChunk::create(
        &mut fbb,
        &FBChunkArgs {
            id: Some(&chunk_id),
            state: FBChunkState::Active,
            create_ts_ms: 2000,
            sealed_ts_ms: 0,
            capacity: 4,
            sealed_length: 0,
            strips: Some(strips),
            chunk_type: FBChunkType::Wal,
        },
    );
    let resp = FBAllocateChunkResponse::create(
        &mut fbb,
        &FBAllocateChunkResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBChunkdbRetCode::Success,
            error_msg: None,
            range_start: 0,
            range_end: 0,
            chunk: Some(chunk),
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBAllocateChunkResponseRef::new(&buf);
    assert!(view.ok());
    let chunk = view.chunk().expect("chunk");
    let strips = chunk.strips().expect("strips");
    let strip = strips.get(0);
    assert_eq!(strip.strip_type(), FBStripType::Ec);
    assert_eq!(strip.strip_body_type(), FBStripBody::FBEcStrip);
    let ec = strip.strip_body_as_fbec_strip().expect("ec strip");
    assert_eq!(ec.data_num(), 2);
    assert_eq!(ec.code_num(), 1);
    assert_eq!(ec.ec_state(), FBEcState::Parity);
    let segs = ec.segments().expect("segments");
    assert_eq!(segs.len(), 2);
}

#[test]
fn delete_chunk_range_response_success() {
    let mut fbb = FlatBufferBuilder::new();
    let resp = FBDeleteChunkRangeResponse::create(
        &mut fbb,
        &FBDeleteChunkRangeResponseArgs {
            id: 42,
            rpc_create_nano: 100,
            ret_code: FBChunkdbRetCode::Success,
            error_msg: None,
            range_start: 0,
            range_end: 0,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBDeleteChunkRangeResponseRef::new(&buf);
    assert!(view.valid());
    assert_eq!(view.request_id(), Some(42));
    assert!(view.ok());
    assert_eq!(view.ret_code(), FBChunkdbRetCode::Success);
}

#[test]
fn list_chunks_response_with_next_token() {
    let mut fbb = FlatBufferBuilder::new();
    let chunk1_id = make_chunk_id(0, 1);
    let chunk1 = build_mirror_chunk!(&mut fbb, chunk1_id, FBChunkState::Active, FBChunkType::Repo);
    let chunk2_id = make_chunk_id(0, 2);
    let chunk2 = build_mirror_chunk!(&mut fbb, chunk2_id, FBChunkState::Active, FBChunkType::Repo);
    let chunk_vec = fbb.create_vector(&[chunk1, chunk2]);
    let next_tok = make_chunk_id(0, 3);
    let resp = FBListChunksResponse::create(
        &mut fbb,
        &FBListChunksResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBChunkdbRetCode::Success,
            error_msg: None,
            range_start: 0,
            range_end: 0,
            chunks: Some(chunk_vec),
            next_token: Some(&next_tok),
            has_next_token: true,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBListChunksResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
    assert!(view.has_next_token());
    let tok = view.next_token().expect("next token");
    assert_eq!(tok.high(), 0);
    assert_eq!(tok.low(), 3);
    let chunk_list = view.chunks().expect("chunks present");
    assert_eq!(chunk_list.len(), 2);
    let id0 = chunk_list.get(0).id().expect("chunk 0 id");
    assert_eq!(id0.low(), 1);
    let id1 = chunk_list.get(1).id().expect("chunk 1 id");
    assert_eq!(id1.low(), 2);
}
