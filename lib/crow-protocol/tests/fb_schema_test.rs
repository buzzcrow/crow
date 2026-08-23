// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Flatbuffer control-message schema layout tests (R104).

use crow_protocol::diskio_fb::{
    FBDiskFsyncRequest, FBDiskFsyncRequestArgs, FBDiskReadRequest, FBDiskReadRequestArgs, FBDiskWriteRequest,
    FBDiskWriteRequestArgs, FBInt128 as FBDiskInt128,
};
use crow_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, FBDiskIoRetCode, FBInt128, FBMsgType, FBRetCode,
};
use flatbuffers::FlatBufferBuilder;

#[test]
fn msg_type_common_range() {
    // flatbuffers 25.x generates enums as newtype structs with
    // associated consts; compare via the const + inner i16 field.
    assert_eq!(FBMsgType::EUnknownRequest.0, 0);
    assert_eq!(FBMsgType::EUnknownResponse.0, 1);
    assert_eq!(FBMsgType::EConnectionPingRequest.0, 2);
    assert_eq!(FBMsgType::EConnectionPingResponse.0, 3);
}

#[test]
fn msg_type_diskio_range() {
    assert_eq!(FBMsgType::EDiskWriteRequest.0, 3600);
    assert_eq!(FBMsgType::EDiskWriteResponse.0, 3601);
    assert_eq!(FBMsgType::EDiskReadRequest.0, 3602);
    assert_eq!(FBMsgType::EDiskReadResponse.0, 3603);
    assert_eq!(FBMsgType::EDiskFsyncRequest.0, 3604);
    assert_eq!(FBMsgType::EDiskFsyncResponse.0, 3605);
}

#[test]
fn ret_code_common_subset() {
    assert_eq!(FBRetCode::Success.0, 0);
    assert_eq!(FBRetCode::Error.0, 1);
    assert_eq!(FBRetCode::HaveNotSupport.0, 2);
}

#[test]
fn inline_struct_sizes() {
    assert_eq!(std::mem::size_of::<FBInt128>(), 16);
}

#[test]
fn ping_request_layout() {
    // Use id=1 as template (id=0 triggers flatbuffer default-value
    // optimization which omits the field from the vtable).
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 1,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let template = fbb.finished_data().to_vec();

    let mut fbb2 = FlatBufferBuilder::new();
    let req2 = ConnectionPingRequest::create(
        &mut fbb2,
        &ConnectionPingRequestArgs {
            id: 0xDEAD_BEEF_CAFE_BABE,
            rpc_create_nano: 0,
        },
    );
    fbb2.finish(req2, None);
    let with_id = fbb2.finished_data().to_vec();

    assert_eq!(template.len(), with_id.len(), "same size regardless of id");
    let diffs: Vec<usize> = (0..template.len())
        .filter(|&i| template[i] != with_id[i])
        .collect();
    eprintln!("template = {template:02x?}");
    eprintln!("with_id  = {with_id:02x?}");
    eprintln!("diff offsets = {diffs:?}");
    // The id field is a u64 at a fixed offset — verify it's 8 consecutive bytes
    assert_eq!(diffs.len(), 8, "id is u64 = 8 bytes diff");
    assert_eq!(diffs[1] - diffs[0], 1, "consecutive bytes");
    assert_eq!(diffs[7] - diffs[0], 7, "8 consecutive bytes");
}

#[test]
fn disk_io_ret_code_values() {
    assert_eq!(FBDiskIoRetCode::Success.0, 0);
    assert_eq!(FBDiskIoRetCode::DiskNotExist.0, 1);
    assert_eq!(FBDiskIoRetCode::ZoneNotExist.0, 2);
    assert_eq!(FBDiskIoRetCode::IoError.0, 3);
    assert_eq!(FBDiskIoRetCode::PartialWrite.0, 4);
    assert_eq!(FBDiskIoRetCode::InvalidAlignment.0, 5);
    assert_eq!(FBDiskIoRetCode::ConnectionError.0, 6);
}

#[test]
fn disk_write_request_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let mut id_bytes = [0u8; 16];
    id_bytes[0..8].copy_from_slice(&0xAABB_u64.to_le_bytes());
    id_bytes[8..16].copy_from_slice(&0xCCDD_u64.to_le_bytes());
    let disk_id = FBDiskInt128(id_bytes);
    let req = FBDiskWriteRequest::create(
        &mut fbb,
        &FBDiskWriteRequestArgs {
            id: 1001,
            rpc_create_nano: 0,
            disk_id: Some(&disk_id),
            zone_index: 2,
            zone_offset: 4096,
            size: 4096,
        },
    );
    fbb.finish(req, None);
    let buf = fbb.finished_data();
    let parsed = flatbuffers::root::<FBDiskWriteRequest>(buf).expect("valid root");
    let parsed_id = parsed.disk_id().expect("disk_id present");
    assert_eq!(parsed_id.high(), 0xAABB);
    assert_eq!(parsed_id.low(), 0xCCDD);
    assert_eq!(parsed.id(), 1001);
    assert_eq!(parsed.zone_index(), 2);
    assert_eq!(parsed.zone_offset(), 4096);
    assert_eq!(parsed.size(), 4096);
}

#[test]
fn disk_read_request_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let mut id_bytes = [0u8; 16];
    id_bytes[0..8].copy_from_slice(&1u64.to_le_bytes());
    id_bytes[8..16].copy_from_slice(&2u64.to_le_bytes());
    let disk_id = FBDiskInt128(id_bytes);
    let req = FBDiskReadRequest::create(
        &mut fbb,
        &FBDiskReadRequestArgs {
            id: 2002,
            rpc_create_nano: 0,
            disk_id: Some(&disk_id),
            zone_index: 5,
            zone_offset: 8192,
            size: 2048,
            test_pattern_offset: 0x1234,
        },
    );
    fbb.finish(req, None);
    let buf = fbb.finished_data();
    let parsed = flatbuffers::root::<FBDiskReadRequest>(buf).expect("valid root");
    let parsed_id = parsed.disk_id().expect("disk_id present");
    assert_eq!(parsed_id.high(), 1);
    assert_eq!(parsed_id.low(), 2);
    assert_eq!(parsed.id(), 2002);
    assert_eq!(parsed.zone_index(), 5);
    assert_eq!(parsed.zone_offset(), 8192);
    assert_eq!(parsed.size(), 2048);
    assert_eq!(parsed.test_pattern_offset(), 0x1234);
}

#[test]
fn disk_fsync_request_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let mut id_bytes = [0u8; 16];
    id_bytes[0..8].copy_from_slice(&99u64.to_le_bytes());
    id_bytes[8..16].copy_from_slice(&100u64.to_le_bytes());
    let disk_id = FBDiskInt128(id_bytes);
    let req = FBDiskFsyncRequest::create(
        &mut fbb,
        &FBDiskFsyncRequestArgs {
            id: 3003,
            rpc_create_nano: 0,
            disk_id: Some(&disk_id),
        },
    );
    fbb.finish(req, None);
    let buf = fbb.finished_data();
    let parsed = flatbuffers::root::<FBDiskFsyncRequest>(buf).expect("valid root");
    let parsed_id = parsed.disk_id().expect("disk_id present");
    assert_eq!(parsed_id.high(), 99);
    assert_eq!(parsed_id.low(), 100);
    assert_eq!(parsed.id(), 3003);
}
