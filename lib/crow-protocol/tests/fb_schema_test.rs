// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Flatbuffer control-message schema layout tests (R104).

use crow_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs, FBInt128, FBMsgType, FBRetCode};
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
