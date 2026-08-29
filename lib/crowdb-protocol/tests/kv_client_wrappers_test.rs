// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Zero-copy wrapper round-trip tests for KV client-facing response
//! types (R117). Build each response flatbuffer → parse via the `Ref`
//! wrapper → verify every accessor. Malformed buffer → `valid() ==
//! false`.

use crowdb_protocol::fb_wrappers::kv_client::{
    FBCreateSnapshotResponseRef, FBKvJournalScanResponseRef, FBKvResponseRef, FBKvScanResponseRef,
    FBListSnapshotsResponseRef, FBReleaseSnapshotResponseRef, FBSnapshotScanResponseRef,
    FBWatchNotifyErrorRef, FBWatchNotifyRef,
};
use crowdb_protocol::kv_client_fb::{
    FBBytes, FBBytesArgs, FBCreateSnapshotResponse, FBCreateSnapshotResponseArgs, FBKvClientRetCode,
    FBKvJournalOp, FBKvJournalOpArgs, FBKvJournalScanResponse, FBKvJournalScanResponseArgs, FBKvResponse,
    FBKvResponseArgs, FBKvScanItem, FBKvScanItemArgs, FBKvScanResponse, FBKvScanResponseArgs,
    FBListSnapshotsResponse, FBListSnapshotsResponseArgs, FBReleaseSnapshotResponse,
    FBReleaseSnapshotResponseArgs, FBSnapshotInfo, FBSnapshotInfoArgs, FBSnapshotScanResponse,
    FBSnapshotScanResponseArgs, FBWatchNotify, FBWatchNotifyArgs, FBWatchNotifyError, FBWatchNotifyErrorArgs,
};
use flatbuffers::FlatBufferBuilder;

#[test]
fn kv_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let value = fbb.create_vector(b"hello");
    let hint = fbb.create_string("127.0.0.1:28201");
    let resp = FBKvResponse::create(
        &mut fbb,
        &FBKvResponseArgs {
            id: 1234,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            version: 1,
            ok: true,
            revision: 42,
            not_found: false,
            not_leader_hint: Some(hint),
            request_id: 1234,
            request_create_ms: 999,
            value: Some(value),
            read_slot: 7,
            safe_slot: 5,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBKvResponseRef::new(&buf);
    assert!(view.valid());
    assert_eq!(view.request_id(), Some(1234));
    assert_eq!(view.ret_code(), FBKvClientRetCode::Success);
    assert!(view.ok());
    assert_eq!(view.revision(), 42);
    assert!(!view.not_found());
    assert_eq!(view.not_leader_hint(), Some("127.0.0.1:28201"));
    assert_eq!(view.value(), Some(b"hello".as_slice()));
    assert_eq!(view.read_slot(), 7);
    assert_eq!(view.safe_slot(), 5);
}

#[test]
fn kv_response_not_leader_ret_code() {
    let mut fbb = FlatBufferBuilder::new();
    let resp = FBKvResponse::create(
        &mut fbb,
        &FBKvResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::NotLeader,
            error_msg: None,
            version: 1,
            ok: false,
            revision: 0,
            not_found: false,
            not_leader_hint: None,
            request_id: 1,
            request_create_ms: 0,
            value: None,
            read_slot: 0,
            safe_slot: 0,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBKvResponseRef::new(&buf);
    assert_eq!(view.ret_code(), FBKvClientRetCode::NotLeader);
    assert!(!view.ok());
}

#[test]
fn kv_response_malformed() {
    let view = FBKvResponseRef::new(&[0u8; 3]);
    assert!(!view.valid());
    assert_eq!(view.ret_code(), FBKvClientRetCode::Internal);
}

#[test]
fn kv_scan_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let key1 = fbb.create_vector(b"k1");
    let val1 = fbb.create_vector(b"v1");
    let item1 = FBKvScanItem::create(
        &mut fbb,
        &FBKvScanItemArgs {
            key: Some(key1),
            value: Some(val1),
        },
    );
    let key2 = fbb.create_vector(b"k2");
    let val2 = fbb.create_vector(b"v2");
    let item2 = FBKvScanItem::create(
        &mut fbb,
        &FBKvScanItemArgs {
            key: Some(key2),
            value: Some(val2),
        },
    );
    let items = fbb.create_vector(&[item1, item2]);
    let resp = FBKvScanResponse::create(
        &mut fbb,
        &FBKvScanResponseArgs {
            id: 55,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            version: 1,
            ok: true,
            truncated: true,
            items: Some(items),
            request_id: 55,
            request_create_ms: 0,
            read_slot: 99,
            not_leader_hint: None,
            count: 0,
            timed_out: false,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBKvScanResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
    assert!(view.truncated());
    assert_eq!(view.read_slot(), 99);
    let items = view.items().expect("items present");
    assert_eq!(items.len(), 2);
    assert_eq!(items.get(0).key().map(|v| v.bytes()), Some(b"k1".as_slice()));
    assert_eq!(items.get(0).value().map(|v| v.bytes()), Some(b"v1".as_slice()));
    assert_eq!(items.get(1).key().map(|v| v.bytes()), Some(b"k2".as_slice()));
}

#[test]
fn kv_journal_scan_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let key = fbb.create_vector(b"jk");
    let val = fbb.create_vector(b"jv");
    let op = FBKvJournalOp::create(
        &mut fbb,
        &FBKvJournalOpArgs {
            key: Some(key),
            value: Some(val),
            is_delete: false,
            slot: 10,
        },
    );
    let ops = fbb.create_vector(&[op]);
    let resp = FBKvJournalScanResponse::create(
        &mut fbb,
        &FBKvJournalScanResponseArgs {
            id: 7,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            version: 1,
            ok: true,
            ops: Some(ops),
            truncated: false,
            last_op_slot: 10,
            read_slot: 15,
            not_leader_hint: None,
            request_id: 7,
            request_create_ms: 0,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBKvJournalScanResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
    assert!(!view.truncated());
    assert_eq!(view.last_op_slot(), 10);
    assert_eq!(view.read_slot(), 15);
    let ops = view.ops().expect("ops present");
    assert_eq!(ops.len(), 1);
    assert_eq!(ops.get(0).slot(), 10);
    assert!(!ops.get(0).is_delete());
}

#[test]
fn kv_journal_scan_gc_gap_ret_code() {
    let mut fbb = FlatBufferBuilder::new();
    let resp = FBKvJournalScanResponse::create(
        &mut fbb,
        &FBKvJournalScanResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::JournalScanGcGap,
            error_msg: None,
            version: 1,
            ok: false,
            ops: None,
            truncated: false,
            last_op_slot: 0,
            read_slot: 0,
            not_leader_hint: None,
            request_id: 1,
            request_create_ms: 0,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBKvJournalScanResponseRef::new(&buf);
    assert_eq!(view.ret_code(), FBKvClientRetCode::JournalScanGcGap);
    assert_ne!(view.ret_code(), FBKvClientRetCode::NotLeader);
}

#[test]
fn create_snapshot_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let hint = fbb.create_string("leader:28201");
    let resp = FBCreateSnapshotResponse::create(
        &mut fbb,
        &FBCreateSnapshotResponseArgs {
            id: 9,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            ok: true,
            snapshot_handle: 0xABCD,
            at_slot: 100,
            not_leader_hint: Some(hint),
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBCreateSnapshotResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
    assert_eq!(view.snapshot_handle(), 0xABCD);
    assert_eq!(view.at_slot(), 100);
    assert_eq!(view.not_leader_hint(), Some("leader:28201"));
}

#[test]
fn list_snapshots_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let snap = FBSnapshotInfo::create(
        &mut fbb,
        &FBSnapshotInfoArgs {
            snapshot_handle: 1,
            at_slot: 50,
            lease_remaining_ms: 30_000,
        },
    );
    let snaps = fbb.create_vector(&[snap]);
    let resp = FBListSnapshotsResponse::create(
        &mut fbb,
        &FBListSnapshotsResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            ok: true,
            snapshots: Some(snaps),
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBListSnapshotsResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
    let snaps = view.snapshots().expect("snapshots present");
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps.get(0).snapshot_handle(), 1);
    assert_eq!(snaps.get(0).at_slot(), 50);
    assert_eq!(snaps.get(0).lease_remaining_ms(), 30_000);
}

#[test]
fn snapshot_scan_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let key = fbb.create_vector(b"sk");
    let val = fbb.create_vector(b"sv");
    let item = FBKvScanItem::create(
        &mut fbb,
        &FBKvScanItemArgs {
            key: Some(key),
            value: Some(val),
        },
    );
    let items = fbb.create_vector(&[item]);
    let resp = FBSnapshotScanResponse::create(
        &mut fbb,
        &FBSnapshotScanResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            ok: true,
            truncated: false,
            items: Some(items),
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBSnapshotScanResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
    let items = view.items().expect("items present");
    assert_eq!(items.len(), 1);
    assert_eq!(items.get(0).key().map(|v| v.bytes()), Some(b"sk".as_slice()));
}

#[test]
fn release_snapshot_response_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let resp = FBReleaseSnapshotResponse::create(
        &mut fbb,
        &FBReleaseSnapshotResponseArgs {
            id: 1,
            rpc_create_nano: 0,
            ret_code: FBKvClientRetCode::Success,
            error_msg: None,
            ok: true,
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBReleaseSnapshotResponseRef::new(&buf);
    assert!(view.valid());
    assert!(view.ok());
}

#[test]
fn watch_notify_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    // Build two FBBytes wrappers for keys, two for values.
    let k1_data = fbb.create_vector(b"key1");
    let k1 = FBBytes::create(&mut fbb, &FBBytesArgs { data: Some(k1_data) });
    let k2_data = fbb.create_vector(b"key2");
    let k2 = FBBytes::create(&mut fbb, &FBBytesArgs { data: Some(k2_data) });
    let v1_data = fbb.create_vector(b"val1");
    let v1 = FBBytes::create(&mut fbb, &FBBytesArgs { data: Some(v1_data) });
    // Empty value = Delete.
    let v2_data = fbb.create_vector(b"");
    let v2 = FBBytes::create(&mut fbb, &FBBytesArgs { data: Some(v2_data) });
    let keys = fbb.create_vector(&[k1, k2]);
    let values = fbb.create_vector(&[v1, v2]);
    let prefix = fbb.create_vector(b"pref");
    let notify = FBWatchNotify::create(
        &mut fbb,
        &FBWatchNotifyArgs {
            id: 1,
            rpc_create_nano: 0,
            group_id: 5,
            prefix: Some(prefix),
            keys: Some(keys),
            slot: 77,
            values: Some(values),
        },
    );
    fbb.finish(notify, None);
    let buf = fbb.finished_data().to_vec();

    let view = FBWatchNotifyRef::new(&buf);
    assert!(view.valid());
    assert_eq!(view.group_id(), 5);
    assert_eq!(view.prefix(), Some(b"pref".as_slice()));
    assert_eq!(view.slot(), 77);
    let keys: Vec<&[u8]> = view.keys().expect("keys present").collect();
    assert_eq!(keys, vec![b"key1".as_slice(), b"key2".as_slice()]);
    let values: Vec<&[u8]> = view.values().expect("values present").collect();
    assert_eq!(values, vec![b"val1".as_slice(), b"".as_slice()]);
}

#[test]
fn watch_notify_error_round_trip() {
    let mut fbb = FlatBufferBuilder::new();
    let hint = fbb.create_string("leader:28201");
    let err = fbb.create_string("not leader");
    let resp = FBWatchNotifyError::create(
        &mut fbb,
        &FBWatchNotifyErrorArgs {
            id: 1,
            rpc_create_nano: 0,
            group_id: 3,
            not_leader_hint: Some(hint),
            error: Some(err),
        },
    );
    fbb.finish(resp, None);
    let buf = fbb.finished_data().to_vec();
    let view = FBWatchNotifyErrorRef::new(&buf);
    assert!(view.valid());
    assert_eq!(view.group_id(), 3);
    assert_eq!(view.not_leader_hint(), Some("leader:28201"));
    assert_eq!(view.error(), Some("not leader"));
}

#[test]
fn malformed_buffers_invalid() {
    let empty: &[u8] = &[];
    assert!(!FBKvResponseRef::new(empty).valid());
    assert!(!FBKvScanResponseRef::new(empty).valid());
    assert!(!FBKvJournalScanResponseRef::new(empty).valid());
    assert!(!FBCreateSnapshotResponseRef::new(empty).valid());
    assert!(!FBListSnapshotsResponseRef::new(empty).valid());
    assert!(!FBSnapshotScanResponseRef::new(empty).valid());
    assert!(!FBReleaseSnapshotResponseRef::new(empty).valid());
    assert!(!FBWatchNotifyRef::new(empty).valid());
    assert!(!FBWatchNotifyErrorRef::new(empty).valid());
}
