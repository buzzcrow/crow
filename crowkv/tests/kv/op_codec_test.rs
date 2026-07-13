// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Isolated unit tests for `kv/op` wire-format encode/decode.
//!
//! `mem_kv_test.rs` exercises `Batch::decode` indirectly through `InMemKV::apply`.
//! These tests target `Batch::decode` directly: round-trip, truncation, empty
//! payload, multi-op, large keys/values, and boundary conditions.

use crowkv::kv::{Batch, BatchOp, Op};

fn put(key: &[u8], value: &[u8]) -> BatchOp {
    BatchOp {
        key: key.to_vec(),
        op: Op::Put(value.to_vec()),
    }
}

fn del(key: &[u8]) -> BatchOp {
    BatchOp {
        key: key.to_vec(),
        op: Op::Delete,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn encode(ops: &[(&[u8], Option<&[u8]>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ops.len() as u8);
    for (key, value_opt) in ops {
        buf.push(u8::from(value_opt.is_none())); // 0 = Put, 1 = Delete
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        let vlen = value_opt.map_or(0, <[u8]>::len) as u32;
        buf.extend_from_slice(&vlen.to_le_bytes());
        if let Some(v) = value_opt {
            buf.extend_from_slice(v);
        }
    }
    buf
}

#[test]
fn decode_empty_payload_returns_empty_batch() {
    let batch = Batch::decode(&[]);
    assert!(batch.ops.is_empty());
}

#[test]
fn decode_zero_ops_returns_empty_batch() {
    let buf = vec![0u8];
    let batch = Batch::decode(&buf);
    assert!(batch.ops.is_empty());
}

#[test]
fn decode_single_put_roundtrips() {
    let buf = encode(&[(b"k", Some(b"v"))]);
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops, vec![put(b"k", b"v")]);
}

#[test]
fn decode_single_delete_roundtrips() {
    let buf = encode(&[(b"k", None)]);
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops, vec![del(b"k")]);
}

#[test]
fn decode_multi_op_roundtrips() {
    let buf = encode(&[(b"k1", Some(b"v1")), (b"k2", None), (b"k3", Some(b"v3"))]);
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops, vec![put(b"k1", b"v1"), del(b"k2"), put(b"k3", b"v3")]);
}

#[test]
fn decode_empty_value_put() {
    let buf = encode(&[(b"k", Some(b""))]);
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops, vec![put(b"k", b"")]);
}

#[test]
fn decode_large_key_and_value() {
    let key = vec![0x42u8; 1024];
    let value = vec![0xABu8; 4096];
    let buf = encode(&[(&key, Some(&value))]);
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops.len(), 1);
    assert_eq!(batch.ops[0].key, key);
    assert_eq!(batch.ops[0].op, Op::Put(value.clone()));
}

#[test]
fn decode_truncated_after_op_count_stops_cleanly() {
    // Claim 3 ops but provide no data.
    let buf = vec![3u8];
    let batch = Batch::decode(&buf);
    assert!(batch.ops.is_empty(), "truncated payload yields no ops");
}

#[test]
fn decode_truncated_mid_key_stops_cleanly() {
    // Claim 1 Put, key_len=10, but only 3 key bytes.
    let mut buf = vec![1u8, 0u8]; // 1 op, Put
    buf.extend_from_slice(&10u32.to_le_bytes()); // key_len = 10
    buf.extend_from_slice(b"abc"); // only 3 bytes
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops.len(), 1);
    // The decoder reads key_len=10 but only 3 bytes remain; unwrap_or(&[]) yields empty.
    assert_eq!(batch.ops[0].key, b"");
}

#[test]
fn decode_truncated_mid_value_stops_cleanly() {
    // Claim 1 Put, key="k", value_len=10, but only 3 value bytes.
    let mut buf = vec![1u8, 0u8]; // 1 op, Put
    buf.extend_from_slice(&1u32.to_le_bytes()); // key_len = 1
    buf.push(b'k');
    buf.extend_from_slice(&10u32.to_le_bytes()); // value_len = 10
    buf.extend_from_slice(b"abc"); // only 3 bytes
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops.len(), 1);
    // value_len=10 but only 3 bytes remain; unwrap_or(&[]) yields empty.
    assert_eq!(batch.ops[0].op, Op::Put(b"".to_vec()));
}

#[test]
fn decode_unknown_kind_byte_becomes_delete() {
    // kind=2 is not 0 (Put) so it falls to the else branch → Delete.
    let mut buf = vec![1u8, 2u8]; // 1 op, unknown kind
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.push(b'k');
    buf.extend_from_slice(&0u32.to_le_bytes());
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops, vec![del(b"k")]);
}

#[test]
fn decode_max_op_count_u8() {
    // 255 ops, each a single-byte key Put with empty value.
    let mut buf = vec![255u8];
    for i in 0..255u8 {
        buf.push(0u8); // Put
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(i);
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    let batch = Batch::decode(&buf);
    assert_eq!(batch.ops.len(), 255);
}
