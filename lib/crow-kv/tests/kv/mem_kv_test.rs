// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `InMemKV` behavior: shared `KVEngine` conformance suite (see
//! `conformance.rs`) plus `InMemKV`-only cases (`clear`, wire-format decode).

use crate::mem_kv::InMemKV;
use crate::test_util::iter_all_dyn;
use bytes::Bytes;
use crow_kv::kv::{Batch, Cell, KVEngine};

use super::conformance;
use super::conformance::{batch, del, put};

#[test]
fn highest_slot_wins_regardless_of_apply_order() {
    conformance::highest_slot_wins_regardless_of_apply_order(&InMemKV::new());
}

#[test]
fn equal_slot_is_idempotent_noop() {
    conformance::equal_slot_is_idempotent_noop(&InMemKV::new());
}

#[test]
fn delete_writes_tombstone() {
    conformance::delete_writes_tombstone(&InMemKV::new());
}

#[test]
fn intra_batch_last_occurrence_wins() {
    conformance::intra_batch_last_occurrence_wins(&InMemKV::new());
}

#[test]
fn scan_is_ordered_prefix_filtered_and_truncates() {
    conformance::scan_is_ordered_prefix_filtered_and_truncates(&InMemKV::new());
}

#[test]
fn compare_is_empty_for_identical_state_and_detects_divergence() {
    conformance::compare_is_empty_for_identical_state_and_detects_divergence(
        &InMemKV::new(),
        &InMemKV::new(),
    );
}

/// Regression guard: `InMemKV` never
/// needs real I/O, so `get`/`scan`/`apply` must always resolve `Ready` — a
/// `matches!` check on the raw `KVFuture` *before* unwrapping, so a future
/// accidental switch to `Pending` fails loudly here first, not just via a
/// wrong unwrapped value.
#[test]
fn get_scan_apply_always_resolve_ready() {
    use crow_kv::kv::KVFuture;

    let e = InMemKV::new();
    assert!(matches!(
        e.apply(1, &batch(vec![put(b"k", b"v")])),
        KVFuture::Ready(_)
    ));
    assert!(matches!(e.get(b"k"), KVFuture::Ready(_)));
    assert!(matches!(e.scan(b"", b"", 0), KVFuture::Ready(_)));
}

#[test]
fn snapshot_export_import_round_trip() {
    conformance::snapshot_export_import_round_trip(&InMemKV::new(), &InMemKV::new());
}

#[test]
fn is_healthy_defaults_to_true() {
    let e = InMemKV::new();
    assert!(e.is_healthy(), "InMemKV has no I/O path to fail");
}

#[test]
fn delete_nonexistent_key_is_noop() {
    let e = InMemKV::new();
    e.apply(1, &batch(vec![del(b"missing")])).into_ready().unwrap();
    assert_eq!(e.get(b"missing").into_ready(), None);
    assert_eq!(e.live_key_count(), 0);
    let all = iter_all_dyn(&e);
    assert_eq!(
        all,
        vec![(b"missing".to_vec(), 1, Cell::Tombstone)],
        "tombstone recorded at applied slot even for non-existent key"
    );
}

#[test]
fn clear_drops_all_state() {
    let e = InMemKV::new();
    e.apply(1, &batch(vec![put(b"k", b"v")])).into_ready().unwrap();
    e.clear();
    assert_eq!(e.get(b"k").into_ready(), None);
    assert_eq!(e.live_key_count(), 0);
    assert!(iter_all_dyn(&e).is_empty());
}

#[test]
fn batch_decode_matches_put_delete_wire_format() {
    // Mirror PxKvStore::encode_kv_payload: [count][kind][klen][k][vlen][v].
    let mut buf = Vec::new();
    buf.extend_from_slice(&2u16.to_le_bytes()); // op_count
                                                // Put k1=v1
    buf.push(0u8);
    buf.extend_from_slice(&2u32.to_le_bytes());
    buf.extend_from_slice(b"k1");
    buf.extend_from_slice(&2u32.to_le_bytes());
    buf.extend_from_slice(b"v1");
    // Delete k2
    buf.push(1u8);
    buf.extend_from_slice(&2u32.to_le_bytes());
    buf.extend_from_slice(b"k2");
    buf.extend_from_slice(&0u32.to_le_bytes());

    let decoded = Batch::decode(&Bytes::from(buf));
    assert_eq!(
        decoded,
        batch(vec![put(b"k1", b"v1"), del(b"k2")]),
        "decode must round-trip the wire format"
    );

    // Empty payload decodes to an empty batch (NoOp repair fill).
    assert!(Batch::decode(&Bytes::new()).ops.is_empty());
}
