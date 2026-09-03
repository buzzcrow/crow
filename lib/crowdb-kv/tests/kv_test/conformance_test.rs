// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared `KVEngine` conformance suite, parametrized over any implementation
//! (`InMemKV`, `CrowdbTreeEngine`) so both get identical behavioral coverage:
//! per-key highest-slot-wins apply, tombstones, apply idempotency, ordered
//! prefix scan with truncation, intra-batch dedup, and cross-engine compare.
//!
//! Each function is a reusable assertion body; callers wrap them in their own
//! `#[test]` functions with a freshly constructed engine.

use crate::test_util::{compare_dyn, iter_all_dyn};
use bytes::Bytes;
use crowdb_kv::kv::{Batch, BatchOp, Cell, KVEngine, Op};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

pub fn put(key: &[u8], value: &[u8]) -> BatchOp {
    BatchOp {
        key: Bytes::copy_from_slice(key),
        op: Op::Put(Bytes::copy_from_slice(value)),
    }
}

pub fn del(key: &[u8]) -> BatchOp {
    BatchOp {
        key: Bytes::copy_from_slice(key),
        op: Op::Delete,
    }
}

pub fn batch(ops: Vec<BatchOp>) -> Batch {
    Batch { ops }
}

pub fn highest_slot_wins_regardless_of_apply_order(e: &dyn KVEngine) {
    // Apply the higher slot first, then a lower slot for the same key.
    e.apply(5, &batch(vec![put(b"k", b"v5")])).into_ready().unwrap();
    e.apply(3, &batch(vec![put(b"k", b"v3")])).into_ready().unwrap();
    assert_eq!(
        e.get(b"k").into_ready(),
        Some((5, b"v5".to_vec())),
        "lower slot must not overwrite"
    );

    // A strictly higher slot wins.
    e.apply(7, &batch(vec![put(b"k", b"v7")])).into_ready().unwrap();
    assert_eq!(e.get(b"k").into_ready(), Some((7, b"v7".to_vec())));
}

pub fn equal_slot_is_idempotent_noop(e: &dyn KVEngine) {
    e.apply(4, &batch(vec![put(b"k", b"first")]))
        .into_ready()
        .unwrap();
    // Re-applying the same slot must not change the stored value.
    e.apply(4, &batch(vec![put(b"k", b"second")]))
        .into_ready()
        .unwrap();
    assert_eq!(e.get(b"k").into_ready(), Some((4, b"first".to_vec())));
}

pub fn delete_writes_tombstone(e: &dyn KVEngine) {
    e.apply(1, &batch(vec![put(b"k", b"v")])).into_ready().unwrap();
    e.apply(2, &batch(vec![del(b"k")])).into_ready().unwrap();
    assert_eq!(e.get(b"k").into_ready(), None, "tombstoned key is not live");
    // The tombstone is retained internally (visible via iter_all) at its slot.
    let all = iter_all_dyn(e);
    assert_eq!(all, vec![(b"k".to_vec(), 2, Cell::Tombstone)]);
}

pub fn intra_batch_last_occurrence_wins(e: &dyn KVEngine) {
    e.apply(1, &batch(vec![put(b"k", b"a"), del(b"k"), put(b"k", b"final")]))
        .into_ready()
        .unwrap();
    assert_eq!(e.get(b"k").into_ready(), Some((1, b"final".to_vec())));
}

pub fn scan_is_ordered_prefix_filtered_and_truncates(e: &dyn KVEngine) {
    e.apply(
        1,
        &batch(vec![put(b"a:1", b"1"), put(b"a:2", b"2"), put(b"a:3", b"3")]),
    )
    .into_ready()
    .unwrap();
    e.apply(2, &batch(vec![put(b"b:1", b"x")])).into_ready().unwrap();
    e.apply(3, &batch(vec![del(b"a:2")])).into_ready().unwrap();

    // Unlimited: only live "a:" keys, in order, tombstone excluded.
    let (items, truncated) = e.scan(b"a:", b"", b"", 0, 0, false, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(keys, vec![b"a:1".to_vec(), b"a:3".to_vec()]);
    assert!(!truncated);

    // Limit smaller than the match count sets truncated.
    let (items, truncated) = e.scan(b"a:", b"", b"", 1, 0, false, 0).into_ready().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0.as_ref(), b"a:1");
    assert!(truncated);

    // Empty prefix scans everything live.
    let (all, _) = e.scan(b"", b"", b"", 0, 0, false, 0).into_ready().unwrap();
    assert_eq!(all.len(), 3); // a:1, a:3, b:1

    // start_after: returns keys strictly greater than start_after.
    let (page, truncated) = e.scan(b"a:", b"a:1", b"", 0, 0, false, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = page.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(keys, vec![b"a:3".to_vec()]);
    assert!(!truncated);

    // start_after with limit: pagination returns next page.
    let (page1, trunc1) = e.scan(b"a:", b"", b"", 1, 0, false, 0).into_ready().unwrap();
    assert_eq!(page1.len(), 1);
    assert_eq!(page1[0].0.as_ref(), b"a:1");
    assert!(trunc1);
    let (page2, trunc2) = e.scan(b"a:", b"a:1", b"", 1, 0, false, 0).into_ready().unwrap();
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0].0.as_ref(), b"a:3");
    assert!(!trunc2);
}

/// `end_key` is an exclusive upper bound: only keys strictly less than
/// `end_key` are returned. Empty `end_key` = unbounded (today's behavior).
/// `prefix` + `end_key` together intersect correctly.
pub fn scan_end_key_exclusive_upper_bound(e: &dyn KVEngine) {
    // Keys: k00, k01, k02, k03, k04
    let mut ops = Vec::new();
    for i in 0..5 {
        ops.push(put(format!("k0{i}").as_bytes(), format!("v{i}").as_bytes()));
    }
    e.apply(1, &batch(ops)).into_ready().unwrap();

    // end_key="k03": returns k00, k01, k02 (strictly less than k03).
    let (items, truncated) = e.scan(b"", b"", b"k03", 0, 0, false, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(keys, vec![b"k00".to_vec(), b"k01".to_vec(), b"k02".to_vec()]);
    assert!(!truncated);

    // start_after + end_key: (k01, k04) → k02, k03.
    let (items, _) = e.scan(b"", b"k01", b"k04", 0, 0, false, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(keys, vec![b"k02".to_vec(), b"k03".to_vec()]);

    // prefix + end_key: prefix="k0", end_key="k02" → k00, k01.
    let (items, _) = e.scan(b"k0", b"", b"k02", 0, 0, false, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(keys, vec![b"k00".to_vec(), b"k01".to_vec()]);

    // Empty end_key = unbounded (all 5 keys).
    let (items, _) = e.scan(b"", b"", b"", 0, 0, false, 0).into_ready().unwrap();
    assert_eq!(items.len(), 5);

    // end_key at the very start: no keys are < "k00".
    let (items, _) = e.scan(b"", b"", b"k00", 0, 0, false, 0).into_ready().unwrap();
    assert!(items.is_empty());

    // end_key with limit: limit=1, end_key="k04" → k00, truncated.
    let (items, truncated) = e.scan(b"", b"", b"k04", 1, 0, false, 0).into_ready().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0.as_ref(), b"k00");
    assert!(truncated);
}

/// `byte_budget` caps the total key+value bytes in a scan response. The scan
/// stops with `truncated = true` when the budget is exceeded, always returning
/// at least one entry (so a single oversized entry still makes progress).
pub fn scan_byte_budget_stops_and_truncates(e: &dyn KVEngine) {
    // 5 entries: key=3B ("k00".."k04"), value=10B ("vvvvvvvvv0".."vvvvvvvvv4")
    // = 13B per entry. Budget=30B allows 2 entries (26B) before the 3rd
    // (39B) exceeds the budget.
    let mut ops = Vec::new();
    for i in 0..5 {
        ops.push(put(
            format!("k0{i}").as_bytes(),
            format!("vvvvvvvvv{i}").as_bytes(),
        ));
    }
    e.apply(1, &batch(ops)).into_ready().unwrap();

    // Budget=30: 2 entries (26B) fit, 3rd would be 39B > 30.
    let (items, truncated) = e.scan(b"", b"", b"", 0, 30, false, 0).into_ready().unwrap();
    assert_eq!(items.len(), 2);
    assert!(truncated);
    assert_eq!(items[0].0.as_ref(), b"k00");
    assert_eq!(items[1].0.as_ref(), b"k01");

    // Budget=0 (unlimited): all 5 entries, no truncation.
    let (items, truncated) = e.scan(b"", b"", b"", 0, 0, false, 0).into_ready().unwrap();
    assert_eq!(items.len(), 5);
    assert!(!truncated);

    // Budget smaller than a single entry: always return >= 1 entry.
    // Entry "k00" = 3 + 10 = 13B; budget=5 < 13.
    let (items, truncated) = e.scan(b"", b"", b"", 0, 5, false, 0).into_ready().unwrap();
    assert_eq!(items.len(), 1);
    assert!(truncated); // more entries remain
    assert_eq!(items[0].0.as_ref(), b"k00");
}

/// `keys_only` skips value materialization: the same keys as a full scan are
/// returned, in order, but every value is empty. The byte budget then accounts
/// for key bytes only, so a `keys_only` page fits more entries than a full
/// scan with the same budget.
pub fn scan_keys_only_skips_values(e: &dyn KVEngine) {
    // 5 entries: key=3B ("k00".."k04"), value=10B ("vvvvvvvvv0".."vvvvvvvvv4").
    let mut ops = Vec::new();
    for i in 0..5 {
        ops.push(put(
            format!("k0{i}").as_bytes(),
            format!("vvvvvvvvv{i}").as_bytes(),
        ));
    }
    e.apply(1, &batch(ops)).into_ready().unwrap();

    // keys_only: same keys as a full scan, all values empty.
    let (keys_items, keys_trunc) = e.scan(b"", b"", b"", 0, 0, true, 0).into_ready().unwrap();
    let (full_items, full_trunc) = e.scan(b"", b"", b"", 0, 0, false, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = keys_items.iter().map(|(k, _, _)| k.to_vec()).collect();
    let full_keys: Vec<Vec<u8>> = full_items.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(
        keys,
        vec![
            b"k00".to_vec(),
            b"k01".to_vec(),
            b"k02".to_vec(),
            b"k03".to_vec(),
            b"k04".to_vec()
        ]
    );
    assert_eq!(keys, full_keys, "keys_only returns the same keys as a full scan");
    assert_eq!(keys_trunc, full_trunc, "truncated flags match");
    assert!(
        keys_items.iter().all(|(_, _, v)| v.is_empty()),
        "keys_only values are all empty"
    );
    assert!(
        full_items.iter().all(|(_, _, v)| !v.is_empty()),
        "full scan values are non-empty"
    );

    // keys_only with prefix: only matching keys, values empty.
    let (items, _) = e.scan(b"k0", b"", b"", 0, 0, true, 0).into_ready().unwrap();
    assert_eq!(items.len(), 5);
    assert!(items.iter().all(|(_, _, v)| v.is_empty()));

    // keys_only with start_after: pagination cursor works.
    let (items, trunc) = e.scan(b"", b"k01", b"", 0, 0, true, 0).into_ready().unwrap();
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.to_vec()).collect();
    assert_eq!(keys, vec![b"k02".to_vec(), b"k03".to_vec(), b"k04".to_vec()]);
    assert!(!trunc);

    // keys_only with byte_budget: accounts for key bytes only (3B per key),
    // so budget=7 fits 2 keys (6B) before the 3rd (9B) exceeds — vs a full
    // scan where 13B per entry would fit only 1 entry in 7B.
    let (items, truncated) = e.scan(b"", b"", b"", 0, 7, true, 0).into_ready().unwrap();
    assert_eq!(items.len(), 2);
    assert!(truncated);
    assert_eq!(items[0].0.as_ref(), b"k00");
    assert_eq!(items[1].0.as_ref(), b"k01");
    assert!(items.iter().all(|(_, _, v)| v.is_empty()));
}

pub fn scan_deadline_returns_partial_result(e: &dyn KVEngine) {
    // Populate enough keys that the merge loop's periodic deadline check
    // (every 1024 entries) fires mid-scan.
    for i in 0..3000u64 {
        let key = format!("k{i:05}");
        e.apply(i + 1, &batch(vec![put(key.as_bytes(), b"v")]))
            .into_ready()
            .unwrap();
    }

    // deadline_ms = 0 (no deadline): full scan returns all 3000 entries.
    let (all, all_trunc) = e.scan(b"", b"", b"", 0, 0, false, 0).into_ready().unwrap();
    assert_eq!(all.len(), 3000);
    assert!(!all_trunc);

    // Tight deadline (already expired): the merge loop checks periodically
    // (every 1024 entries) and breaks early with truncated = true. The
    // result is a partial, correctly-ordered prefix (no gaps).
    let deadline = now_ms();
    let (partial, partial_trunc) = e.scan(b"", b"", b"", 0, 0, false, deadline).into_ready().unwrap();
    assert!(partial_trunc);
    assert!(partial.len() < all.len());
    // The partial result is a correctly-ordered prefix of the full scan.
    for (i, (k, _, _)) in partial.iter().enumerate() {
        assert_eq!(k.as_ref(), all[i].0.as_ref(), "partial result must be a prefix");
    }
}

pub fn compare_is_empty_for_identical_state_and_detects_divergence(a: &dyn KVEngine, b: &dyn KVEngine) {
    // Same ops, different apply order — must converge to identical state.
    a.apply(2, &batch(vec![put(b"x", b"2")])).into_ready().unwrap();
    a.apply(1, &batch(vec![put(b"y", b"1")])).into_ready().unwrap();
    b.apply(1, &batch(vec![put(b"y", b"1")])).into_ready().unwrap();
    b.apply(2, &batch(vec![put(b"x", b"2")])).into_ready().unwrap();
    assert!(compare_dyn(a, b).is_empty(), "identical logical state");

    // Divergent resolved-slot for the same value is a difference. Slot 3
    // (not e.g. 9) keeps this contiguous with the slots already applied
    // above -- crowdb-tree only flushes its *contiguous*-applied prefix into
    // the durable tree `iter_all`/`compare` read from, so a slot left behind
    // a gap wouldn't be visible yet even though `get`/`scan` would already
    // see it (an engine-specific difference from `InMemKV`, not something
    // this test is trying to exercise).
    b.apply(3, &batch(vec![put(b"x", b"2")])).into_ready().unwrap();
    let diff = compare_dyn(a, b);
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].key, b"x".to_vec());
}

/// `KVEngine::snapshot_export`/`snapshot_import` round trip
/// (exporting `source`'s state and importing it into a fresh `target` of the
/// same engine kind must reproduce `source`'s exact logical state
/// (`compare` empty) and report the same `at_slot`. `target` must be freshly
/// constructed and never previously `apply`ed to (`snapshot_import`'s
/// documented precondition).
pub fn snapshot_export_import_round_trip(source: &dyn KVEngine, target: &dyn KVEngine) {
    source
        .apply(1, &batch(vec![put(b"a", b"1"), put(b"b", b"2")]))
        .into_ready()
        .unwrap();
    source.apply(2, &batch(vec![del(b"a")])).into_ready().unwrap();
    source
        .apply(3, &batch(vec![put(b"c", b"3")]))
        .into_ready()
        .unwrap();

    let (export_at_slot, stream) = source.snapshot_export().expect("snapshot_export should succeed");
    assert_eq!(
        export_at_slot, 3,
        "at_slot should reflect the highest applied slot"
    );

    let import_at_slot = target
        .snapshot_import(&stream)
        .expect("snapshot_import should succeed");
    assert_eq!(
        import_at_slot, export_at_slot,
        "import must report the same at_slot as export"
    );

    assert!(
        compare_dyn(target, source).is_empty(),
        "imported state must be logically identical to the exporter's"
    );
    assert_eq!(target.get(b"a").into_ready(), None, "tombstoned key stays absent");
    assert_eq!(target.get(b"b").into_ready(), Some((1, b"2".to_vec())));
    assert_eq!(target.get(b"c").into_ready(), Some((3, b"3".to_vec())));
}
