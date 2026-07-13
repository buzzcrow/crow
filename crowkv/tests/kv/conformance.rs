//! Shared `KVEngine` conformance suite, parametrized over any implementation
//! (`InMemKV`, `CrowtreeEngine`) so both get identical behavioral coverage:
//! per-key highest-slot-wins apply, tombstones, apply idempotency, ordered
//! prefix scan with truncation, intra-batch dedup, and cross-engine compare.
//!
//! Each function is a reusable assertion body; callers wrap them in their own
//! `#[test]` functions with a freshly constructed engine.

use crowkv::kv::{Batch, BatchOp, Cell, KVEngine, Op};

pub fn put(key: &[u8], value: &[u8]) -> BatchOp {
    BatchOp {
        key: key.to_vec(),
        op: Op::Put(value.to_vec()),
    }
}

pub fn del(key: &[u8]) -> BatchOp {
    BatchOp {
        key: key.to_vec(),
        op: Op::Delete,
    }
}

pub fn batch(ops: Vec<BatchOp>) -> Batch {
    Batch { ops }
}

pub fn highest_slot_wins_regardless_of_apply_order(e: &dyn KVEngine) {
    // Apply the higher slot first, then a lower slot for the same key.
    e.apply(5, &batch(vec![put(b"k", b"v5")])).into_ready();
    e.apply(3, &batch(vec![put(b"k", b"v3")])).into_ready();
    assert_eq!(
        e.get(b"k").into_ready(),
        Some((5, b"v5".to_vec())),
        "lower slot must not overwrite"
    );

    // A strictly higher slot wins.
    e.apply(7, &batch(vec![put(b"k", b"v7")])).into_ready();
    assert_eq!(e.get(b"k").into_ready(), Some((7, b"v7".to_vec())));
}

pub fn equal_slot_is_idempotent_noop(e: &dyn KVEngine) {
    e.apply(4, &batch(vec![put(b"k", b"first")])).into_ready();
    // Re-applying the same slot must not change the stored value.
    e.apply(4, &batch(vec![put(b"k", b"second")])).into_ready();
    assert_eq!(e.get(b"k").into_ready(), Some((4, b"first".to_vec())));
}

pub fn delete_writes_tombstone(e: &dyn KVEngine) {
    e.apply(1, &batch(vec![put(b"k", b"v")])).into_ready();
    e.apply(2, &batch(vec![del(b"k")])).into_ready();
    assert_eq!(e.get(b"k").into_ready(), None, "tombstoned key is not live");
    assert_eq!(e.live_key_count(), 0);
    // The tombstone is retained internally (visible via iter_all) at its slot.
    let all = e.iter_all();
    assert_eq!(all, vec![(b"k".to_vec(), 2, Cell::Tombstone)]);
}

pub fn intra_batch_last_occurrence_wins(e: &dyn KVEngine) {
    e.apply(1, &batch(vec![put(b"k", b"a"), del(b"k"), put(b"k", b"final")]))
        .into_ready();
    assert_eq!(e.get(b"k").into_ready(), Some((1, b"final".to_vec())));
}

pub fn scan_is_ordered_prefix_filtered_and_truncates(e: &dyn KVEngine) {
    e.apply(
        1,
        &batch(vec![put(b"a:1", b"1"), put(b"a:2", b"2"), put(b"a:3", b"3")]),
    )
    .into_ready();
    e.apply(2, &batch(vec![put(b"b:1", b"x")])).into_ready();
    e.apply(3, &batch(vec![del(b"a:2")])).into_ready();

    // Unlimited: only live "a:" keys, in order, tombstone excluded.
    let (items, truncated) = e.scan(b"a:", 0).into_ready();
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"a:1".to_vec(), b"a:3".to_vec()]);
    assert!(!truncated);

    // Limit smaller than the match count sets truncated.
    let (items, truncated) = e.scan(b"a:", 1).into_ready();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, b"a:1".to_vec());
    assert!(truncated);

    // Empty prefix scans everything live.
    let (all, _) = e.scan(b"", 0).into_ready();
    assert_eq!(all.len(), 3); // a:1, a:3, b:1
}

pub fn compare_is_empty_for_identical_state_and_detects_divergence(a: &dyn KVEngine, b: &dyn KVEngine) {
    // Same ops, different apply order — must converge to identical state.
    a.apply(2, &batch(vec![put(b"x", b"2")])).into_ready();
    a.apply(1, &batch(vec![put(b"y", b"1")])).into_ready();
    b.apply(1, &batch(vec![put(b"y", b"1")])).into_ready();
    b.apply(2, &batch(vec![put(b"x", b"2")])).into_ready();
    assert!(a.compare(b).is_empty(), "identical logical state");

    // Divergent resolved-slot for the same value is a difference. Slot 3
    // (not e.g. 9) keeps this contiguous with the slots already applied
    // above -- crowtree only flushes its *contiguous*-applied prefix into
    // the durable tree `iter_all`/`compare` read from, so a slot left behind
    // a gap wouldn't be visible yet even though `get`/`scan` would already
    // see it (an engine-specific difference from `InMemKV`, not something
    // this test is trying to exercise).
    b.apply(3, &batch(vec![put(b"x", b"2")])).into_ready();
    let diff = a.compare(b);
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].key, b"x".to_vec());
}

/// `KVEngine::snapshot_export`/`snapshot_import` round trip
/// (`design-crowtree-snapshot-gc.md` §2/§6, plan-tree #20 new-member join):
/// exporting `source`'s state and importing it into a fresh `target` of the
/// same engine kind must reproduce `source`'s exact logical state
/// (`compare` empty) and report the same `at_slot`. `target` must be freshly
/// constructed and never previously `apply`ed to (`snapshot_import`'s
/// documented precondition).
pub fn snapshot_export_import_round_trip(source: &dyn KVEngine, target: &dyn KVEngine) {
    source
        .apply(1, &batch(vec![put(b"a", b"1"), put(b"b", b"2")]))
        .into_ready();
    source.apply(2, &batch(vec![del(b"a")])).into_ready();
    source.apply(3, &batch(vec![put(b"c", b"3")])).into_ready();

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
        target.compare(source).is_empty(),
        "imported state must be logically identical to the exporter's"
    );
    assert_eq!(target.get(b"a").into_ready(), None, "tombstoned key stays absent");
    assert_eq!(target.get(b"b").into_ready(), Some((1, b"2".to_vec())));
    assert_eq!(target.get(b"c").into_ready(), Some((3, b"3".to_vec())));
}
