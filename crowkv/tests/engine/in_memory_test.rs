//! `InMemoryEngine` behavior: per-key highest-slot-wins apply, tombstones,
//! apply idempotency, ordered prefix scan with truncation, and cross-engine
//! `compare`.

use crowkv::engine::{Batch, BatchOp, Cell, Engine, InMemoryEngine, Op};

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

fn batch(ops: Vec<BatchOp>) -> Batch {
    Batch { ops }
}

#[test]
fn highest_slot_wins_regardless_of_apply_order() {
    let e = InMemoryEngine::new();
    // Apply the higher slot first, then a lower slot for the same key.
    e.apply(5, &batch(vec![put(b"k", b"v5")]));
    e.apply(3, &batch(vec![put(b"k", b"v3")]));
    assert_eq!(
        e.get(b"k"),
        Some((5, b"v5".to_vec())),
        "lower slot must not overwrite"
    );

    // A strictly higher slot wins.
    e.apply(7, &batch(vec![put(b"k", b"v7")]));
    assert_eq!(e.get(b"k"), Some((7, b"v7".to_vec())));
}

#[test]
fn equal_slot_is_idempotent_noop() {
    let e = InMemoryEngine::new();
    e.apply(4, &batch(vec![put(b"k", b"first")]));
    // Re-applying the same slot must not change the stored value.
    e.apply(4, &batch(vec![put(b"k", b"second")]));
    assert_eq!(e.get(b"k"), Some((4, b"first".to_vec())));
}

#[test]
fn delete_writes_tombstone() {
    let e = InMemoryEngine::new();
    e.apply(1, &batch(vec![put(b"k", b"v")]));
    e.apply(2, &batch(vec![del(b"k")]));
    assert_eq!(e.get(b"k"), None, "tombstoned key is not live");
    assert_eq!(e.live_key_count(), 0);
    // The tombstone is retained internally (visible via iter_all) at its slot.
    let all = e.iter_all();
    assert_eq!(all, vec![(b"k".to_vec(), 2, Cell::Tombstone)]);
}

#[test]
fn intra_batch_last_occurrence_wins() {
    let e = InMemoryEngine::new();
    e.apply(1, &batch(vec![put(b"k", b"a"), del(b"k"), put(b"k", b"final")]));
    assert_eq!(e.get(b"k"), Some((1, b"final".to_vec())));
}

#[test]
fn scan_is_ordered_prefix_filtered_and_truncates() {
    let e = InMemoryEngine::new();
    e.apply(
        1,
        &batch(vec![put(b"a:1", b"1"), put(b"a:2", b"2"), put(b"a:3", b"3")]),
    );
    e.apply(2, &batch(vec![put(b"b:1", b"x")]));
    e.apply(3, &batch(vec![del(b"a:2")]));

    // Unlimited: only live "a:" keys, in order, tombstone excluded.
    let (items, truncated) = e.scan(b"a:", 0);
    let keys: Vec<Vec<u8>> = items.iter().map(|(k, _, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"a:1".to_vec(), b"a:3".to_vec()]);
    assert!(!truncated);

    // Limit smaller than the match count sets truncated.
    let (items, truncated) = e.scan(b"a:", 1);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, b"a:1".to_vec());
    assert!(truncated);

    // Empty prefix scans everything live.
    let (all, _) = e.scan(b"", 0);
    assert_eq!(all.len(), 3); // a:1, a:3, b:1
}

#[test]
fn compare_is_empty_for_identical_state_and_detects_divergence() {
    let a = InMemoryEngine::new();
    let b = InMemoryEngine::new();
    // Same ops, different apply order — must converge to identical state.
    a.apply(2, &batch(vec![put(b"x", b"2")]));
    a.apply(1, &batch(vec![put(b"y", b"1")]));
    b.apply(1, &batch(vec![put(b"y", b"1")]));
    b.apply(2, &batch(vec![put(b"x", b"2")]));
    assert!(a.compare(&b).is_empty(), "identical logical state");

    // Divergent resolved-slot for the same value is a difference.
    b.apply(9, &batch(vec![put(b"x", b"2")]));
    let diff = a.compare(&b);
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].key, b"x".to_vec());
}

#[test]
fn clear_drops_all_state() {
    let e = InMemoryEngine::new();
    e.apply(1, &batch(vec![put(b"k", b"v")]));
    e.clear();
    assert_eq!(e.get(b"k"), None);
    assert_eq!(e.live_key_count(), 0);
    assert!(e.iter_all().is_empty());
}

#[test]
fn batch_decode_matches_put_delete_wire_format() {
    // Mirror PxKvStore::encode_kv_payload: [count][kind][klen][k][vlen][v].
    let mut buf = Vec::new();
    buf.push(2u8); // op_count
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

    let decoded = Batch::decode(&buf);
    assert_eq!(
        decoded,
        batch(vec![put(b"k1", b"v1"), del(b"k2")]),
        "decode must round-trip the wire format"
    );

    // Empty payload decodes to an empty batch (NoOp repair fill).
    assert!(Batch::decode(&[]).ops.is_empty());
}
