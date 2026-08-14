// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowTreeEngine` behavior: same shared `KVEngine` conformance suite as
//! `mem_kv_test.rs` (see `conformance.rs`), proving `CrowTreeEngine` and
//! `InMemKV` satisfy the identical `KVEngine` contract.

use crate::test_util::{compare_dyn, iter_all_dyn};
use crow_kv::kv::{CrowTreeEngine, CrowTreeOptions};

use super::conformance;

fn open() -> CrowTreeEngine {
    CrowTreeEngine::open(&CrowTreeOptions::default()).unwrap()
}

#[test]
fn highest_slot_wins_regardless_of_apply_order() {
    conformance::highest_slot_wins_regardless_of_apply_order(&open());
}

#[test]
fn equal_slot_is_idempotent_noop() {
    conformance::equal_slot_is_idempotent_noop(&open());
}

#[test]
fn delete_writes_tombstone() {
    conformance::delete_writes_tombstone(&open());
}

#[test]
fn intra_batch_last_occurrence_wins() {
    conformance::intra_batch_last_occurrence_wins(&open());
}

#[test]
fn scan_is_ordered_prefix_filtered_and_truncates() {
    conformance::scan_is_ordered_prefix_filtered_and_truncates(&open());
}

#[test]
fn scan_end_key_exclusive_upper_bound() {
    conformance::scan_end_key_exclusive_upper_bound(&open());
}

#[test]
fn scan_byte_budget_stops_and_truncates() {
    conformance::scan_byte_budget_stops_and_truncates(&open());
}

#[test]
fn scan_keys_only_skips_values() {
    conformance::scan_keys_only_skips_values(&open());
}

#[test]
fn scan_deadline_returns_partial_result() {
    conformance::scan_deadline_returns_partial_result(&open());
}

#[test]
fn compare_is_empty_for_identical_state_and_detects_divergence() {
    conformance::compare_is_empty_for_identical_state_and_detects_divergence(&open(), &open());
}

#[test]
fn snapshot_export_import_round_trip() {
    conformance::snapshot_export_import_round_trip(&open(), &open());
}

/// No FFI fault-injection hook exists yet to genuinely trip
/// `Crowtree::io_failed` from Rust (the C++ side's own fault-injection
/// coverage lives in `crow-tree/tests/integration/crash_recovery_test.cpp`),
/// so this only guards the clean-state default -- that `is_healthy` is
/// wired to a real call, not hardcoded `true` on `CrowTreeEngine` the way
/// the trait default is.
#[test]
fn is_healthy_is_true_on_a_freshly_opened_engine() {
    use crow_kv::kv::KVEngine;

    assert!(open().is_healthy());
}

/// Regression guard: an in-memory
/// `CrowTreeEngine` (`opt.path: None`, no page store, no reactor -- see
/// `CrowTreeOptions::default`) has no I/O path *at all*, so `get`/`scan`/
/// `apply` must always resolve `Ready` -- proves the "fast path stays fast"
/// property holds for the durable engine's in-memory mode too, not just
/// `InMemKV`.
#[test]
fn get_scan_apply_always_resolve_ready() {
    use crow_kv::kv::{KVEngine, KVFuture};

    let e = open();
    assert!(matches!(
        e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")])),
        KVFuture::Ready(_)
    ));
    assert!(matches!(e.get(b"k"), KVFuture::Ready(_)));
    assert!(matches!(
        e.scan(b"", b"", b"", 0, 0, false, 0),
        KVFuture::Ready(_)
    ));
}

/// Regression guard : unlike the in-memory case
/// above, a *durable* (file-backed) `CrowTreeEngine`'s `get` genuinely
/// constructs `KVFuture::Pending` for a demand-load miss -- evict the
/// key's leaf (forcing it unloaded) after a snapshot has made it clean,
/// mirroring `async_get_test.cpp`'s `MissAfterEvictionCompletesViaReactor`
/// one layer up. Awaiting that `Pending` future still resolves to the
/// correct value either way (via the reactor on a liburing build, or a
/// synchronous fallback otherwise), proving the `Pending`
/// path is correct, not just that it exists.
#[tokio::test]
async fn get_constructs_pending_for_genuine_demand_load_miss() {
    use crow_kv::kv::{KVEngine, KVFuture};

    let tmp = tempfile::tempdir().expect("tempdir");
    let e = CrowTreeEngine::open(&CrowTreeOptions {
        path: Some(tmp.path().to_string_lossy().into_owned()),
        ..Default::default()
    })
    .expect("open durable engine");

    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")]))
        .into_ready()
        .unwrap();
    e.handle().flush().expect("flush");
    e.handle().snapshot().expect("snapshot");
    let evicted = e.handle().evict_clean_leaves(0);
    assert!(
        evicted > 0,
        "snapshot should have made the leaf clean and evictable"
    );

    // On builds/platforms without the io_uring reactor (e.g. macOS, or Linux
    // without liburing) ct_get_async completes synchronously, so there is no
    // genuine Pending path to observe. Verify the value is still correct and
    // skip the Pending-only assertion in that case.
    if !e.handle().is_reactor_available() {
        assert_eq!(e.get(b"k").into_ready(), Some((1, b"v".to_vec())));
        return;
    }

    match e.get(b"k") {
        KVFuture::Ready(_) => panic!("expected a genuine Pending after evicting the resident leaf"),
        KVFuture::Pending(fut) => {
            assert_eq!(fut.await, Some((1, b"v".to_vec())));
        }
    }
}

/// Same regression guard as
/// [`get_constructs_pending_for_genuine_demand_load_miss`], for `scan`:
/// `CrowTreeEngine::scan` now goes through `AsyncCrowtree::try_scan`
/// instead of the old always-synchronous
/// `Crowtree::scan`, so a scan over an evicted leaf must genuinely
/// construct `KVFuture::Pending` too, not just `get`.
#[tokio::test]
async fn scan_constructs_pending_for_genuine_demand_load_miss() {
    use crow_kv::kv::{KVEngine, KVFuture};

    let tmp = tempfile::tempdir().expect("tempdir");
    let e = CrowTreeEngine::open(&CrowTreeOptions {
        path: Some(tmp.path().to_string_lossy().into_owned()),
        ..Default::default()
    })
    .expect("open durable engine");

    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")]))
        .into_ready()
        .unwrap();
    e.handle().flush().expect("flush");
    e.handle().snapshot().expect("snapshot");
    let evicted = e.handle().evict_clean_leaves(0);
    assert!(
        evicted > 0,
        "snapshot should have made the leaf clean and evictable"
    );

    if !e.handle().is_reactor_available() {
        let (items, truncated) = e.scan(b"", b"", b"", 0, 0, false, 0).into_ready().unwrap();
        let items_vec: Vec<(Vec<u8>, u64, Vec<u8>)> = items
            .into_iter()
            .map(|(k, s, v)| (k.to_vec(), s, v.to_vec()))
            .collect();
        assert_eq!(items_vec, vec![(b"k".to_vec(), 1, b"v".to_vec())]);
        assert!(!truncated);
        return;
    }

    match e.scan(b"", b"", b"", 0, 0, false, 0) {
        KVFuture::Ready(_) => panic!("expected a genuine Pending after evicting the resident leaf"),
        KVFuture::Pending(fut) => {
            let (items, truncated) = fut.await.unwrap();
            let items_vec: Vec<(Vec<u8>, u64, Vec<u8>)> = items
                .into_iter()
                .map(|(k, s, v)| (k.to_vec(), s, v.to_vec()))
                .collect();
            assert_eq!(items_vec, vec![(b"k".to_vec(), 1, b"v".to_vec())]);
            assert!(!truncated);
        }
    }
}

/// `KVEngine::clear`: mirrors `mem_kv_test.rs`'s
/// `clear_drops_all_state` for the crow-tree-backed engine now that
/// `CrowTreeEngine::clear` is wired to `Crowtree::clear` instead of
/// panicking.
#[test]
fn clear_drops_all_state() {
    use crow_kv::kv::KVEngine;

    let e = open();
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")]))
        .into_ready()
        .unwrap();
    e.clear();
    assert_eq!(e.get(b"k").into_ready(), None);
    assert_eq!(e.live_key_count(), 0);
    assert!(iter_all_dyn(&e).is_empty());
}

/// `clear` must reset per-slot bookkeeping (`received_slots_`/
/// `max_seen_slot_`), not just the key/value data -- otherwise re-applying
/// a slot number that was already seen before the wipe (e.g. slot 1, on a
/// freshly-reset replica about to re-learn its whole log from scratch)
/// would be silently ignored by `apply`'s idempotency check, even though
/// this is logically a brand-new tree.
#[test]
fn apply_after_clear_accepts_the_same_slot_number_again() {
    use crow_kv::kv::KVEngine;

    let e = open();
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v1")]))
        .into_ready()
        .unwrap();
    e.clear();
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v2")]))
        .into_ready()
        .unwrap();
    assert_eq!(e.get(b"k").into_ready(), Some((1, b"v2".to_vec())));
}

/// Crash-safety-adjacent check for a *durable* (file-backed) engine:
/// `clear` alone is not durable (matching `snapshot_import`'s own
/// contract), but once followed by an explicit `persist_snapshot`, the
/// wipe must survive a close + reopen -- proving `clear` actually
/// updates the on-disk commit anchor via the normal snapshot path, not
/// just the in-memory tree.
#[tokio::test]
async fn clear_then_persist_survives_reopen() {
    use crow_kv::kv::KVEngine;

    let tmp = tempfile::tempdir().expect("tempdir");
    let opt = CrowTreeOptions {
        path: Some(tmp.path().to_string_lossy().into_owned()),
        ..Default::default()
    };

    let e = CrowTreeEngine::open(&opt).expect("open durable engine");
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")]))
        .into_ready()
        .unwrap();
    assert_eq!(e.persist_snapshot(), 1, "sanity: pre-clear state persisted");
    e.clear();
    assert_eq!(
        e.persist_snapshot(),
        0,
        "an empty tree has nothing applied to report as its resume floor"
    );
    drop(e);

    let reopened = CrowTreeEngine::open(&opt).expect("reopen durable engine");
    // `.await`, not `.into_ready`: a just-reopened durable tree's root is
    // installed as an *unloaded* descriptor (lazy recovery -- see
    // `Crowtree::open`'s doc comment in persist.cpp), so this first read
    // may genuinely demand-load it, constructing `KVFuture::Pending` --
    // exactly like `get_constructs_pending_for_genuine_demand_load_miss`,
    // just incidentally rather than via a forced eviction.
    assert_eq!(
        reopened.get(b"k").await,
        None,
        "clear() + persist must not resurrect the pre-clear key after reopen"
    );
    assert_eq!(reopened.live_key_count(), 0);
    assert_eq!(reopened.resume_from_slot(), 0);
}

/// Cross-engine parity (design's strongest correctness gate): apply the same
/// op stream to `InMemKV` and `CrowTreeEngine`, `compare` must be empty.
#[test]
fn parity_with_in_mem_kv_after_identical_op_stream() {
    use crate::mem_kv::InMemKV;
    use crow_kv::kv::KVEngine;

    let mem = InMemKV::new();
    let ct = open();
    let mut slot = 0u64;
    for round in 0..20u32 {
        slot += 1;
        let key = format!("key{:03}", round % 7).into_bytes();
        let b = if round % 5 == 4 {
            conformance::batch(vec![conformance::del(&key)])
        } else {
            let value = format!("v{round}").into_bytes();
            conformance::batch(vec![conformance::put(&key, &value)])
        };
        mem.apply(slot, &b).into_ready().unwrap();
        ct.apply(slot, &b).into_ready().unwrap();
        assert!(compare_dyn(&mem, &ct).is_empty(), "diverged after round {round}");
    }
}

/// `get_bytes` fast path: returns `Bytes` matching the written value
/// without an intermediate `Vec<u8>` allocation (uses `PinnedValue`
/// internally). Verifies the zero-copy read path produces correct results
/// for both found and not-found cases.
#[tokio::test]
async fn get_bytes_fast_path_returns_correct_value() {
    use crow_kv::kv::KVEngine;

    let e = open();
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"hello")]))
        .into_ready()
        .unwrap();

    let result = e.get_bytes(b"k").await;
    assert_eq!(result, Some((1, bytes::Bytes::from_static(b"hello"))));

    let missing = e.get_bytes(b"missing").await;
    assert_eq!(missing, None);
}

/// `snapshot_view` returns a frozen, key-sorted vector including
/// tombstones, pinned at `last_applied_slot` after a flush.
#[test]
fn snapshot_view_returns_pinned_entries() {
    use crow_kv::kv::KVEngine;

    let e = open();
    e.apply(1, &conformance::batch(vec![conformance::put(b"a", b"v1")]))
        .into_ready()
        .unwrap();
    e.apply(2, &conformance::batch(vec![conformance::put(b"b", b"v2")]))
        .into_ready()
        .unwrap();
    e.apply(3, &conformance::batch(vec![conformance::del(b"a")]))
        .into_ready()
        .unwrap();

    let (at_slot, entries) = e.snapshot_view().expect("snapshot_view");
    assert_eq!(at_slot, 3);
    // Key-sorted: a (tombstoned), b (live).
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].key, b"a");
    assert!(entries[0].tombstone);
    assert_eq!(entries[1].key, b"b");
    assert!(!entries[1].tombstone);
    assert_eq!(entries[1].value, b"v2");
}

/// `snapshot_view` is point-in-time-consistent: writes applied after
/// the snapshot do not appear in the frozen view.
#[test]
fn snapshot_view_is_point_in_time_consistent() {
    use crow_kv::kv::KVEngine;

    let e = open();
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v1")]))
        .into_ready()
        .unwrap();

    let (at_slot, entries) = e.snapshot_view().expect("snapshot_view");
    assert_eq!(at_slot, 1);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"k");
    assert_eq!(entries[0].value, b"v1");

    // Apply more writes after the snapshot.
    e.apply(2, &conformance::batch(vec![conformance::put(b"k", b"v2")]))
        .into_ready()
        .unwrap();
    e.apply(3, &conformance::batch(vec![conformance::put(b"new", b"v3")]))
        .into_ready()
        .unwrap();

    // The frozen view is unchanged.
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value, b"v1");
    // A new snapshot reflects the latest state.
    let (at_slot2, entries2) = e.snapshot_view().expect("snapshot_view 2");
    assert_eq!(at_slot2, 3);
    assert_eq!(entries2.len(), 2);
    assert_eq!(entries2[0].key, b"k");
    assert_eq!(entries2[0].value, b"v2");
    assert_eq!(entries2[1].key, b"new");
    assert_eq!(entries2[1].value, b"v3");
}

/// `get_bytes` with a large value: verifies the pinned-value path
/// correctly handles values that span the full frame.
#[tokio::test]
async fn get_bytes_fast_path_large_value() {
    use crow_kv::kv::KVEngine;

    let e = open();
    let large: Vec<u8> = (0..4096u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    e.apply(1, &conformance::batch(vec![conformance::put(b"k", &large)]))
        .into_ready()
        .unwrap();

    let result = e.get_bytes(b"k").await;
    assert!(result.is_some());
    let (slot, value) = result.unwrap();
    assert_eq!(slot, 1);
    assert_eq!(value.as_ref(), large.as_slice());
}
