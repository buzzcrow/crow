//! `CrowtreeEngine` behavior: same shared `KVEngine` conformance suite as
//! `mem_kv_test.rs` (see `conformance.rs`), proving `CrowtreeEngine` and
//! `InMemKV` satisfy the identical `KVEngine` contract.

use crowkv::kv::{CrowtreeEngine, CrowtreeOptions};

use super::conformance;

fn open() -> CrowtreeEngine {
    CrowtreeEngine::open(&CrowtreeOptions::default()).unwrap()
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
fn compare_is_empty_for_identical_state_and_detects_divergence() {
    conformance::compare_is_empty_for_identical_state_and_detects_divergence(&open(), &open());
}

#[test]
fn snapshot_export_import_round_trip() {
    conformance::snapshot_export_import_round_trip(&open(), &open());
}

/// Regression guard (`design-crowkv-async-kvengine.md` §6): an in-memory
/// `CrowtreeEngine` (`opt.path: None`, no page store, no reactor -- see
/// `CrowtreeOptions::default`) has no I/O path *at all*, so `get`/`scan`/
/// `apply` must always resolve `Ready` -- proves the "fast path stays fast"
/// property holds for the durable engine's in-memory mode too, not just
/// `InMemKV`.
#[test]
fn get_scan_apply_always_resolve_ready() {
    use crowkv::kv::{KVEngine, KVFuture};

    let e = open();
    assert!(matches!(
        e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")])),
        KVFuture::Ready(_)
    ));
    assert!(matches!(e.get(b"k"), KVFuture::Ready(_)));
    assert!(matches!(e.scan(b"", 0), KVFuture::Ready(_)));
}

/// Regression guard (plan-tree.md #11 Phase 6): unlike the in-memory case
/// above, a *durable* (file-backed) `CrowtreeEngine`'s `get` genuinely
/// constructs `KVFuture::Pending` for a demand-load miss -- evict the
/// key's leaf (forcing it unloaded) after a snapshot has made it clean,
/// mirroring `async_get_test.cpp`'s `MissAfterEvictionCompletesViaReactor`
/// one layer up. Awaiting that `Pending` future still resolves to the
/// correct value either way (via the reactor on a liburing build, or a
/// synchronous fallback otherwise -- design §6.3), proving the `Pending`
/// path is correct, not just that it exists.
#[tokio::test]
async fn get_constructs_pending_for_genuine_demand_load_miss() {
    use crowkv::kv::{KVEngine, KVFuture};

    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let e = CrowtreeEngine::open(&CrowtreeOptions {
        path: Some(tmp.path().to_string_lossy().into_owned()),
        ..Default::default()
    })
    .expect("open durable engine");

    e.apply(1, &conformance::batch(vec![conformance::put(b"k", b"v")]))
        .into_ready();
    e.handle().flush().expect("flush");
    e.handle().snapshot().expect("snapshot");
    let evicted = e.handle().evict_clean_leaves(0);
    assert!(
        evicted > 0,
        "snapshot should have made the leaf clean and evictable"
    );

    match e.get(b"k") {
        KVFuture::Ready(_) => panic!("expected a genuine Pending after evicting the resident leaf"),
        KVFuture::Pending(fut) => {
            assert_eq!(fut.await, Some((1, b"v".to_vec())));
        }
    }
}

/// Cross-engine parity (design's strongest correctness gate): apply the same
/// op stream to `InMemKV` and `CrowtreeEngine`, `compare()` must be empty.
#[test]
fn parity_with_in_mem_kv_after_identical_op_stream() {
    use crowkv::kv::{InMemKV, KVEngine};

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
        mem.apply(slot, &b).into_ready();
        ct.apply(slot, &b).into_ready();
        assert!(mem.compare(&ct).is_empty(), "diverged after round {round}");
    }
}
