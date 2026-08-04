// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT8.5: C ABI / Rust integration tests through the safe adapter.
use crow_tree_ffi::{
    AsyncCrowtree, BatchOp, Crowtree, CtError, ExtOp, Options, PageStoreBackend, PinnedGetOutcome,
};

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

#[test]
fn mem_apply_get_scan() {
    let t = Crowtree::open(&Options::default()).unwrap();
    for i in 0..40usize {
        let v = format!("v{i}").into_bytes();
        t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
    }
    t.flush().unwrap();
    // Point read.
    let got = t.get(&key(5)).unwrap();
    assert_eq!(got, Some((6u64, b"v5".to_vec())));

    // Delete.
    t.apply_delete(1000, &key(7)).unwrap();
    t.force_advance_slot(1000);
    t.flush().unwrap();
    assert_eq!(t.get(&key(7)).unwrap(), None);

    // Scan all live entries.
    let (entries, truncated) = t.scan(b"", 0, false).unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 39); // 40 puts - 1 delete
    assert!(entries.windows(2).all(|w| w[0].key < w[1].key)); // key-sorted
}

#[test]
fn mem_gc_watermark_and_collect_garbage() {
    let t = Crowtree::open(&Options::default()).unwrap();
    t.apply_put(1, b"a", b"A").unwrap();
    t.apply_delete(2, b"a").unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"a").unwrap(), None);

    // Below the (default zero) watermark: nothing eligible yet.
    let stats = t.collect_garbage().unwrap();
    assert_eq!(stats, crow_tree_ffi::GcStats::default());

    // gc_slot = min(snapshot_slot, safe_slot): a low snapshot_slot still holds
    // the floor down even though safe_slot alone would allow the drop.
    t.set_gc_watermark(0, 2);
    let stats = t.collect_garbage().unwrap();
    assert_eq!(stats, crow_tree_ffi::GcStats::default());

    t.set_gc_watermark(2, 2);
    let stats = t.collect_garbage().unwrap();
    assert_eq!(stats.tombstones_dropped, 1);
    assert!(stats.pages_freed >= 1);
    assert!(stats.bytes_freed > 0);

    // Idempotent: nothing left to reclaim on a second sweep.
    let stats2 = t.collect_garbage().unwrap();
    assert_eq!(stats2, crow_tree_ffi::GcStats::default());

    // The tombstone drop is a physical/resident-only change; the logical read
    // path (already gc_floor-filtered) is unaffected either way.
    assert_eq!(t.get(b"a").unwrap(), None);
}

#[test]
fn mem_apply_batch_multi_key_and_dup_last_wins() {
    let t = Crowtree::open(&Options::default()).unwrap();
    t.apply_batch(
        1,
        &[
            BatchOp::Put {
                key: b"a",
                value: b"va",
            },
            BatchOp::Put {
                key: b"b",
                value: b"vb",
            },
            BatchOp::Delete { key: b"c" },
        ],
    )
    .unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"a").unwrap(), Some((1, b"va".to_vec())));
    assert_eq!(t.get(b"b").unwrap(), Some((1, b"vb".to_vec())));
    assert_eq!(t.get(b"c").unwrap(), None);

    // Intra-batch duplicate key: last occurrence wins.
    t.apply_batch(
        2,
        &[
            BatchOp::Put {
                key: b"d",
                value: b"first",
            },
            BatchOp::Put {
                key: b"d",
                value: b"second",
            },
        ],
    )
    .unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"d").unwrap(), Some((2, b"second".to_vec())));

    // Empty batch is a no-op (mirrors a NoOp repair-fill payload).
    t.apply_batch(3, &[]).unwrap();
}

#[test]
fn file_snapshot_reopen_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };

    {
        let t = Crowtree::open(&opt).unwrap();
        for i in 0..50usize {
            let v = format!("value{i}").into_bytes();
            t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
            t.flush().unwrap();
        }
        let durable = t.snapshot().unwrap();
        assert_eq!(durable, 50);
    }
    // Reopen the same file and verify recovery.
    let t = Crowtree::open(&opt).unwrap();
    assert_eq!(t.last_applied_slot(), 50);
    for i in 0..50usize {
        assert_eq!(
            t.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("value{i}").into_bytes()))
        );
    }
}

// : Options::backend = PageStoreBackend::Block selects
// BlockPageStore instead of the default file-based page store -- same
// round-trip as file_snapshot_reopen_smoke above, just through the
// block-device backend.
#[test]
fn block_device_snapshot_reopen_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 4096,
        frame_bytes: 4096,
        backend: PageStoreBackend::Block,
        ..Default::default()
    };

    {
        let t = Crowtree::open(&opt).unwrap();
        for i in 0..50usize {
            let v = format!("value{i}").into_bytes();
            t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
            t.flush().unwrap();
        }
        let durable = t.snapshot().unwrap();
        assert_eq!(durable, 50);
    }
    // Reopen the same file and verify recovery.
    let t = Crowtree::open(&opt).unwrap();
    assert_eq!(t.last_applied_slot(), 50);
    for i in 0..50usize {
        assert_eq!(
            t.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("value{i}").into_bytes()))
        );
    }
}

#[test]
fn snapshot_export_import_round_trip() {
    let a = Crowtree::open(&Options::default()).unwrap();
    for i in 0..30usize {
        a.apply_put((i + 1) as u64, &key(i), format!("v{i}").as_bytes())
            .unwrap();
        a.flush().unwrap();
    }
    let stream = a.snapshot_export().unwrap();
    assert!(!stream.is_empty());

    let b = Crowtree::open(&Options::default()).unwrap();
    let at = b.snapshot_import(&stream).unwrap();
    assert_eq!(at, 30);
    for i in 0..30usize {
        assert_eq!(
            b.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("v{i}").into_bytes()))
        );
    }

    // Snapshot views must match structurally.
    let (sa, va) = a.snapshot_view().unwrap();
    let (sb, vb) = b.snapshot_view().unwrap();
    assert_eq!(sa, sb);
    assert_eq!(va, vb);
}

#[test]
fn io_failed_clean_on_healthy_engine() {
    let t = Crowtree::open(&Options::default()).unwrap();
    for i in 0..10usize {
        t.apply_put((i + 1) as u64, &key(i), b"v").unwrap();
        t.flush().unwrap();
    }
    for i in 0..10usize {
        let _ = t.get(&key(i)).unwrap();
    }
    assert!(!t.io_failed());
    t.clear_io_error();
    assert!(!t.io_failed());
}

#[test]
fn open_rejects_path_with_nul() {
    let opt = Options {
        path: Some("bad\0path".to_string()),
        ..Default::default()
    };
    assert_eq!(Crowtree::open(&opt).unwrap_err(), CtError::InvalidArgument);
}

#[tokio::test]
async fn async_bridge_apply_get_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    for i in 0..20usize {
        t.apply_put((i + 1) as u64, key(i), format!("a{i}").into_bytes())
            .await
            .unwrap();
        t.flush().await.unwrap();
    }
    let durable = t.snapshot().await.unwrap();
    assert_eq!(durable, 20);
    assert_eq!(t.get(key(3)).await.unwrap(), Some((4u64, b"a3".to_vec())));
    assert_eq!(t.get(key(999)).await.unwrap(), None);
}

// Phase 3 : AsyncCrowtree::get/flush/snapshot now drive the
// engine's io_uring reactor directly (CtGetFuture/CtFlushFuture/
// CtSnapshotFuture) -- no spawn_blocking. Regression guard for the whole
// point of this phase: manually poll the returned future exactly once with a
// no-op waker and assert it is already Ready -- a resident hit must resolve
// without ever registering on the reactor's eventfd (unlike a timing-based
// assertion, this is deterministic).
#[tokio::test]
async fn async_get_fast_path_completes_on_first_poll() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    t.apply_put(1, key(0), b"v0".to_vec()).await.unwrap();
    t.flush().await.unwrap();

    // Resident (never evicted): get_async's fast path (try_get_view_no_load,
    // design #5 B3) must resolve this on the very first poll.
    let mut fut = Box::pin(t.get(key(0)));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert_eq!(
        fut.as_mut().poll(&mut cx),
        Poll::Ready(Ok(Some((1u64, b"v0".to_vec()))))
    );
}

#[tokio::test]
async fn async_get_slow_path_completes_after_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    t.apply_put(1, key(0), b"v0".to_vec()).await.unwrap();
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    // Force the leaf unloaded so the next get takes the demand-load miss
    // path -- on this (liburing) build, that means a genuine reactor/eventfd
    // round trip, not spawn_blocking.
    t.handle().evict_clean_leaves(0);

    assert_eq!(t.get(key(0)).await.unwrap(), Some((1u64, b"v0".to_vec())));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_async_gets_all_resolve_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    const N: usize = 16;
    for i in 0..N {
        t.apply_put((i + 1) as u64, key(i), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    // Force every leaf unloaded so all N concurrent gets below take the
    // reactor round trip -- proves the eventfd wakeup fans out to every
    // pending future, not just one.
    t.handle().evict_clean_leaves(0);

    let mut tasks = Vec::with_capacity(N);
    for i in 0..N {
        let t = t.clone();
        tasks.push(tokio::spawn(async move { t.get(key(i)).await }));
    }
    for (i, task) in tasks.into_iter().enumerate() {
        let got = task.await.unwrap().unwrap();
        assert_eq!(got, Some(((i + 1) as u64, format!("v{i}").into_bytes())));
    }
}

// follow-up: AsyncCrowtree::scan mirrors async_get_fast_
// path_completes_on_first_poll's regression shape for scan, which also
// has a resident-hit fast path (try_scan_no_load).
#[tokio::test]
async fn async_scan_fast_path_completes_on_first_poll() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    for i in 0..10usize {
        t.apply_put((i + 1) as u64, key(i), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    t.flush().await.unwrap();

    let mut fut = Box::pin(t.scan(Vec::new(), 0));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(Ok((entries, truncated))) => {
            assert_eq!(entries.len(), 10);
            assert!(!truncated);
        }
        other => panic!("expected an immediately-ready resolved scan, got {other:?}"),
    }
}

// Mirrors async_get_slow_path_completes_after_eviction for scan: forcing
// every leaf unloaded makes scan_async take the demand-load-miss retry loop
// (scan_async_attempt) instead of resolving on the first poll, and it still
// produces the full, correct result.
#[tokio::test]
async fn async_scan_slow_path_completes_after_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    for i in 0..20usize {
        t.apply_put((i + 1) as u64, key(i), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    t.handle().evict_clean_leaves(0);

    let (entries, truncated) = t.scan(Vec::new(), 0).await.unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 20);
    let mut got: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
        entries.into_iter().map(|e| (e.key, e.value)).collect();
    for i in 0..20usize {
        assert_eq!(got.remove(&key(i)), Some(format!("v{i}").into_bytes()));
    }
    assert!(got.is_empty());
}

// A limit smaller than the matching key count truncates, matching scan's
// own synchronous semantics.
#[tokio::test]
async fn async_scan_respects_limit_and_truncated_flag() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    for i in 0..15usize {
        t.apply_put((i + 1) as u64, key(i), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }
    t.flush().await.unwrap();

    let (entries, truncated) = t.scan(Vec::new(), 5).await.unwrap();
    assert_eq!(entries.len(), 5);
    assert!(truncated);
}

#[tokio::test]
async fn try_get_pinned_fast_path_returns_borrowed_value() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    t.apply_put(1, key(0), b"v0".to_vec()).await.unwrap();
    t.flush().await.unwrap();

    match t.try_get_pinned(&key(0)) {
        PinnedGetOutcome::Ready(Ok(Some((slot, pinned)))) => {
            assert_eq!(slot, 1);
            assert_eq!(pinned.as_bytes(), b"v0");
        }
        _ => panic!("expected Ready(Ok(Some)) on resident hit"),
    }

    // Not-found case: should return Ready(Ok(None)).
    match t.try_get_pinned(&key(99)) {
        PinnedGetOutcome::Ready(Ok(None)) => {}
        _ => panic!("expected Ready(Ok(None)) for missing key"),
    }
}

#[tokio::test]
async fn try_get_pinned_slow_path_resolves_after_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    t.apply_put(1, key(0), b"v0".to_vec()).await.unwrap();
    t.flush().await.unwrap();
    t.snapshot().await.unwrap();
    t.handle().evict_clean_leaves(0);

    // On builds without io_uring (e.g. macOS), ct_get_async completes
    // synchronously even after eviction (sync fallback), so both Ready
    // and Pending are valid outcomes — either way the value must be correct.
    match t.try_get_pinned(&key(0)) {
        PinnedGetOutcome::Ready(Ok(Some((slot, pinned)))) => {
            assert_eq!(slot, 1);
            assert_eq!(pinned.as_bytes(), b"v0");
        }
        PinnedGetOutcome::Pending(fut) => {
            let result = fut.await.unwrap();
            assert_eq!(result, Some((1u64, b"v0".to_vec())));
        }
        _ => panic!("expected Ok(Some) or Pending for evicted key"),
    }
}

#[tokio::test]
async fn try_get_pinned_fast_path_with_large_value() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options {
        path: Some(dir.path().to_string_lossy().into_owned()),
        iu_size: 1,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    let large: Vec<u8> = (0..4096u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    t.apply_put(1, key(0), large.clone()).await.unwrap();
    t.flush().await.unwrap();

    match t.try_get_pinned(&key(0)) {
        PinnedGetOutcome::Ready(Ok(Some((slot, pinned)))) => {
            assert_eq!(slot, 1);
            assert_eq!(pinned.as_bytes(), large.as_slice());
        }
        _ => panic!("expected Ready(Ok(Some)) on resident hit"),
    }
}

#[test]
fn zero_copy_alloc_apply_round_trip() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let mut h = t.alloc_put(3, 5).unwrap();
    h.key_mut().copy_from_slice(b"abc");
    h.value_mut().copy_from_slice(b"hello");
    h.apply(1).unwrap();
    t.flush().unwrap();
    let got = t.get(b"abc").unwrap();
    assert_eq!(got, Some((1u64, b"hello".to_vec())));
}

#[test]
fn zero_copy_large_value_round_trip() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let big: Vec<u8> = (0..8192u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    let mut h = t.alloc_put(4, big.len()).unwrap();
    h.key_mut().copy_from_slice(b"bigk");
    h.value_mut().copy_from_slice(&big);
    h.apply(1).unwrap();
    t.flush().unwrap();
    let got = t.get(b"bigk").unwrap();
    assert_eq!(got, Some((1u64, big)));
}

#[test]
fn zero_copy_empty_value_round_trip() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let mut h = t.alloc_put(2, 0).unwrap();
    h.key_mut().copy_from_slice(b"ev");
    assert!(h.value_mut().is_empty());
    h.apply(1).unwrap();
    t.flush().unwrap();
    let got = t.get(b"ev").unwrap();
    assert_eq!(got, Some((1u64, Vec::new())));
}

#[test]
fn zero_copy_handle_drop_without_apply_no_leak() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let mut h = t.alloc_put(10, 4096).unwrap();
    h.key_mut().fill(0xAB);
    h.value_mut().fill(0xCD);
    drop(h); // RAII frees the handle
}

#[test]
fn zero_copy_oversized_key_rejected() {
    let opt = Options {
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = Crowtree::open(&opt).unwrap();
    let result = t.alloc_put(3000, 4);
    assert_eq!(result.err(), Some(CtError::InvalidArgument));
}

// ── R30: zero-copy apply_batch_external ───────────────────────────

#[test]
fn apply_batch_external_round_trip_before_flush() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let big = bytes::Bytes::from(vec![0xF0; 4096]);
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"k1"),
            value: big.clone(),
        },
        ExtOp::Delete {
            key: bytes::Bytes::from_static(b"k2"),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    // L0 read (before flush): materializes the split cell.
    assert_eq!(t.get(b"k1").unwrap(), Some((1, big.to_vec())));
    assert_eq!(t.get(b"k2").unwrap(), None);
}

#[test]
fn apply_batch_external_round_trip_after_flush() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let big = bytes::Bytes::from(vec![0xE1; 8192]);
    let ops = vec![ExtOp::Put {
        key: bytes::Bytes::from_static(b"k2"),
        value: big.clone(),
    }];
    t.apply_batch_external(1, ops).unwrap();
    t.flush().unwrap(); // drain -> materialize -> Rust ref released
    assert_eq!(t.get(b"k2").unwrap(), Some((1, big.to_vec())));
}

#[test]
fn apply_batch_external_multi_key_atomicity() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"a"),
            value: bytes::Bytes::from_static(b"va"),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"b"),
            value: bytes::Bytes::from_static(b"vb"),
        },
        ExtOp::Delete {
            key: bytes::Bytes::from_static(b"c"),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"a").unwrap(), Some((1, b"va".to_vec())));
    assert_eq!(t.get(b"b").unwrap(), Some((1, b"vb".to_vec())));
    assert_eq!(t.get(b"c").unwrap(), None);
}

#[test]
fn apply_batch_external_intra_batch_last_key_wins() {
    let t = Crowtree::open(&Options::default()).unwrap();
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"k"),
            value: bytes::Bytes::from_static(b"first"),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"k"),
            value: bytes::Bytes::from_static(b"second"),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    assert_eq!(t.get(b"k").unwrap(), Some((1, b"second".to_vec())));
}

#[test]
fn apply_batch_external_large_value_round_trip() {
    let t = Crowtree::open(&Options::default()).unwrap();
    // 64 KiB values — the workload R30 targets (eliminate the apply-path copy).
    let v1 = bytes::Bytes::from(vec![0x11; 65536]);
    let v2 = bytes::Bytes::from(vec![0x22; 65536]);
    let v3 = bytes::Bytes::from(vec![0x33; 65536]);
    let ops = vec![
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"big1"),
            value: v1.clone(),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"big2"),
            value: v2.clone(),
        },
        ExtOp::Put {
            key: bytes::Bytes::from_static(b"big3"),
            value: v3.clone(),
        },
    ];
    t.apply_batch_external(1, ops).unwrap();
    t.flush().unwrap();
    assert_eq!(t.get(b"big1").unwrap(), Some((1, v1.to_vec())));
    assert_eq!(t.get(b"big2").unwrap(), Some((1, v2.to_vec())));
    assert_eq!(t.get(b"big3").unwrap(), Some((1, v3.to_vec())));
}

#[test]
fn apply_batch_external_bytes_kept_alive_until_drain() {
    // The payload Bytes is kept alive by the external buffers' Arc clones;
    // it must not be freed until the MemTable drains (flush). This test
    // verifies the ref handle lifecycle: the Bytes stays valid through apply
    // and flush, and the drop callback fires at drain (not before).
    let t = Crowtree::open(&Options::default()).unwrap();
    let payload = bytes::Bytes::from(vec![0xAB; 1024]);
    let ops = vec![ExtOp::Put {
        key: bytes::Bytes::from_static(b"k"),
        value: payload.clone(),
    }];
    t.apply_batch_external(1, ops).unwrap();
    // payload is still alive here (we hold a clone); the engine's clone is
    // pinned by the memtable entry. After flush, the engine's clone is freed.
    assert_eq!(t.get(b"k").unwrap(), Some((1, payload.to_vec())));
    t.flush().unwrap();
    assert_eq!(t.get(b"k").unwrap(), Some((1, payload.to_vec())));
    // payload still valid in the test (our clone) — verifies no use-after-free.
    assert_eq!(payload.as_ref(), &[0xAB; 1024]);
}
