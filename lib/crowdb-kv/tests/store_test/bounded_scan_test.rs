// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed-cutoff current-version scan coverage through `PxKvStore`.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::rpc::ReadMode;

fn store() -> PxKvStore {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(PxGroup::new(
        1,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store
}

async fn bounded_scan(
    store: &PxKvStore,
    start_after: &[u8],
    limit: u32,
    cutoff: u64,
) -> crowdb_kv::rpc::KvScanResponse {
    store
        .kv_scan(
            1,
            b"k:",
            start_after,
            b"",
            limit,
            ReadMode::Linearizable as i32,
            0,
            false,
            false,
            0,
            true,
            cutoff,
            100,
            1000,
        )
        .await
}

#[tokio::test]
async fn bounded_scan_reports_commit_slots_and_omits_newer_current_versions() {
    let store = store();
    let a = store.kv_put(1, b"k:a", b"a1", 1, 1, 1, 1).await;
    let b = store.kv_put(1, b"k:b", b"b1", 1, 2, 2, 2).await;
    assert!(a.ok && b.ok);

    let first = bounded_scan(&store, b"", 0, 0).await;
    assert!(first.ok);
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].commit_slot, a.revision);
    assert_eq!(first.items[1].commit_slot, b.revision);
    let cutoff = first.scan_cutoff;

    let overwritten = store.kv_put(1, b"k:a", b"a2", 1, 3, 3, 3).await;
    assert!(overwritten.ok);
    assert!(overwritten.revision > cutoff);
    let fixed = bounded_scan(&store, b"", 0, cutoff).await;
    assert!(fixed.ok);
    assert_eq!(fixed.scan_cutoff, cutoff);
    assert_eq!(fixed.items.len(), 1);
    assert_eq!(fixed.items[0].key.as_ref(), b"k:b");
    assert_eq!(fixed.items[0].commit_slot, b.revision);
}

#[tokio::test]
async fn bounded_scan_keeps_one_cutoff_across_pages() {
    let store = store();
    for index in 0..6u64 {
        let key = format!("k:{index}");
        assert!(
            store
                .kv_put(1, key.as_bytes(), b"value", 2, index + 1, index + 1, index + 1)
                .await
                .ok
        );
    }

    let page1 = bounded_scan(&store, b"", 2, 0).await;
    assert!(page1.ok && page1.truncated);
    let cutoff = page1.scan_cutoff;
    assert!(store.kv_put(1, b"k:9", b"later", 3, 1, 20, 20).await.ok);

    let page2 = bounded_scan(&store, page1.items.last().unwrap().key.as_ref(), 2, cutoff).await;
    assert!(page2.ok && page2.truncated);
    let page3 = bounded_scan(&store, page2.items.last().unwrap().key.as_ref(), 2, cutoff).await;
    assert!(page3.ok && !page3.truncated);
    assert_eq!(page2.scan_cutoff, cutoff);
    assert_eq!(page3.scan_cutoff, cutoff);
    assert!(page1
        .items
        .iter()
        .chain(&page2.items)
        .chain(&page3.items)
        .all(|item| item.commit_slot <= cutoff && item.key.as_ref() != b"k:9"));
}

#[tokio::test]
async fn bounded_scan_rejects_cutoff_above_current_frontier() {
    let store = store();
    let put = store.kv_put(1, b"k:a", b"a", 1, 1, 1, 1).await;
    assert!(put.ok);

    let response = bounded_scan(&store, b"", 0, put.revision + 1).await;

    assert!(!response.ok);
    assert!(response.error.contains("exceeds contiguous applied"));
    assert_eq!(response.scan_cutoff, 0);
    assert!(response.items.is_empty());
}
