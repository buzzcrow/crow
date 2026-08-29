// Copyright 2026-present Gian <crow.db@outlook.com>

//! R59 snapshot versioning API: store-level tests through `PxKvStore`
//! public API with a real `CrowdbTreeEngine`-backed group.
//!
//! Covers:
//! - `kv_create_snapshot` returns a handle + `at_slot`.
//! - `kv_snapshot_scan` returns a point-in-time-consistent view (no
//!   vanishing keys, no value drift) even as concurrent writes mutate
//!   the keyspace after the snapshot.
//! - `kv_list_snapshots` returns active handles.
//! - `kv_release_snapshot` drops the handle; subsequent scan fails.
//! - Backward-compat: live `kv_scan` is unchanged.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::kv::{CrowdbTreeEngine, CrowdbTreeOptions};
use crowdb_kv::rpc::ReadMode;
use crowdb_kv::wal::io_backend::IoBackend;
use crowdb_kv::wal::replay::replay_group;
use std::path::PathBuf;
use std::sync::Arc;

async fn crowdb_tree_store() -> PxKvStore {
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    let engine = CrowdbTreeEngine::open(&CrowdbTreeOptions::default()).expect("open crowdb-tree engine");
    // Empty replay (no WAL segments) — gives a fresh replica with the
    // caller-supplied engine wired in.
    let backend = Arc::new(IoBackend::mem_block());
    let empty_replay = replay_group(&backend, &[PathBuf::from("/nonexistent")], 1)
        .await
        .expect("empty replay");
    let replica = PxLocalReplica::restore_from_replay_with_engine(
        10,
        PxLocalReplicaRole::Leader,
        &empty_replay,
        Box::new(engine),
    )
    .await
    .expect("restore_from_replay_with_engine");
    let group = PxGroup::new(1, replica);
    store.add_group(group);
    store
}

#[tokio::test]
async fn create_snapshot_returns_handle_and_at_slot() {
    let store = crowdb_tree_store().await;
    // Write a key so the engine has state.
    assert!(store.kv_put(1, b"k", b"v", 1, 1, 100, 1000).await.ok);
    let resp = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(resp.ok, "create failed: {}", resp.error);
    assert!(resp.snapshot_handle != 0, "handle should be non-zero");
    assert!(resp.at_slot > 0, "at_slot should be > 0");
}

#[tokio::test]
async fn snapshot_scan_is_point_in_time_consistent() {
    let store = crowdb_tree_store().await;
    // Write initial keys (unique client_id/seq per write to avoid
    // idempotency dedup).
    assert!(store.kv_put(1, b"a", b"v1", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_put(1, b"b", b"v2", 1, 2, 101, 1001).await.ok);

    // Create snapshot — pins the view at this point.
    let snap = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(snap.ok, "{}", snap.error);
    let handle = snap.snapshot_handle;
    let pinned_slot = snap.at_slot;

    // Mutate the keyspace after the snapshot.
    assert!(store.kv_put(1, b"a", b"v1_overwritten", 1, 3, 102, 1002).await.ok);
    assert!(store.kv_put(1, b"c", b"v3", 1, 4, 103, 1003).await.ok);
    assert!(store.kv_delete(1, b"b", 1, 5, 104, 1004).await.ok);

    // Snapshot scan should see the original state — no drift, no phantoms.
    let resp = store.kv_snapshot_scan(1, handle, b"", b"", 100).await;
    assert!(resp.ok, "{}", resp.error);
    assert!(!resp.truncated);
    let items: Vec<(Vec<u8>, Vec<u8>)> = resp
        .items
        .into_iter()
        .map(|i| (i.key.to_vec(), i.value.to_vec()))
        .collect();
    assert_eq!(
        items,
        vec![(b"a".to_vec(), b"v1".to_vec()), (b"b".to_vec(), b"v2".to_vec())],
        "snapshot should reflect pinned state at slot {pinned_slot}, not live state"
    );

    // Live scan should see the latest state.
    let live = store
        .kv_scan(
            1,
            b"",
            b"",
            b"",
            100,
            ReadMode::Linearizable as i32,
            0,
            false,
            false,
            0,
            200,
            2000,
        )
        .await;
    assert!(live.ok);
    let live_items: Vec<(Vec<u8>, Vec<u8>)> = live
        .items
        .into_iter()
        .map(|i| (i.key.to_vec(), i.value.to_vec()))
        .collect();
    assert_eq!(
        live_items,
        vec![
            (b"a".to_vec(), b"v1_overwritten".to_vec()),
            (b"c".to_vec(), b"v3".to_vec()),
        ],
        "live scan should reflect latest state (b deleted, a overwritten, c added)"
    );

    // Clean up.
    let rel = store.kv_release_snapshot(1, handle).await;
    assert!(rel.ok);
}

#[tokio::test]
async fn snapshot_scan_prefix_filter() {
    let store = crowdb_tree_store().await;
    assert!(store.kv_put(1, b"prefix_a", b"1", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_put(1, b"prefix_b", b"2", 2, 1, 101, 1001).await.ok);
    assert!(store.kv_put(1, b"other_c", b"3", 3, 1, 102, 1002).await.ok);

    let snap = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(snap.ok);
    let handle = snap.snapshot_handle;

    let resp = store.kv_snapshot_scan(1, handle, b"prefix_", b"", 100).await;
    assert!(resp.ok);
    let items: Vec<Vec<u8>> = resp.items.into_iter().map(|i| i.key.to_vec()).collect();
    assert_eq!(items, vec![b"prefix_a".to_vec(), b"prefix_b".to_vec()]);

    store.kv_release_snapshot(1, handle).await;
}

#[tokio::test]
async fn snapshot_scan_pagination() {
    let store = crowdb_tree_store().await;
    for i in 0..10u8 {
        let key = format!("k{i:02}");
        let val = format!("v{i}");
        assert!(
            store
                .kv_put(
                    1,
                    key.as_bytes(),
                    val.as_bytes(),
                    1,
                    u64::from(i) + 1,
                    100 + u64::from(i),
                    1000
                )
                .await
                .ok
        );
    }

    let snap = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(snap.ok);
    let handle = snap.snapshot_handle;

    // Page 1: limit 3.
    let p1 = store.kv_snapshot_scan(1, handle, b"", b"", 3).await;
    assert!(p1.ok);
    assert!(p1.truncated, "page 1 should be truncated");
    assert_eq!(p1.items.len(), 3);
    assert_eq!(p1.items[0].key, "k00");
    assert_eq!(p1.items[2].key, "k02");

    // Page 2: start_after = last key of page 1.
    let p2 = store.kv_snapshot_scan(1, handle, b"", b"k02", 3).await;
    assert!(p2.ok);
    assert!(p2.truncated, "page 2 should be truncated");
    assert_eq!(p2.items.len(), 3);
    assert_eq!(p2.items[0].key, "k03");
    assert_eq!(p2.items[2].key, "k05");

    // Page 4 (last): start_after = k08, limit 3 — only k09 left.
    let p4 = store.kv_snapshot_scan(1, handle, b"", b"k08", 3).await;
    assert!(p4.ok);
    assert!(!p4.truncated, "last page should not be truncated");
    assert_eq!(p4.items.len(), 1);
    assert_eq!(p4.items[0].key, "k09");

    store.kv_release_snapshot(1, handle).await;
}

#[tokio::test]
async fn list_snapshots_returns_active_handles() {
    let store = crowdb_tree_store().await;
    assert!(store.kv_put(1, b"k", b"v", 1, 1, 100, 1000).await.ok);

    let s1 = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(s1.ok);
    let s2 = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(s2.ok);

    let list = store.kv_list_snapshots(1).await;
    assert!(list.ok);
    assert_eq!(list.snapshots.len(), 2);
    assert!(list
        .snapshots
        .iter()
        .any(|s| s.snapshot_handle == s1.snapshot_handle));
    assert!(list
        .snapshots
        .iter()
        .any(|s| s.snapshot_handle == s2.snapshot_handle));

    // Release one, list should show one.
    store.kv_release_snapshot(1, s1.snapshot_handle).await;
    let list2 = store.kv_list_snapshots(1).await;
    assert!(list2.ok);
    assert_eq!(list2.snapshots.len(), 1);
    assert_eq!(list2.snapshots[0].snapshot_handle, s2.snapshot_handle);

    store.kv_release_snapshot(1, s2.snapshot_handle).await;
}

#[tokio::test]
async fn release_snapshot_makes_scan_fail() {
    let store = crowdb_tree_store().await;
    assert!(store.kv_put(1, b"k", b"v", 1, 1, 100, 1000).await.ok);

    let snap = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(snap.ok);
    let handle = snap.snapshot_handle;

    // Scan works before release.
    let before = store.kv_snapshot_scan(1, handle, b"", b"", 100).await;
    assert!(before.ok);

    // Release.
    let rel = store.kv_release_snapshot(1, handle).await;
    assert!(rel.ok);

    // Scan fails after release.
    let after = store.kv_snapshot_scan(1, handle, b"", b"", 100).await;
    assert!(!after.ok);
    assert!(after.error.contains("not found"));
}

#[tokio::test]
async fn snapshot_scan_skips_tombstones() {
    let store = crowdb_tree_store().await;
    assert!(store.kv_put(1, b"a", b"1", 1, 1, 100, 1000).await.ok);
    assert!(store.kv_put(1, b"b", b"2", 2, 1, 101, 1001).await.ok);
    assert!(store.kv_delete(1, b"a", 3, 1, 102, 1002).await.ok);

    let snap = store
        .kv_create_snapshot(1, ReadMode::Linearizable as i32, 0)
        .await;
    assert!(snap.ok);
    let handle = snap.snapshot_handle;

    let resp = store.kv_snapshot_scan(1, handle, b"", b"", 100).await;
    assert!(resp.ok);
    let items: Vec<Vec<u8>> = resp.items.into_iter().map(|i| i.key.to_vec()).collect();
    // Only b — a is tombstoned and should be skipped.
    assert_eq!(items, vec![b"b".to_vec()]);

    store.kv_release_snapshot(1, handle).await;
}
