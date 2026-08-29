// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for restore mode (R104): local-disk scan + restart rebuild.

mod common;

use std::path::Path;

use common::process::{start_test_server, start_test_server_at};
use crowdb_kv_server::restore::{group0_exists, scan_local_groups};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

// ── Unit tests: scan_local_groups / group0_exists ───────────────

#[tokio::test]
async fn scan_local_groups_multi_store() {
    let dir = tempfile::tempdir().unwrap();
    let waldata = dir.path().join("waldata");
    // store0/group0, store1/group1, store1/group2
    tokio::fs::create_dir_all(waldata.join("store0").join("group0"))
        .await
        .unwrap();
    tokio::fs::create_dir_all(waldata.join("store1").join("group1"))
        .await
        .unwrap();
    tokio::fs::create_dir_all(waldata.join("store1").join("group2"))
        .await
        .unwrap();
    // stray file at waldata root + a non-matching dir
    tokio::fs::write(waldata.join("README"), b"ignore me")
        .await
        .unwrap();
    tokio::fs::create_dir_all(waldata.join("notastore"))
        .await
        .unwrap();
    // store3 with no groups
    tokio::fs::create_dir_all(waldata.join("store3")).await.unwrap();

    let groups = scan_local_groups(&waldata).await.unwrap();
    assert_eq!(
        groups,
        vec![
            crowdb_kv_server::restore::LocalGroup {
                store_id: 0,
                group_id: 0
            },
            crowdb_kv_server::restore::LocalGroup {
                store_id: 1,
                group_id: 1
            },
            crowdb_kv_server::restore::LocalGroup {
                store_id: 1,
                group_id: 2
            },
        ]
    );
}

#[tokio::test]
async fn scan_local_groups_missing_dir_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let waldata = dir.path().join("waldata"); // not created
    let groups = scan_local_groups(&waldata).await.unwrap();
    assert!(groups.is_empty());
}

#[tokio::test]
async fn group0_exists_true_and_false() {
    let dir = tempfile::tempdir().unwrap();
    let waldata = dir.path().join("waldata");
    assert!(!group0_exists(&waldata));
    tokio::fs::create_dir_all(waldata.join("store0").join("group0"))
        .await
        .unwrap();
    assert!(group0_exists(&waldata));
}

#[tokio::test]
async fn group0_exists_ignores_other_stores() {
    let dir = tempfile::tempdir().unwrap();
    let waldata = dir.path().join("waldata");
    tokio::fs::create_dir_all(waldata.join("store1").join("group1"))
        .await
        .unwrap();
    assert!(!group0_exists(&waldata));
}

// ── E2E: restart restores group 0 from disk ─────────────────────

#[tokio::test]
async fn restart_restores_group0_from_disk() {
    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_path_buf();

    // First boot: no group 0 on disk → first-boot mode, empty mgmt API.
    let server = start_test_server_at(&root_path, &[], &[0])
        .await
        .expect("start crowdb-kv-server (first boot)");
    let resp: serde_json::Value = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["store_id"], 0);
    assert_eq!(resp["group_id"], 0);
    // Drop the handle → SIGTERM → process exits; root tempdir survives
    // (owned by the test, not the handle).
    drop(server);

    // Give the process a moment to exit and release the WAL files.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        group0_exists(&Path::new(&root_path).join("waldata")),
        "group 0 WAL should still be on disk after stop"
    );

    // Restart with the SAME root, no --stores/--groups, no --config.
    let server = start_test_server_at(&root_path, &[], &[0])
        .await
        .expect("start crowdb-kv-server (restart)");
    server
        .wait_for_ready(std::time::Duration::from_secs(10))
        .await
        .unwrap();

    // group 0 must have been restored from disk: a second /system/init
    // returns 409 (group 0 already exists) rather than 201.
    let resp = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        409,
        "group 0 should already exist after restore-mode restart"
    );

    // And /stores lists store 0.
    let resp: serde_json::Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids: Vec<u64> = resp["stores"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| s["store_id"].as_u64().unwrap_or(u64::MAX))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        ids.contains(&0),
        "store 0 should be present after restore; got {ids:?}"
    );
}

// ── E2E: first boot with --root only (no toml) still works ───────

#[tokio::test]
async fn first_boot_root_only_no_toml() {
    // start_test_server uses --root only (no --config). Verify the
    // server boots and /system/init works.
    let server = start_test_server(&[]).await.expect("start crowdb-kv-server");
    let resp = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);
}
