// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for `PxGroupConfig` encode/decode and `GroupConfigStore`
//! file-based persistence.

use crowdb_kv::cluster::group_config::{GroupConfigStore, PxGroupConfig, PxGroupMember};

#[test]
fn roundtrip_config() {
    let cfg = PxGroupConfig {
        group_id: 7,
        term: 3,
        members: vec![
            PxGroupMember {
                replica_id: 1,
                endpoint: "127.0.0.1:10001".into(),
                voting: true,
            },
            PxGroupMember {
                replica_id: 2,
                endpoint: "127.0.0.1:10002".into(),
                voting: true,
            },
            PxGroupMember {
                replica_id: 3,
                endpoint: "127.0.0.1:10003".into(),
                voting: true,
            },
        ],
        ..Default::default()
    };
    let encoded = cfg.encode();
    let decoded = PxGroupConfig::decode(&encoded).expect("decode");
    assert_eq!(cfg, decoded);
}

#[tokio::test]
async fn store_save_then_load_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = GroupConfigStore::new(dir.path(), 1, 7);

    let cfg = PxGroupConfig {
        group_id: 7,
        term: 3,
        members: vec![
            PxGroupMember {
                replica_id: 1,
                endpoint: "127.0.0.1:10001".into(),
                voting: true,
            },
            PxGroupMember {
                replica_id: 2,
                endpoint: "127.0.0.1:10002".into(),
                voting: true,
            },
        ],
        ..Default::default()
    };

    store.save(&cfg).await.expect("save");
    let loaded = store.load().await.expect("load");
    assert_eq!(loaded, Some(cfg));
}

#[tokio::test]
async fn store_load_returns_none_when_no_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = GroupConfigStore::new(dir.path(), 1, 7);
    let loaded = store.load().await.expect("load");
    assert!(loaded.is_none());
}

#[tokio::test]
async fn store_save_overwrites_previous_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = GroupConfigStore::new(dir.path(), 1, 1);

    let cfg1 = PxGroupConfig {
        group_id: 1,
        term: 1,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: String::new(),
            voting: true,
        }],
        ..Default::default()
    };
    store.save(&cfg1).await.expect("save 1");

    let cfg2 = PxGroupConfig {
        group_id: 1,
        term: 2,
        members: vec![
            PxGroupMember {
                replica_id: 1,
                endpoint: String::new(),
                voting: true,
            },
            PxGroupMember {
                replica_id: 2,
                endpoint: "127.0.0.1:9000".into(),
                voting: true,
            },
        ],
        ..Default::default()
    };
    store.save(&cfg2).await.expect("save 2");

    let loaded = store.load().await.expect("load");
    assert_eq!(loaded, Some(cfg2));
}

#[tokio::test]
async fn store_isolates_groups_by_store_and_group_id() {
    let dir = tempfile::tempdir().expect("tempdir");

    let store_a = GroupConfigStore::new(dir.path(), 1, 1);
    let store_b = GroupConfigStore::new(dir.path(), 1, 2);

    let cfg_a = PxGroupConfig {
        group_id: 1,
        term: 1,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: String::new(),
            voting: true,
        }],
        ..Default::default()
    };
    store_a.save(&cfg_a).await.expect("save a");

    // store_b should not see store_a's config.
    let loaded_b = store_b.load().await.expect("load b");
    assert!(loaded_b.is_none());

    // store_a should see its own config.
    let loaded_a = store_a.load().await.expect("load a");
    assert_eq!(loaded_a, Some(cfg_a));
}
