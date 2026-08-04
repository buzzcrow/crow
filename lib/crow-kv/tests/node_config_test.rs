// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for `NodeConfig` and `NodeConfigStore`.

use crow_kv::cluster::group_config::{PxGroupConfig, PxGroupMember};
use crow_kv::cluster::node_config::{NodeConfig, NodeConfigStore};

#[test]
fn node_config_default_is_empty() {
    let config = NodeConfig::default();
    assert!(config.stores.is_empty());
}

#[test]
fn node_config_upsert_group_creates_store_if_missing() {
    let mut config = NodeConfig::default();
    let group_config = PxGroupConfig {
        group_id: 5,
        term: 3,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: "127.0.0.1:28001".to_string(),
            voting: true,
        }],
        membership_epoch: 2,
    };
    config.upsert_group(10, &group_config, 1);
    let store = config.store(10).expect("store created");
    assert_eq!(store.groups.len(), 1);
    assert_eq!(store.groups[0].group_id, 5);
    assert_eq!(store.groups[0].replica_id, 1);
    assert_eq!(store.groups[0].membership_epoch, 2);
    assert_eq!(store.groups[0].term, 3);
    assert_eq!(store.groups[0].members.len(), 1);
}

#[test]
fn node_config_upsert_group_updates_existing() {
    let mut config = NodeConfig::default();
    let g1 = PxGroupConfig {
        group_id: 5,
        term: 1,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: "127.0.0.1:28001".to_string(),
            voting: true,
        }],
        membership_epoch: 0,
    };
    config.upsert_group(10, &g1, 1);
    let g2 = PxGroupConfig {
        group_id: 5,
        term: 4,
        members: vec![
            PxGroupMember {
                replica_id: 1,
                endpoint: "127.0.0.1:28001".to_string(),
                voting: true,
            },
            PxGroupMember {
                replica_id: 2,
                endpoint: "127.0.0.1:28002".to_string(),
                voting: true,
            },
        ],
        membership_epoch: 1,
    };
    config.upsert_group(10, &g2, 1);
    let store = config.store(10).expect("store exists");
    assert_eq!(store.groups.len(), 1, "should still be one group");
    assert_eq!(store.groups[0].members.len(), 2);
    assert_eq!(store.groups[0].membership_epoch, 1);
    assert_eq!(store.groups[0].term, 4);
}

#[test]
fn node_config_upsert_group_multiple_stores() {
    let mut config = NodeConfig::default();
    let g = PxGroupConfig {
        group_id: 1,
        term: 0,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: String::new(),
            voting: true,
        }],
        membership_epoch: 0,
    };
    config.upsert_group(1, &g, 1);
    config.upsert_group(2, &g, 1);
    config.upsert_group(3, &g, 1);
    assert_eq!(config.stores.len(), 3);
}

#[test]
fn node_config_remove_group() {
    let mut config = NodeConfig::default();
    let g = PxGroupConfig {
        group_id: 7,
        term: 0,
        members: vec![],
        membership_epoch: 0,
    };
    config.upsert_group(1, &g, 1);
    assert!(config.group(1, 7).is_some());
    assert!(config.remove_group(1, 7));
    assert!(config.group(1, 7).is_none());
    assert!(!config.remove_group(1, 7), "idempotent");
}

#[test]
fn node_config_remove_store() {
    let mut config = NodeConfig::default();
    let g = PxGroupConfig {
        group_id: 1,
        term: 0,
        members: vec![],
        membership_epoch: 0,
    };
    config.upsert_group(5, &g, 1);
    assert!(config.store(5).is_some());
    assert!(config.remove_store(5));
    assert!(config.store(5).is_none());
    assert!(!config.remove_store(5), "idempotent");
}

#[tokio::test]
async fn node_config_store_save_then_load_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());

    let config = PxGroupConfig {
        group_id: 7,
        term: 3,
        members: vec![
            PxGroupMember {
                replica_id: 1,
                endpoint: "127.0.0.1:28001".to_string(),
                voting: true,
            },
            PxGroupMember {
                replica_id: 2,
                endpoint: "127.0.0.1:28002".to_string(),
                voting: true,
            },
        ],
        membership_epoch: 5,
    };
    store.save_group(1, &config, 1).await.expect("save");

    let loaded = store.load_group(1, 7).await.expect("load");
    let loaded = loaded.expect("group should exist");
    assert_eq!(loaded.group_id, 7);
    assert_eq!(loaded.term, 3);
    assert_eq!(loaded.members.len(), 2);
    assert_eq!(loaded.membership_epoch, 5);
}

#[tokio::test]
async fn node_config_store_load_returns_none_when_no_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());
    let loaded = store.load_group(1, 1).await.expect("load");
    assert!(loaded.is_none());
}

#[tokio::test]
async fn node_config_store_load_returns_none_for_missing_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());
    let config = PxGroupConfig {
        group_id: 1,
        term: 0,
        members: vec![],
        membership_epoch: 0,
    };
    store.save_group(1, &config, 1).await.expect("save");
    let loaded = store.load_group(1, 99).await.expect("load");
    assert!(loaded.is_none());
}

#[tokio::test]
async fn node_config_store_save_overwrites_previous() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());

    let cfg1 = PxGroupConfig {
        group_id: 1,
        term: 1,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: "a".to_string(),
            voting: true,
        }],
        membership_epoch: 0,
    };
    store.save_group(1, &cfg1, 1).await.expect("save 1");

    let cfg2 = PxGroupConfig {
        group_id: 1,
        term: 5,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: "b".to_string(),
            voting: true,
        }],
        membership_epoch: 3,
    };
    store.save_group(1, &cfg2, 1).await.expect("save 2");

    let loaded = store.load_group(1, 1).await.expect("load").expect("exists");
    assert_eq!(loaded.term, 5);
    assert_eq!(loaded.members[0].endpoint, "b");
    assert_eq!(loaded.membership_epoch, 3);
}

#[tokio::test]
async fn node_config_store_isolates_groups_by_store_and_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());

    let cfg_a = PxGroupConfig {
        group_id: 1,
        term: 0,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: "a".to_string(),
            voting: true,
        }],
        membership_epoch: 0,
    };
    let cfg_b = PxGroupConfig {
        group_id: 2,
        term: 0,
        members: vec![PxGroupMember {
            replica_id: 1,
            endpoint: "b".to_string(),
            voting: true,
        }],
        membership_epoch: 0,
    };
    store.save_group(1, &cfg_a, 1).await.expect("save a");
    store.save_group(1, &cfg_b, 1).await.expect("save b");

    let loaded_a = store.load_group(1, 1).await.expect("load").expect("exists");
    let loaded_b = store.load_group(1, 2).await.expect("load").expect("exists");
    assert_eq!(loaded_a.members[0].endpoint, "a");
    assert_eq!(loaded_b.members[0].endpoint, "b");
}

#[tokio::test]
async fn node_config_store_remove_group_persists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());

    let cfg = PxGroupConfig {
        group_id: 3,
        term: 0,
        members: vec![],
        membership_epoch: 0,
    };
    store.save_group(1, &cfg, 1).await.expect("save");
    assert!(store.load_group(1, 3).await.expect("load").is_some());

    store.remove_group(1, 3).await.expect("remove");
    assert!(store.load_group(1, 3).await.expect("load").is_none());
}

#[tokio::test]
async fn node_config_store_multiple_stores_in_one_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NodeConfigStore::new(dir.path());

    let g = PxGroupConfig {
        group_id: 1,
        term: 0,
        members: vec![],
        membership_epoch: 0,
    };
    store.save_group(1, &g, 1).await.expect("save store 1");
    store.save_group(2, &g, 1).await.expect("save store 2");
    store.save_group(3, &g, 1).await.expect("save store 3");

    let full = store.load().await.expect("load");
    assert_eq!(full.stores.len(), 3);
    assert!(full.store(1).is_some());
    assert!(full.store(2).is_some());
    assert!(full.store(3).is_some());
}
