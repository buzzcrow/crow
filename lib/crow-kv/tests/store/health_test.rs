// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Health-report tests for `PxKvStore::status()`.
//!
//! HTTP /health integration (200 vs 503 status mapping, full hierarchical
//! JSON shape) is covered end-to-end in the real-process test suite.

use std::sync::Arc;
use std::time::Duration;

use crow_kv::cluster::group::PxGroup;
use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::cluster::status::StatusLevel;

fn make_store_single_replica() -> Arc<PxKvStore> {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    store.add_group(group);
    store
}

#[tokio::test]
async fn health_unhealthy_before_start() {
    // gRPC server has not been started — listen handle absent → Unhealthy.
    let store = make_store_single_replica();
    let s = store.status();
    assert_eq!(s.status, StatusLevel::Unhealthy, "{s:?}");
    assert!(
        s.messages.iter().any(|m| m.contains("not running")),
        "expected 'not running' message, got: {messages:?}",
        messages = s.messages
    );
}

#[tokio::test]
async fn health_ok_after_start_single_replica() {
    let store = make_store_single_replica();
    store.start().await.expect("start failed");
    let s = store.status();
    assert_eq!(s.status, StatusLevel::Ok, "{s:?}");
}

#[tokio::test]
async fn health_unhealthy_after_shutdown() {
    let store = make_store_single_replica();
    store.start().await.expect("start failed");
    let _ = store.shutdown(Duration::from_secs(2)).await;
    let s = store.status();
    assert_eq!(s.status, StatusLevel::Unhealthy);
    assert!(
        s.messages.iter().any(|m| m.contains("shut down")),
        "expected 'shut down' message, got: {:?}",
        s.messages
    );
}
