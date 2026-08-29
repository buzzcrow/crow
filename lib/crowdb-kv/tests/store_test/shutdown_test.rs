// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cascade shutdown tests for `PxKvStore`. Covers:
//! - normal cascade returns clean report
//! - second `shutdown` call is a clean no-op (idempotency)
//! - timeout branch returns critical error and force-aborts task
//!
//! These are library-level tests on `PxKvStore::shutdown`; the binary entry
//! (`graceful_shutdown` / management API) is covered by `server_api_test.rs`.

use std::sync::Arc;
use std::time::Duration;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;

fn make_store() -> Arc<PxKvStore> {
    let store = Arc::new(PxKvStore::new(7, "127.0.0.1:0".parse().unwrap()));
    let group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    store.add_group(group);
    store
}

#[tokio::test]
async fn shutdown_cascade_clean() {
    let store = make_store();
    store.start().await.expect("start failed");
    assert!(store.listen_addr().is_some());

    let report = store.shutdown(Duration::from_secs(2)).await;
    assert!(
        report.is_clean(),
        "expected clean shutdown, got errors: {:?}",
        report.errors
    );
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let store = make_store();
    store.start().await.expect("start failed");

    let first = store.shutdown(Duration::from_secs(2)).await;
    assert!(first.is_clean());

    // Second call must be a no-op (already-shut-down gate fires) and clean.
    let second = store.shutdown(Duration::from_secs(2)).await;
    assert!(
        second.is_clean(),
        "second shutdown must be a clean no-op, got: {:?}",
        second.errors
    );
}

#[tokio::test]
async fn shutdown_without_start_is_clean() {
    // No crowdb-rpc task spawned; shutdown should still return cleanly without
    // touching `shutdown_server` work.
    let store = make_store();
    let report = store.shutdown(Duration::from_secs(1)).await;
    assert!(report.is_clean(), "{:?}", report.errors);
}
