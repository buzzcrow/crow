// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Store-layer tests (`PxKvStore`).
//!
//! A store hosts multiple groups behind one node identity and the shared crowdb-rpc
//! server. These tests cover request routing to the right group, dynamic
//! add/remove of groups, single-node KV/read modes, the topology `status`
//! snapshot, the `health` report, and graceful `shutdown` cascade.
//!
//! These exercise the `crowdb_kv` library `PxKvStore` directly (the embedded crowdb-rpc
//! server, no HTTP). Tests that boot the `crowdb-kv-server` binary / HTTP
//! management API live under `crowdb-kv-server/tests`.

mod common;

#[path = "store_test/node_test.rs"]
mod node;

#[path = "store_test/multi_group_test.rs"]
mod multi_group;

#[path = "store_test/status_test.rs"]
mod status;

#[path = "store_test/health_test.rs"]
mod health;

#[path = "store_test/shutdown_test.rs"]
mod shutdown;

#[path = "store_test/persistence_test.rs"]
mod persistence;

#[path = "store_test/wal_isolation_test.rs"]
mod wal_isolation;

#[path = "store_test/kv_correctness_test.rs"]
mod kv_correctness;

#[path = "store_test/multi_node_multi_group_test.rs"]
mod multi_node_multi_group;

#[path = "store_test/shutdown_under_load_test.rs"]
mod shutdown_under_load;

#[path = "store_test/read_metrics_test.rs"]
mod read_metrics;

#[path = "store_test/readindex_batch_test.rs"]
mod readindex_batch;

#[path = "store_test/apply_fence_test.rs"]
mod apply_fence;

#[path = "store_test/snapshot_api_test.rs"]
mod snapshot_api;

#[path = "store_test/bounded_scan_test.rs"]
mod bounded_scan;
