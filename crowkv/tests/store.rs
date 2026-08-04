// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Store-layer tests (`PxKvStore`).
//!
//! A store hosts multiple groups behind one node identity and the shared gRPC
//! server. These tests cover request routing to the right group, dynamic
//! add/remove of groups, single-node KV/read modes, the topology `status`
//! snapshot, the `health` report, and graceful `shutdown` cascade.
//!
//! These exercise the `crowkv` library `PxKvStore` directly (the embedded gRPC
//! server, no HTTP). Tests that boot the `crowkv-server` binary / HTTP
//! management API live under `crowkv-server/tests`.

#[path = "testkit/mod.rs"]
mod testkit;

#[path = "store/node_test.rs"]
mod node;

#[path = "store/multi_group_test.rs"]
mod multi_group;

#[path = "store/status_test.rs"]
mod status;

#[path = "store/health_test.rs"]
mod health;

#[path = "store/shutdown_test.rs"]
mod shutdown;

#[path = "store/persistence_test.rs"]
mod persistence;

#[path = "store/kv_correctness_test.rs"]
mod kv_correctness;

#[path = "store/multi_node_multi_group_test.rs"]
mod multi_node_multi_group;

#[path = "store/shutdown_under_load_test.rs"]
mod shutdown_under_load;

#[path = "store/read_metrics_test.rs"]
mod read_metrics;

#[path = "store/readindex_batch_test.rs"]
mod readindex_batch;

#[path = "store/apply_fence_test.rs"]
mod apply_fence;
