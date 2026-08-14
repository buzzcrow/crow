// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Standalone client library for `CrowKV`.
//!
//! Wraps `crow_kv`'s generated `KvService` gRPC client with:
//! - **Topology cache** (`(store_id, group_id) -> leader_endpoint`) sourced
//!   from `crow-kv-server`'s HTTP management API `/topology` (no gRPC
//!   `DescribeCluster`).
//! - **Retry policy** on `NotLeaderHint` / timeout / other errors, reusing
//!   the same `(client_id, seq)` across retries of one logical write so the
//!   server's dedup cache can do its job.
//! - **`ReadMode` routing**, including client-side `MinSlot` slot
//!   tracking (a bounded per-group high-watermark, not per-key).
//! - A per-endpoint `tonic::Channel` pool.
//!
//! `crow-console` is expected to depend on this crate rather than rolling
//! its own gRPC client.

mod client;
mod client_admin;
mod client_retry;
mod config;
mod error;
mod hardware;
mod kv_cluster;
mod metrics;
mod pool;
mod service_registry;
mod space_usage;
mod topology;
mod watch_notify;

pub use client::{
    new_client_id, BatchOp, CrowkvClient, GetOutcome, JournalOp, JournalScanOutcome, ScanOutcome,
    WriteOutcome,
};
pub use config::{ClientConfig, ReadEndpointPolicy, RetryConfig};
pub use error::{Error, Result};
pub use hardware::HardwareClient;
pub use kv_cluster::{KVClusterAdmin, KVClusterMetaClient};
pub use metrics::{ClientMetrics, ClientMetricsSnapshot, LeaderChangeEpisode, WindowLatencySnapshot};
pub use service_registry::ServiceRegistryClient;
pub use space_usage::{ClusterUsage, NodeUsage, RackUsage, SpaceUsageClient};
pub use watch_notify::{WatchNotify, WatchNotifyClient, WatchSubscription};

/// Re-exported so callers don't need a direct `crow_kv` dependency just to
/// pick a read mode.
pub use crow_kv::rpc::ReadMode;
