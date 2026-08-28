// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Standalone client library for `CrowKV`.
//!
//! Wraps `crow_kv`'s generated `KvService` crow-rpc client with:
//! - **Topology cache** (`(store_id, group_id) -> leader_endpoint`) sourced
//!   from `crow-kv-server`'s HTTP management API `/topology` (no crow-rpc
//!   `DescribeCluster`).
//! - **Retry policy** on `NotLeaderHint` / timeout / other errors, reusing
//!   the same `(client_id, seq)` across retries of one logical write so the
//!   server's dedup cache can do its job.
//! - **`ReadMode` routing**, including client-side `MinSlot` slot
//!   tracking (a bounded per-group high-watermark, not per-key).
//! - A per-endpoint connection pool (crow-rpc).
//!
//! `crow-console` is expected to depend on this crate rather than rolling
//! its own crow-rpc client.

mod binding_framework;
mod chunkdb_binding_strategy;
mod client;
mod client_admin;
mod client_retry;
mod config;
mod error;
mod hardware;
mod kv_cluster;
mod kv_rpc_transport;
mod metrics;
mod range_binding;
mod service_registry;
mod space_usage;
mod topology;
mod watch_notify;

// FFI module — only compiled with the `ffi` feature. Produces C ABI
// exports for HardwareClient/ServiceRegistryClient (used by crow-diskio).
#[cfg(feature = "ffi")]
pub mod ffi;

pub use binding_framework::{BindingMonitor, BindingStrategy, MonitorTickResult};
pub use chunkdb_binding_strategy::{
    compute_sub_range_assignment, ChunkdbRangeStrategy, DEFAULT_SUB_RANGE_COUNT,
};
pub use client::{
    new_client_id, BatchOp, CrowkvClient, GetOutcome, JournalOp, JournalScanOutcome, ScanOutcome,
    WriteOutcome,
};
pub use config::{ClientConfig, ReadEndpointPolicy, RetryConfig};
pub use error::{Error, Result};
pub use hardware::{
    DiskCapacityEntry, DiskGroupCapacityEntry, DiskRecord, HardwareCapacitySummary, HardwareClient,
    NodeCapacityEntry, RackCapacityEntry,
};
pub use kv_cluster::{KVClusterAdmin, KVClusterMetaClient};
pub use kv_rpc_transport::KvRpcTransport;
pub use metrics::{ClientMetrics, ClientMetricsSnapshot, LeaderChangeEpisode, WindowLatencySnapshot};
pub use range_binding::{ChunkdbRangeBinding, RangeBindingClient, RangeRouteError, RouteWithFallback};
pub use service_registry::ServiceRegistryClient;
pub use space_usage::{ClusterUsage, NodeUsage, RackUsage, SpaceUsageClient};
pub use watch_notify::{WatchNotify, WatchNotifyClient, WatchSubscription};

/// Re-exported so callers don't need a direct `crow_kv` dependency just to
/// pick a read mode.
pub use crow_kv::rpc::ReadMode;
