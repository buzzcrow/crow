//! Standalone client library for `CrowKV`.
//!
//! Wraps `crowkv`'s generated `KvService` gRPC client with:
//! - **Topology cache** (`(store_id, group_id) -> leader_endpoint`) sourced
//!   from `crowkv-server`'s HTTP management API `/topology` (no gRPC
//!   `DescribeCluster`).
//! - **Retry policy** on `NotLeaderHint` / timeout / other errors, reusing
//!   the same `(client_id, seq)` across retries of one logical write so the
//!   server's dedup cache can do its job.
//! - **`ReadMode` routing**, including client-side `ReadYourWrites` slot
//!   tracking (a bounded per-group high-watermark, not per-key).
//! - A per-endpoint `tonic::Channel` pool.
//!
//! `crowkv-console` is expected to depend on this crate rather than rolling
//! its own gRPC client.

mod client;
mod config;
mod error;
mod pool;
mod topology;

pub use client::{BatchOp, CrowkvClient, GetOutcome, ScanOutcome, WriteOutcome};
pub use config::{ClientConfig, RetryConfig};
pub use error::{Error, Result};

/// Re-exported so callers don't need a direct `crowkv` dependency just to
/// pick a read mode.
pub use crowkv::rpc::ReadMode;
