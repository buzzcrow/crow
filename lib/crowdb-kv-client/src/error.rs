// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Error type for [`crate::CrowdbKvClient`].

/// Errors surfaced by the client library. Transport/server errors that the
/// retry loop can recover from are handled internally and never reach the
/// caller unless retries are exhausted.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error to {endpoint}: {status}")]
    Transport { endpoint: String, status: String },

    #[error("server rejected the request: {0}")]
    Server(String),

    #[error("disk-group {disk_group_id} owner is immutable: current={current}, requested={requested}")]
    OwnerConflict {
        disk_group_id: u64,
        current: u64,
        requested: u64,
    },

    /// A `JournalScan` asked for slots already GC'd below the WAL trim
    /// point (server `KV_ERROR_JOURNAL_SCAN_GC_GAP`). Deterministic —
    /// the client does not retry it; the caller (diskdb recovery) falls
    /// back to a full-scan rebuild.
    #[error("journal scan asked for slots already GC'd below the WAL trim point")]
    JournalScanGcGap,

    #[error("not leader (hint: {hint})")]
    NotLeader { hint: String },

    #[error("no known leader for group (store_id={store_id}, group_id={group_id})")]
    NoLeader { store_id: u64, group_id: u64 },

    #[error("retries exhausted after {attempts} attempts, last error: {last}")]
    RetriesExhausted { attempts: u32, last: String },

    #[error("topology discovery failed: {0}")]
    Topology(String),

    /// No mgmt seed URLs are configured — the client cannot discover
    /// any leader. Returned immediately (no retry) so the caller gets
    /// a clear, fast error instead of a 5s timeout. If you see this,
    /// the code that created the `CrowdbKvClient` forgot to call
    /// `set_mgmt_seeds` with the cluster's server URLs.
    #[error("no mgmt seeds configured — cannot discover leader; call set_mgmt_seeds first")]
    NoSeeds,

    #[error("invalid endpoint {endpoint}: {reason}")]
    InvalidEndpoint { endpoint: String, reason: String },

    #[error("sysdata decode failed for key {key}: {reason}")]
    SysdataDecode { key: String, reason: String },

    #[error("sysdata key parse failed: {0}")]
    SysdataKeyParse(String),

    #[error("mgmt API error: {0}")]
    Mgmt(String),

    /// No living instances of a service were found in the group-0
    /// service registry. Returned by `ServiceDiscoveryClient::discover_one`
    /// when `read_all_instances` returns an empty vector.
    #[error("no living instances of service '{service}' in group-0 registry")]
    NoLivingInstances { service: String },

    /// The group-0 service registry is unreachable — the underlying
    /// `CrowdbKvClient` exhausted its retry budget trying to read
    /// `/srv/<service>/`. The cache is not invalidated on this error
    /// (a stale cache is better than no cache if group-0 is transiently
    /// down).
    #[error("group-0 registry unreachable for service '{service}': {source}")]
    DiscoveryUnreachable { service: String, source: Box<Error> },
}

pub type Result<T> = std::result::Result<T, Error>;
