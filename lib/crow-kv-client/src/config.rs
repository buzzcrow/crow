// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client configuration. Retry-policy defaults mirror the client
//! interaction spec (retry on `NotLeaderHint`, 100ms-then-retry on unknown
//! leader, exponential backoff on transport errors).

use std::time::Duration;

/// Retry policy:
/// - `NotLeaderHint` with a hint: retried immediately, uncounted (forward
///   progress toward the real leader).
/// - Unknown leader / transport error: counted against `max_retries`, with
///   `unknown_leader_wait` (fixed) or exponential backoff (transport)
///   between attempts.
/// - Anything else: counted against `max_retries`.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Cap on retries for "unknown leader" and "other/transport" outcomes.
    /// `NotLeaderHint`-with-hint retries do not count against this.
    pub max_retries: u32,
    /// Wait before retrying when the leader is completely unknown (no cached
    /// endpoint, no hint from the server). The client queries `/topology`
    /// first, then sleeps this duration before checking again.
    pub unknown_leader_wait: Duration,
    /// Initial backoff after a transport-level error (connect failure,
    /// timeout). Doubles each attempt up to `backoff_max`.
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            unknown_leader_wait: Duration::from_millis(100),
            backoff_base: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
        }
    }
}

/// Read-endpoint selection policy for `MinSlot` reads. Linearizable
/// reads always target the leader regardless of this setting (they must,
/// for correctness); the policy only affects how a `MinSlot` read's
/// first endpoint is chosen.
///
/// - `Leader` (default) — preserve the historical behavior: every read
///   starts at the cached leader endpoint. Backward compatible and
///   linearizable-safe.
/// - `AnyReplica` — `MinSlot` reads are distributed round-robin across
///   all replica endpoints the topology cache knows for the group. If a
///   chosen follower has not applied `min_slot`, the server returns a
///   `NotLeader` hint pointing at the leader and the client follows it
///   for that one request (mirroring the existing retry path).
/// - `LeastConnections` — routes to the replica with the fewest
///   in-flight reads (tracked client-side via per-endpoint atomic
///   counters). Ties and the first request (no history) fall back to
///   round-robin. Same `NotLeader` fallback as `AnyReplica`.
/// - `Latency` — routes to the replica with the lowest recent RTT
///   (per-endpoint EWMA, `alpha = 0.25`). Ties and the first request
///   (no RTT history) fall back to round-robin. Same `NotLeader`
///   fallback as `AnyReplica`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadEndpointPolicy {
    #[default]
    Leader,
    AnyReplica,
    LeastConnections,
    Latency,
}

impl ReadEndpointPolicy {
    /// Returns `true` when the policy distributes `MinSlot` reads across
    /// the replica list (i.e. it is not `Leader`). Used to gate the
    /// `read_endpoint_distributed` / `read_endpoint_fallback` counters
    /// so they fire for every distributed policy, not just `AnyReplica`.
    #[must_use]
    pub fn is_distributed(self) -> bool {
        !matches!(self, Self::Leader)
    }
}

/// Top-level client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// HTTP management-API endpoints (`http://host:port`) used to bootstrap
    /// and refresh the topology cache. At least one
    /// must be reachable for the client to discover any group leader.
    pub mgmt_seeds: Vec<String>,
    /// Number of `tonic::Channel`s kept per gRPC endpoint, round-robined.
    /// `1` (default) is sufficient for most workloads since a single
    /// HTTP/2 channel already multiplexes concurrent requests; raise this
    /// only if profiling shows one channel is the bottleneck.
    pub pool_size_per_endpoint: usize,
    /// Minimum interval between two *actual* topology HTTP fetches; bursts
    /// of concurrent refresh requests within this window coalesce into one
    /// fetch.
    pub topology_min_refresh_interval: Duration,
    pub retry: RetryConfig,
    /// How `MinSlot` reads pick their first endpoint. See
    /// [`ReadEndpointPolicy`]. Default `Leader` preserves the pre-R26
    /// behavior; `AnyReplica` enables follower read distribution.
    pub read_endpoint_policy: ReadEndpointPolicy,
}

impl ClientConfig {
    #[must_use]
    pub fn new(mgmt_seeds: Vec<String>) -> Self {
        Self {
            mgmt_seeds,
            pool_size_per_endpoint: 1,
            topology_min_refresh_interval: Duration::from_millis(200),
            retry: RetryConfig::default(),
            read_endpoint_policy: ReadEndpointPolicy::default(),
        }
    }
}
