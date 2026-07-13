//! Client configuration. Retry-policy defaults mirror the client
//! interaction spec (retry on `NotLeaderHint`, 1s-then-retry on unknown
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
    /// endpoint, no hint from the server).
    pub unknown_leader_wait: Duration,
    /// Initial backoff after a transport-level error (connect failure,
    /// timeout). Doubles each attempt up to `backoff_max`.
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    /// When `true`, disables *all* client-side resilience -- including the
    /// normally-uncounted, unconditional `NotLeaderHint` follow -- so every
    /// call makes exactly one RPC and returns whatever that single attempt
    /// produced (success, application error, or transport error) with no
    /// wait, no topology refresh, and no redirect. For latency/error-rate
    /// benchmarking callers (e.g. `crowkv-cli`'s bench runner) where any
    /// client-side retry would silently convert a real failure into a
    /// slower success and corrupt the measurement. Default `false` (normal
    /// resilient behavior) for every other caller.
    pub single_attempt: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            unknown_leader_wait: Duration::from_secs(1),
            backoff_base: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
            single_attempt: false,
        }
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
}

impl ClientConfig {
    #[must_use]
    pub fn new(mgmt_seeds: Vec<String>) -> Self {
        Self {
            mgmt_seeds,
            pool_size_per_endpoint: 1,
            topology_min_refresh_interval: Duration::from_millis(200),
            retry: RetryConfig::default(),
        }
    }
}
