//! Per-layer health reporting for the `PxKvStore` hierarchy.
//!
//! Each layer (`PxKvStore` → `PxGroup` → `PxLocalReplica` / `PxRemoteReplica`)
//! exposes `health()` returning a [`HealthReport`]. Parent layers compose
//! children reports by taking the **worst** status and concatenating messages.
//!
//! - Each layer decides its own status (`Ok` / `Degraded` / `Unhealthy`).
//! - Layers may attach human-readable `messages`. These do not change the
//!   numeric status; they are hints for operators.
//! - V1 does **not** actively probe peers — status is derived from cached
//!   state (gRPC channel state, recent error counters from metrics, …).
//!   A future `force=true` query parameter can opt in to active probes.

/// Three-state health classification.
///
/// - `Ok`: layer (and all its children) operating normally.
/// - `Degraded`: layer is functional but with reduced redundancy / latency
///   (e.g. one remote unreachable but quorum is still possible).
/// - `Unhealthy`: layer cannot serve writes (lost quorum, local replica down,
///   gRPC server stopped, …). Maps to HTTP 503.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HealthStatus {
    #[default]
    Ok,
    Degraded,
    Unhealthy,
}

impl HealthStatus {
    /// Lowercase string used in JSON output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }

    /// Take the worse of two statuses (`Unhealthy > Degraded > Ok`).
    #[must_use]
    pub fn worst(a: Self, b: Self) -> Self {
        use HealthStatus::{Degraded, Ok, Unhealthy};
        match (a, b) {
            (Unhealthy, _) | (_, Unhealthy) => Unhealthy,
            (Degraded, _) | (_, Degraded) => Degraded,
            (Ok, Ok) => Ok,
        }
    }

    /// Whether this status maps to HTTP 503 (load-balancer signal).
    #[must_use]
    pub fn is_unhealthy(self) -> bool {
        matches!(self, Self::Unhealthy)
    }
}

/// Aggregated health report for one layer.
///
/// `messages` are advisory: a non-empty list does NOT change the status; it
/// surfaces context (e.g. "remote 3 has not been contacted yet").
#[derive(Clone, Debug, Default)]
pub struct HealthReport {
    pub status: HealthStatus,
    pub messages: Vec<String>,
}

impl HealthReport {
    #[must_use]
    pub fn ok() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn degraded(msg: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Degraded,
            messages: vec![msg.into()],
        }
    }

    #[must_use]
    pub fn unhealthy(msg: impl Into<String>) -> Self {
        Self {
            status: HealthStatus::Unhealthy,
            messages: vec![msg.into()],
        }
    }

    /// Push a message without altering status.
    pub fn note(&mut self, msg: impl Into<String>) {
        self.messages.push(msg.into());
    }

    /// Merge a child's report: status becomes worst-of, messages concatenate
    /// (prefixed with `child:` so the source is identifiable).
    pub fn merge_child(&mut self, prefix: &str, child: HealthReport) {
        self.status = HealthStatus::worst(self.status, child.status);
        for msg in child.messages {
            self.messages.push(format!("{prefix}: {msg}"));
        }
    }
}
