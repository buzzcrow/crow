//! Cascade shutdown contract for the `PxKvStore` hierarchy.
//!
//! Every layer (`PxKvStore` → `PxGroup` → `PxLocalReplica` / `PxRemoteReplica`
//! → `acceptor` / `learner` / `slot_list` / `kv_store`) implements `shutdown(timeout)`
//! that:
//!
//! 1. Stops accepting new work for that layer.
//! 2. Cascades into children, **continuing on errors** (never aborts the chain).
//! 3. Force-cleans the resource it owns (abort task, close channel, drain
//!    retired pointers, …) when graceful join times out.
//! 4. Returns a [`ShutdownReport`] with aggregated `critical:` errors.
//!
//! Calls are **idempotent** — second and later calls return an empty clean
//! report and log at `debug`. Layers are responsible for their own
//! `AtomicBool` "already-shutdown" gate.
//!
//! ## Why this shape
//!
//! - Caller decides what to do with errors (retry, surface to operator, panic
//!   in tests). Mirrors how Rust idiomatic shutdown is usually expressed via
//!   `Result`-aggregation.
//! - Per-layer timeout guarantees the chain returns even if a child hangs;
//!   the timed-out layer is force-cleaned and a `critical:` line tells the
//!   operator which resource leaked.
//! - Sub-shutdowns are awaited (not spawned) so the report accurately
//!   reflects the state of every owned resource at return time.

use std::time::Duration;

/// Default per-layer graceful-shutdown timeout.
///
/// Shutdowns that take longer almost always indicate a stuck task and are
/// better force-cleaned than waited on.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Aggregated outcome of a cascade shutdown.
///
/// `errors` contains one entry per `critical:` issue encountered (timeout,
/// failed force-cleanup). An empty `errors` means every layer shut down
/// gracefully within its budget.
#[derive(Debug, Default)]
pub struct ShutdownReport {
    pub errors: Vec<String>,
}

impl ShutdownReport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if no `critical:` error was recorded.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }

    /// Append a single `critical:` error message.
    pub fn push_error(&mut self, msg: impl Into<String>) {
        self.errors.push(msg.into());
    }

    /// Merge another report's errors into this one.
    pub fn merge(&mut self, other: ShutdownReport) {
        self.errors.extend(other.errors);
    }
}
