// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![cfg(feature = "test-util")]

use crate::cluster::group::{PxGroup, RepairOutcome};
use crate::paxos::roles::SlotIndex;
use crate::paxos::PxNodeId;

/// Test-only hooks (compiled under the `test-util` feature). These expose
/// crate-internal mechanisms — the proposer admission semaphore, a single
/// repair step, and peer-applied injection — to integration tests under
/// `tests/` without permanently widening the production public API.
impl PxGroup {
    /// Acquire all inflight admission permits across all queues so a
    /// test can exhaust the window and observe `ProposeResult::Busy`
    /// (Reject mode) or blocking (Queue mode). Returns permits that
    /// release on drop.
    pub fn try_acquire_all_inflight_permits(&self) -> Vec<tokio::sync::SemaphorePermit<'_>> {
        self.inflight.try_acquire_all()
    }

    /// Borrow the inflight semaphore for tests that need direct access.
    #[must_use]
    pub fn inflight_semaphore(&self) -> &tokio::sync::Semaphore {
        &self.inflight.semaphore
    }

    /// Run one background-repair step, returning the slot that was filled
    /// (`Some`) or `None` when there was no gap to repair / repair did not
    /// choose. Wraps the internal [`Self::repair_once`].
    pub async fn repair_once_for_tests(&self) -> Option<u64> {
        match self.repair_once().await {
            RepairOutcome::Filled { slot } => Some(slot),
            RepairOutcome::NoGap | RepairOutcome::NotLeader | RepairOutcome::Failed => None,
        }
    }

    /// Inject a peer's reported `contiguous_applied` watermark, normally driven
    /// by the leader heartbeat round, so a test can exercise group-safe-slot
    /// computation deterministically. Wraps the internal [`Self::note_peer_applied`].
    pub fn note_peer_applied_for_tests(&self, peer_id: PxNodeId, applied: SlotIndex) {
        self.note_peer_applied(peer_id, applied);
    }

    /// Inject a peer's reported `durable_snapshot_slot` watermark, normally
    /// driven by the leader heartbeat round, so a test can exercise
    /// group-snapshot-slot computation deterministically. Wraps the
    /// internal [`Self::note_peer_durable`].
    pub fn note_peer_durable_for_tests(&self, peer_id: PxNodeId, durable: SlotIndex) {
        self.note_peer_durable(peer_id, durable);
    }

    /// Clear all peer-applied/-durable tracking and reset the published
    /// `group_safe_slot`/`group_snapshot_slot` to `0`, so a test can
    /// exercise the new-leader-tenure reset deterministically without
    /// driving a real election. Wraps the internal
    /// [`Self::reset_safe_slot_tracking`].
    pub fn reset_safe_slot_tracking_for_tests(&self) {
        self.reset_safe_slot_tracking();
    }

    /// Run one [`crate::cluster::group_maintenance`] pass synchronously,
    /// without spawning/waiting on the periodic loop's timer, so a test can
    /// exercise engine-snapshot / GC-watermark / WAL-GC wiring
    /// deterministically.
    pub async fn run_maintenance_pass_for_tests(&self) {
        crate::cluster::group_maintenance::run_pass(self).await;
    }

    /// Install a one-shot gate that holds the next `ReadIndex` heartbeat
    /// round open until `release` is consumed. The test keeps the
    /// `oneshot::Sender` and sends `()` once the batch of concurrent
    /// reads has been fired, so the round leader blocks long enough for
    /// the other reads to enqueue on the pending-barrier queue. Consumed
    /// by the first round that runs after this call.
    pub fn set_readindex_round_gate_for_tests(&self, release: tokio::sync::oneshot::Receiver<()>) {
        *self.readindex_round_gate.lock() = Some(release);
    }

    /// Whether a `ReadIndex` heartbeat round is currently in flight (i.e.
    /// a pending-barrier batch exists). Used by tests to wait until the
    /// round leader has registered its batch before firing the waiters.
    #[must_use]
    pub fn has_pending_read_barrier_for_tests(&self) -> bool {
        self.pending_read_barrier.lock().is_some()
    }

    /// Number of waiters currently queued on the in-flight `ReadIndex`
    /// round. Used by tests to confirm all concurrent reads have batched
    /// onto one round before releasing the gate.
    #[must_use]
    pub fn pending_read_barrier_waiters_for_tests(&self) -> usize {
        self.pending_read_barrier
            .lock()
            .as_ref()
            .map_or(0, |p| p.waiters.len())
    }

    /// Install a one-shot gate that holds the next coalescer round open
    /// until the test releases it, so concurrent ops deterministically
    /// join the pending batch. The test keeps the `oneshot::Sender` and
    /// sends `()` once the batch of concurrent ops has been fired.
    pub fn set_coalesce_round_gate_for_tests(&self, release: tokio::sync::oneshot::Receiver<()>) {
        *self.coalesce_round_gate.lock() = Some(release);
    }

    /// Whether a coalescer round is in flight (pending batch exists).
    #[must_use]
    pub fn has_coalesce_pending_for_tests(&self) -> bool {
        self.coalescer.lock().is_some()
    }

    /// Number of ops in the current pending batch.
    #[must_use]
    pub fn coalesce_pending_count_for_tests(&self) -> u16 {
        self.coalescer.lock().as_ref().map_or(0, |b| b.op_count)
    }

    /// Set the `max_keys` for testing.
    pub fn set_coalesce_max_keys_for_tests(&self, max_keys: u16) {
        self.coalesce_max_keys
            .store(max_keys, std::sync::atomic::Ordering::Relaxed);
    }
}
