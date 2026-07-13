//! Per-group engine durability + WAL GC maintenance loop
//! (`design-crowtree-snapshot-gc.md`).
//!
//! Periodically, for the local replica of one group:
//!
//! 1. Persists a durable KV-engine snapshot
//!    ([`KVEngine::persist_snapshot`](crate::kv::KVEngine::persist_snapshot))
//!    -- a no-op for `InMemKV`; for `CrowtreeEngine` this is what makes
//!    [`KVEngine::resume_from_slot`](crate::kv::KVEngine::resume_from_slot)
//!    non-zero on a real restart (plan-tree #20). Purely local: every
//!    replica (leader or follower) does this independently of group-wide
//!    agreement, gated only on its own applied progress.
//! 2. Advances the engine's GC retention watermark and sweeps it
//!    ([`KVEngine::set_gc_watermark`](crate::kv::KVEngine::set_gc_watermark)/
//!    [`collect_garbage`](crate::kv::KVEngine::collect_garbage)), and runs a
//!    WAL segment GC pass ([`crate::wal::gc::run_gc_with_watermark`]) --
//!    these two *are* cross-replica-safety sensitive, so they stay gated on
//!    [`PxGroup::group_safe_slot`] (`0` — not yet established, or this
//!    replica is a follower that doesn't track it; see that method's own
//!    doc — means "nothing yet provably safe to reclaim").
//!
//! This treats `snapshot_slot` (design §1: state durable on the leader plus
//! at least one peer) as `safe_slot` (design §1: state every learner has
//! applied) for `set_gc_watermark`'s two inputs, since there is no
//! dedicated "durable on at least one peer" tracker today -- stricter than
//! the design's ideal (which would let reclaim run slightly ahead of full
//! `safe_slot`), but always safe: a slot cannot be locally applied
//! everywhere without having first been durably chosen, so
//! `group_safe_slot` never overstates what `snapshot_slot` would have
//! allowed anyway.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::group::PxGroup;
use crate::wal::gc::run_gc_with_watermark;

/// Default tick for the per-group maintenance loop. Not currently
/// configurable (a natural follow-up); a conservative interval matching the
/// cadence `design-crowtree-snapshot-gc.md §4`'s periodic GC trigger
/// describes.
pub const DEFAULT_MAINTENANCE_TICK: Duration = Duration::from_secs(30);

/// Start the per-group maintenance loop on `group`, unless already running
/// or `election_cfg.election_driver_disabled` is set (reused here too:
/// legacy pinned-leader tests that want no per-group background tasks
/// already set this).
pub(crate) async fn start(group: &Arc<PxGroup>) {
    if group.election_cfg.election_driver_disabled {
        return;
    }
    let mut guard = group.maintenance_handle.lock().await;
    if guard.is_some() {
        return;
    }
    let weak = Arc::downgrade(group);
    *guard = Some(spawn(weak, DEFAULT_MAINTENANCE_TICK, group.tenure_cancel.clone()));
}

/// Spawn the per-group maintenance loop task directly (used by [`start`]
/// and available to tests that want a non-default tick). Held weakly so a
/// dropped group does not leak the task; exits the first time `upgrade()`
/// fails or `cancel` fires.
#[must_use]
pub fn spawn(group: Weak<PxGroup>, tick: Duration, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(maintenance_loop(group, tick, cancel))
}

async fn maintenance_loop(group: Weak<PxGroup>, tick: Duration, cancel: CancellationToken) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(tick) => {}
        }
        let Some(g) = group.upgrade() else { return };
        run_pass(&g).await;
    }
}

/// Run one maintenance pass. Exposed `pub(crate)` so tests can drive a
/// single deterministic pass without waiting on the periodic loop's timer.
pub(crate) async fn run_pass(group: &PxGroup) {
    let engine = group.local_replica().learner.engine();
    let engine_snapshot_at = engine.persist_snapshot();

    let safe_slot = group.group_safe_slot();
    engine.set_gc_watermark(safe_slot, safe_slot);
    engine.collect_garbage();

    let Some(wal) = group.local_replica().wal() else {
        return;
    };
    if engine_snapshot_at > wal.snapshot_slot() {
        wal.set_snapshot_slot(engine_snapshot_at);
    }
    if safe_slot == 0 {
        return;
    }
    let gc_slot = wal.snapshot_slot().min(safe_slot);
    if let Err(e) = run_gc_with_watermark(wal, gc_slot).await {
        warn!(
            group_id = group.group_id(),
            error = %e,
            "engine maintenance: wal gc pass failed"
        );
    }
}
