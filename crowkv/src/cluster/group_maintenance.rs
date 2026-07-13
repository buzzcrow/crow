//! Per-group engine durability + WAL GC maintenance loop
//! (`design-crowtree-storage.md`).
//!
//! Periodically, for the local replica of one group:
//!
//! 1. Persists a durable KV-engine snapshot
//!    ([`KVEngine::persist_snapshot`](crate::kv::KVEngine::persist_snapshot))
//!    -- a no-op for `InMemKV`; for `CrowtreeEngine` this is what makes
//!    [`KVEngine::resume_from_slot`](crate::kv::KVEngine::resume_from_slot)
//!    non-zero on a real restart. Purely local: every
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
//! `set_gc_watermark`'s two inputs are `snapshot_slot` (design §1: state
//! durable on the leader plus at least one peer) and `safe_slot` (design §1:
//! state every learner has applied). `snapshot_slot` is
//! [`PxGroup::group_snapshot_slot`]: each replica's own
//! `WalEngine::snapshot_slot` (updated below, right after `persist_snapshot`
//! advances it) is gossiped to the leader piggybacked on the same heartbeat
//! round as `contiguous_applied` (see `HeartbeatReply::durable_snapshot_slot`
//! / `PxGroup::note_peer_durable`), which aggregates it into `min(leader's
//! own durable slot, max(voting peer durable slots))` -- always `<=` what
//! `group_safe_slot` would have allowed (a slot cannot be locally applied
//! everywhere without having first been durably chosen), so this can only
//! let reclaim run *more* conservatively than the old `safe_slot`
//! approximation, never less.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use super::group::PxGroup;
use crate::wal::gc::run_gc_with_watermark;

/// Start the per-group maintenance loop on `group`, unless already running
/// or `election_cfg.election_driver_disabled` is set (reused here too:
/// legacy pinned-leader tests that want no per-group background tasks
/// already set this). Tick interval is `election_cfg.maintenance_tick_ms`
/// (follow-up: previously a hardcoded
/// `DEFAULT_MAINTENANCE_TICK` constant here, now a normal per-group
/// tunable alongside the election timings on `PxElectionConfig`).
pub(crate) async fn start(group: &Arc<PxGroup>) {
    if group.election_cfg.election_driver_disabled {
        return;
    }
    let mut guard = group.maintenance_handle.lock().await;
    if guard.is_some() {
        return;
    }
    let weak = Arc::downgrade(group);
    let tick = Duration::from_millis(group.election_cfg.maintenance_tick_ms);
    *guard = Some(spawn(weak, tick, group.tenure_cancel.clone()));
}

/// Spawn the per-group maintenance loop task directly (used by [`start`]
/// and available to tests that want a non-default tick). Held weakly so a
/// dropped group does not leak the task; exits the first time `upgrade`
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
    if !engine.is_healthy() {
        // No automatic step-out trigger exists yet -- this is the
        // observability half of that gap: a persistently
        // unhealthy engine is now at least loudly, repeatedly logged on
        // every maintenance tick instead of being silently invisible
        // outside of an explicit health-check call.
        error!(
            group_id = group.group_id(),
            replica_id = group.local_replica().id,
            "engine maintenance: KV engine reports unhealthy (durable I/O fault latched); \
             next step: this replica's local state may be missing durably-committed writes -- \
             investigate the underlying storage and consider removing this replica from the \
             group via the admin API"
        );
    }
    let engine_snapshot_at = engine.persist_snapshot();

    let safe_slot = group.group_safe_slot();
    let snapshot_slot = group.group_snapshot_slot();
    engine.set_gc_watermark(snapshot_slot, safe_slot);
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
