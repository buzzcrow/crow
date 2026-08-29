// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-group engine durability + WAL GC maintenance loop.
//!
//! Periodically, for the local replica of one group:
//!
//! 1. Flushes the in-memory write buffer (L0) into the B+tree (L1) via
//!    [`KVEngine::flush`](crate::kv::KVEngine::flush) — cheap, runs every
//!    tick, advances `last_applied_slot` in memory only.
//! 2. Conditionally persists a durable snapshot to disk via
//!    [`KVEngine::persist_snapshot`](crate::kv::KVEngine::persist_snapshot)
//!    — only when `contiguous_applied - last_snapshot_slot >= threshold`
//!    or `time-since-last-snapshot >= threshold`, to reduce expensive
//!    disk I/O. A no-op for `InMemKV`; for `CrowdbTreeEngine` this is what
//!    makes [`KVEngine::resume_from_slot`](crate::kv::KVEngine::resume_from_slot)
//!    non-zero on a real restart. Purely local: every replica (leader or
//!    follower) does this independently of group-wide agreement.
//! 3. Advances the engine's GC retention watermark and sweeps it
//!    ([`KVEngine::set_gc_watermark`](crate::kv::KVEngine::set_gc_watermark)/
//!    [`collect_garbage`](crate::kv::KVEngine::collect_garbage)), and runs a
//!    WAL segment GC pass ([`crate::wal::gc::run_gc_with_watermark`]) --
//!    these two *are* cross-replica-safety sensitive, so they stay gated on
//!    [`PxGroup::group_safe_slot`] (`0` — not yet established, or this
//!    replica is a follower that doesn't track it; see that method's own
//!    doc — means "nothing yet provably safe to reclaim").
//!
//! `set_gc_watermark`'s two inputs are `snapshot_slot` (state
//! durable on the leader plus at least one peer) and `safe_slot` (state every learner has applied). `snapshot_slot` is
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
use tracing::{debug, error, info, warn};

use super::group::PxGroup;
use crate::wal::gc::run_gc_with_watermark;

/// Start the per-group maintenance loop on `group`, unless already running
/// or `config.election.election_driver_disabled` is set (reused here too:
/// legacy pinned-leader tests that want no per-group background tasks
/// already set this). Tick interval is `config.election.maintenance_tick_ms`
/// (follow-up: previously a hardcoded
/// `DEFAULT_MAINTENANCE_TICK` constant here, now a normal per-group
/// tunable alongside the election timings on `PxElectionConfig`).
pub(crate) async fn start(group: &Arc<PxGroup>) {
    if group.config.election.election_driver_disabled {
        return;
    }
    let mut guard = group.maintenance_handle.lock().await;
    if guard.is_some() {
        return;
    }
    let weak = Arc::downgrade(group);
    let tick = Duration::from_millis(group.config.election.maintenance_tick_ms);
    *guard = Some(spawn(weak, tick, group.tenure_cancel.clone()));
}

/// Spawn the per-group maintenance loop task directly (used by [`start`]
/// and available to tests that want a non-default tick). Held weakly so a
/// dropped group does not leak the task; exits the first time `upgrade`
/// fails or `cancel` fires.
#[must_use]
pub(crate) fn spawn(group: Weak<PxGroup>, tick: Duration, cancel: CancellationToken) -> JoinHandle<()> {
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

/// Persist a durable snapshot on a blocking thread, update watermarks,
/// and return the snapshot slot. Runs on a blocking thread so a large
/// snapshot (e.g. 100k × 16 KiB = 1.6 GB) cannot stall the async
/// election driver / heartbeat task.
async fn persist_snapshot_blocking(
    group: &PxGroup,
    contiguous: u64,
    slot_advance: u64,
    time_elapsed: Duration,
) -> u64 {
    debug!(
        group_id = group.group_id(),
        replica_id = group.local_replica().id,
        contiguous_slot = contiguous,
        slot_advance,
        time_elapsed_ms = u64::try_from(time_elapsed.as_millis()).unwrap_or(u64::MAX),
        "maintenance: persisting snapshot (threshold met)"
    );
    let snap_start = std::time::Instant::now();
    let engine_arc = group.local_replica().learner.engine_arc();
    let group_id = group.group_id();
    let at = tokio::task::spawn_blocking(move || engine_arc.persist_snapshot())
        .await
        .unwrap_or_else(|e| {
            error!(
                group_id,
                error = %e,
                "maintenance: persist_snapshot blocking task panicked; \
                 next step: inspect engine snapshot path for panic"
            );
            0
        });
    let elapsed_ms = u64::try_from(snap_start.elapsed().as_millis()).unwrap_or(u64::MAX);
    if elapsed_ms > 100 {
        info!(
            group_id = group.group_id(),
            replica_id = group.local_replica().id,
            elapsed_ms,
            snapshot_slot = at,
            "maintenance: persist_snapshot completed on blocking thread"
        );
    }
    group
        .last_snapshot_slot
        .store(at, std::sync::atomic::Ordering::Release);
    *group.last_snapshot_time.lock() = std::time::Instant::now();
    debug!(
        group_id = group.group_id(),
        replica_id = group.local_replica().id,
        snapshot_slot = at,
        "maintenance: snapshot persisted"
    );
    at
}

/// Run one maintenance pass. Exposed `pub(crate)` so tests can drive a
/// single deterministic pass without waiting on the periodic loop's timer.
pub(crate) async fn run_pass(group: &PxGroup) {
    let engine = group.local_replica().learner.engine();
    if !engine.is_healthy() {
        error!(
            group_id = group.group_id(),
            replica_id = group.local_replica().id,
            "engine maintenance: KV engine reports unhealthy (durable I/O fault latched); \
             next step: this replica's local state may be missing durably-committed writes -- \
             investigate the underlying storage and consider removing this replica from the \
             group via the admin API"
        );
    }

    // 1. Flush every tick: drain L0 memtable into L1 in memory (cheap no-op
    //    when L0 is empty). Advances last_applied_slot in memory only.
    // Runs on a blocking thread because flush holds the C++ write_mutex_.
    let engine_arc = group.local_replica().learner.engine_arc();
    tokio::task::spawn_blocking(move || engine_arc.flush())
        .await
        .unwrap_or_else(|e| {
            error!(
                group_id = group.group_id(),
                error = %e,
                "maintenance: flush blocking task panicked"
            );
        });

    // 2. Conditionally persist a durable snapshot to disk (expensive: sync +
    //    page writes). Only when the slot advance or time threshold is met.
    let contiguous = group.local_replica().contiguous_applied();
    let last_snap_slot = group
        .last_snapshot_slot
        .load(std::sync::atomic::Ordering::Acquire);
    let slot_advance = contiguous.saturating_sub(last_snap_slot);
    let time_elapsed = {
        let prev = *group.last_snapshot_time.lock();
        std::time::Instant::now().duration_since(prev)
    };
    let should_snapshot = slot_advance >= group.config.election.snapshot_slot_threshold
        || time_elapsed >= Duration::from_millis(group.config.election.snapshot_time_threshold_ms);

    let engine_snapshot_at = if should_snapshot {
        persist_snapshot_blocking(group, contiguous, slot_advance, time_elapsed).await
    } else {
        0
    };

    // 3. GC: set watermark + sweep every tick. The B-tree's own
    //    dropped-count check makes a no-op tick cheap.
    // `set_gc_watermark` is a cheap atomic store and stays inline;
    // `collect_garbage` holds the C++ write_mutex_ and runs on a blocking thread.
    let safe_slot = group.group_safe_slot();
    let snapshot_slot = group.group_snapshot_slot();
    engine.set_gc_watermark(snapshot_slot, safe_slot);
    let engine_arc = group.local_replica().learner.engine_arc();
    tokio::task::spawn_blocking(move || engine_arc.collect_garbage())
        .await
        .unwrap_or_else(|e| {
            error!(
                group_id = group.group_id(),
                error = %e,
                "maintenance: collect_garbage blocking task panicked"
            );
        });

    // 4. WAL GC: only advance snapshot_slot when we actually persisted.
    let Some(wal) = group.local_replica().wal() else {
        return;
    };
    if engine_snapshot_at > 0 && engine_snapshot_at > wal.snapshot_slot() {
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
