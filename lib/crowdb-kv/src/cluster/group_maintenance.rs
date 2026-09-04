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
//! 3. Conditionally forces a durable WAL flush via
//!    [`WalEngine::flush_all`] — only when
//!    `time-since-last-wal-flush >= wal_flush_interval_ms`, so WAL data
//!    is persisted even with `--no-fsync`. `0` disables (WAL is still
//!    flushed on shutdown).
//! 4. Advances the engine's GC retention watermark
//!    ([`KVEngine::set_gc_watermark`](crate::kv::KVEngine::set_gc_watermark)),
//!    conditionally runs cadence-driven block compaction
//!    ([`KVEngine::compact_sparse_blocks`](crate::kv::KVEngine::compact_sparse_blocks)),
//!    and runs a WAL segment GC pass
//!    ([`crate::wal::gc::run_gc_with_watermark`]) -- these *are*
//!    cross-replica-safety sensitive, so they stay gated on
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

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, info_span, warn, Instrument, Span};

use super::group::PxGroup;
use super::group_election::LeaderElection;
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
    let span = group.upgrade().map_or_else(tracing::Span::none, |group| {
        let g = group.group_id();
        let replica = group.local_replica().id;
        group.log_store_id().map_or_else(
            || info_span!("group_maintenance", g, replica),
            |s| info_span!("group_maintenance", s, g, replica),
        )
    });
    tokio::spawn(maintenance_loop(group, tick, cancel).instrument(span))
}

async fn maintenance_loop(group: Weak<PxGroup>, tick: Duration, cancel: CancellationToken) {
    let mut last_flush = std::time::Instant::now();
    loop {
        let Some(g) = group.upgrade() else { return };
        // Gap 5 step 2: wait for either a flush signal (memtable froze),
        // the tick timeout (watchdog), or cancellation. The flush signal
        // makes drain rate track write rate; the tick is now a safety net
        // for idle/low-write tails, not the primary flush trigger.
        let by_notify = tokio::select! {
            () = cancel.cancelled() => return,
            () = g.flush_notify.notified() => true,
            () = tokio::time::sleep(tick) => false,
        };
        // On tick: skip flush if we recently flushed via notify (proactive).
        // The tick is a safety net for idle periods — if writes are active,
        // flush_notify already drains small batches promptly. Skipping the
        // tick flush avoids force-freezing the active table and draining a
        // large batch, which would create big per-leaf deltas and trigger
        // excessive consolidation.
        let do_flush = by_notify || last_flush.elapsed() >= tick;
        run_pass(&g, do_flush).await;
        if do_flush {
            last_flush = std::time::Instant::now();
        }
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
        contiguous_slot = contiguous,
        slot_advance,
        time_elapsed_ms = u64::try_from(time_elapsed.as_millis()).unwrap_or(u64::MAX),
        "maintenance: persisting snapshot (threshold met)"
    );
    let snap_start = std::time::Instant::now();
    let engine_arc = group.local_replica().learner.engine_arc();
    let span = Span::current();
    let at = tokio::task::spawn_blocking(move || span.in_scope(|| engine_arc.persist_snapshot()))
        .await
        .unwrap_or_else(|e| {
            error!(
                error = %e,
                "maintenance: persist_snapshot blocking task panicked; \
                 next step: inspect engine snapshot path for panic"
            );
            0
        });
    let elapsed_ms = u64::try_from(snap_start.elapsed().as_millis()).unwrap_or(u64::MAX);
    if elapsed_ms > 100 {
        info!(
            elapsed_ms,
            snapshot_slot = at,
            "maintenance: persist_snapshot completed on blocking thread"
        );
    }
    // Gap 4: only advance watermarks if the snapshot actually succeeded
    // (at > 0). A failed snapshot (at == 0) must NOT update
    // `last_snapshot_slot` or `last_snapshot_time` — otherwise the next
    // pass thinks a snapshot at slot 0 happened, messing up the threshold
    // check and potentially advancing the GC watermark incorrectly.
    if at > 0 {
        group
            .last_snapshot_slot
            .store(at, std::sync::atomic::Ordering::Release);
        *group.last_snapshot_time.lock() = std::time::Instant::now();
        debug!(snapshot_slot = at, "maintenance: snapshot persisted");
    } else {
        warn!(
            "maintenance: persist_snapshot returned 0 — snapshot failed or nothing to persist; \
             watermarks not advanced"
        );
    }
    at
}

/// Run one maintenance pass. Exposed `pub(crate)` so tests can drive a
/// single deterministic pass without waiting on the periodic loop's timer.
/// `do_flush`: when false, skip the flush phase (tick woke the loop but a
/// recent proactive flush already drained — only check snapshot/WAL).
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_pass(group: &PxGroup, do_flush: bool) {
    let engine = group.local_replica().learner.engine();
    if !engine.is_healthy() {
        error!(
            "engine maintenance: KV engine reports unhealthy (durable I/O fault latched); \
             next step: this replica's local state may be missing durably-committed writes -- \
             investigate the underlying storage and consider removing this replica from the \
             group via the admin API"
        );
    }

    // 1. Flush: drain L0 memtable into L1 in memory (cheap no-op when L0
    //    is empty). Advances last_applied_slot in memory only. Skipped on
    //    tick wakes when a recent proactive flush already drained — avoids
    //    force-freezing the active table and draining a large batch.
    // Runs on a blocking thread because flush holds the C++ write_mutex_.
    if do_flush {
        let flush_start = std::time::Instant::now();
        let engine_arc = group.local_replica().learner.engine_arc();
        let span = Span::current();
        tokio::task::spawn_blocking(move || span.in_scope(|| engine_arc.flush()))
            .await
            .unwrap_or_else(|e| {
                error!(
                    error = %e,
                    "maintenance: flush blocking task panicked"
                );
            });
        debug!(
            elapsed_ms = u64::try_from(flush_start.elapsed().as_millis()).unwrap_or(u64::MAX),
            "maintenance: flush phase complete"
        );

        // Gap 5 step 4: increment flush counter for the snapshot trigger.
        group
            .flushes_since_snapshot
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // 2. Conditionally persist a durable snapshot to disk (expensive: sync +
    //    page writes). Three triggers: slot advance, time elapsed, or
    //    flush-count since last snapshot (Gap 5 step 4).
    let contiguous = group.local_replica().contiguous_applied();
    let last_snap_slot = group
        .last_snapshot_slot
        .load(std::sync::atomic::Ordering::Acquire);
    let slot_advance = contiguous.saturating_sub(last_snap_slot);
    let time_elapsed = {
        let prev = *group.last_snapshot_time.lock();
        std::time::Instant::now().duration_since(prev)
    };
    let flush_count = group
        .flushes_since_snapshot
        .load(std::sync::atomic::Ordering::Relaxed);
    let flush_threshold = group.config.election.snapshot_flush_count_threshold;
    let should_snapshot = slot_advance >= group.config.election.snapshot_slot_threshold
        || time_elapsed >= Duration::from_millis(group.config.election.snapshot_time_threshold_ms)
        || (flush_threshold > 0 && flush_count >= flush_threshold);
    if should_snapshot {
        let trigger = if slot_advance >= group.config.election.snapshot_slot_threshold {
            "slot"
        } else if time_elapsed >= Duration::from_millis(group.config.election.snapshot_time_threshold_ms) {
            "time"
        } else {
            "flush_count"
        };
        info!(
            trigger,
            slot_advance,
            time_elapsed_ms = u64::try_from(time_elapsed.as_millis()).unwrap_or(u64::MAX),
            flush_count,
            "maintenance: snapshot triggered"
        );
    }

    let engine_snapshot_at = if should_snapshot {
        let at = persist_snapshot_blocking(group, contiguous, slot_advance, time_elapsed).await;
        // Gap 5 step 4: reset the flush counter on a successful snapshot.
        if at > 0 {
            group
                .flushes_since_snapshot
                .store(0, std::sync::atomic::Ordering::Relaxed);
        }
        at
    } else {
        0
    };

    // 3. Conditionally force a durable WAL flush (real fsync, even with
    //    --no-fsync). Gated on wal_flush_interval_ms so we don't fsync
    //    every tick. The WAL writer's own pipeline mutex serializes
    //    flush commands against appends — no extra lock needed here.
    let wal_interval = group.config.election.wal_flush_interval_ms;
    if wal_interval > 0 {
        let elapsed = {
            let prev = *group.last_wal_flush_time.lock();
            std::time::Instant::now().duration_since(prev)
        };
        if elapsed >= Duration::from_millis(wal_interval) {
            if let Some(wal) = group.local_replica().wal() {
                let wal_start = std::time::Instant::now();
                match wal.flush_all().await {
                    Ok(()) => {
                        *group.last_wal_flush_time.lock() = std::time::Instant::now();
                        debug!(
                            elapsed_ms = u64::try_from(wal_start.elapsed().as_millis()).unwrap_or(u64::MAX),
                            "maintenance: WAL flush phase complete"
                        );
                    }
                    Err(e) => {
                        warn!(
                            elapsed_ms = u64::try_from(wal_start.elapsed().as_millis()).unwrap_or(u64::MAX),
                            error = %e,
                            "maintenance: WAL flush_all failed"
                        );
                    }
                }
            }
        }
    }

    // 4. GC: set watermark every tick; run cadence-driven block compaction
    //    when the configured interval has elapsed. Tombstone folding is now
    //    part of snapshot preparation (R129), so there is no standalone
    //    resident-tree GC sweep.
    // `set_gc_watermark` is a cheap atomic store and stays inline;
    // `compact_sparse_blocks` holds the C++ write_mutex_ and runs on a
    // blocking thread.
    let safe_slot = group.group_safe_slot();
    let snapshot_slot = group.group_snapshot_slot();
    engine.set_gc_watermark(snapshot_slot, safe_slot);
    let merge_gc_interval_ms = group.election_config().merge_gc_interval_ms;
    if merge_gc_interval_ms > 0 {
        let now_ms = crate::common::time::instant_to_anchor_ms(std::time::Instant::now());
        let last_compact_ms = group.last_merge_gc_time_ms.load(Ordering::Acquire);
        if now_ms.saturating_sub(last_compact_ms) >= merge_gc_interval_ms {
            let compact_start = std::time::Instant::now();
            let engine_arc = group.local_replica().learner.engine_arc();
            let span = Span::current();
            match tokio::task::spawn_blocking(move || span.in_scope(|| engine_arc.compact_sparse_blocks()))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    error!(error, "maintenance: compact_sparse_blocks failed");
                }
                Err(error) => {
                    error!(
                        %error,
                        "maintenance: compact_sparse_blocks blocking task panicked"
                    );
                }
            }
            group.last_merge_gc_time_ms.store(
                crate::common::time::instant_to_anchor_ms(std::time::Instant::now()),
                Ordering::Release,
            );
            debug!(
                elapsed_ms = u64::try_from(compact_start.elapsed().as_millis()).unwrap_or(u64::MAX),
                "maintenance: block compaction phase complete"
            );
        }
    }

    // 5. WAL GC: only advance snapshot_slot when we actually persisted.
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
            error = %e,
            "engine maintenance: wal gc pass failed"
        );
    }
}
