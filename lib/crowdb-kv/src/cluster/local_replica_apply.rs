// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::local_replica::PxLocalReplica;
use crate::metrics::Counter;
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{Acceptor, DedupTag, Learner, PxBallot, PxLogEntry, SlotIndex};
use crate::paxos::PxTerm;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::trace;

/// R63: bounded batch size for the background apply loop. Limits the number
/// of entries processed in one iteration before re-checking cancellation and
/// `known_commit_slot` advances.
const MAX_APPLY_PER_BATCH: u64 = 64;

/// R65: maximum in-flight `FetchGap` requests per follower. Prevents
/// flooding the leader when the follower has a large gap backlog.
const MAX_INFLIGHT_FETCHGAP: u64 = 16;

impl PxLocalReplica {
    /// Learn a chosen entry (apply to state machine) and record each
    /// dedup tag against the slot. A single-key propose passes one tag;
    /// a coalesced batch passes one per client op; repair/election pass
    /// none.
    pub async fn learn_chosen(&self, entry: &PxLogEntry, dedup_tags: &[DedupTag]) {
        self.learner.learn(entry.clone(), dedup_tags).await;
    }

    /// R17: defer the engine apply (`apply_entry` + applied-frontier
    /// advance) to a detached background task, while advancing the chosen
    /// frontier and recording dedup **synchronously**. Used when
    /// `async_engine_apply` is enabled — the value is already Paxos-chosen,
    /// so the FFI/memtable insert can happen asynchronously.
    ///
    /// The chosen frontier (`contiguous_chosen`) and dedup must advance
    /// before `propose` returns so a subsequent Linearizable read's
    /// `read_slot = contiguous_chosen` reflects this slot — the R35 apply
    /// fence then waits for `contiguous_applied >= read_slot` (the spawned
    /// apply) before serving the read, preserving read-your-writes. The
    /// learner's `apply_entry` is idempotent, and the frontier/dedup
    /// updates are atomic, so a delayed apply is safe.
    pub fn spawn_learn_chosen(&self, entry: PxLogEntry, dedup_tags: &[DedupTag]) {
        let learner = Arc::clone(&self.learner);
        // Sync: chosen frontier + dedup (cheap atomics; must precede
        // `propose` returning `Chosen` for read-your-writes).
        learner.update_chosen_frontier(entry.slot, entry.term);
        learner.record_dedup_tags(dedup_tags, entry.slot);
        // Deferred: engine apply + applied frontier (the FFI/memtable insert
        // moved off the write critical path; the apply fence gates reads).
        // Gap 2: only advance `contiguous_applied` if the apply succeeded.
        tokio::spawn(async move {
            if learner.apply_entry(entry.slot, &entry.payload).await.is_ok() {
                learner.advance_applied_frontier(entry.slot);
            }
        });
    }

    /// R63: advance `known_commit_slot` to at least `slot` via `fetch_max`.
    /// Called by `handle_heartbeat` (with the leader's `committed_safe_slot`)
    /// and by `handle_accept_inner` / `BatchChosen` handler (with the accepted
    /// slot). The background apply loop reads this to know how far it can
    /// apply.
    pub(crate) fn advance_known_commit_slot(&self, slot: SlotIndex) {
        self.known_commit_slot.fetch_max(slot, Ordering::AcqRel);
    }

    /// R63: wake the background apply loop. Also ensures the loop is running
    /// (lazily spawned on first call). Called after `advance_known_commit_slot`
    /// from `handle_heartbeat`, `handle_accept_inner`, and the `BatchChosen`
    /// handler.
    pub(crate) fn wake_apply_loop(&self) {
        self.ensure_apply_loop();
        self.apply_notify.notify_one();
    }

    /// R63: lazily spawn the background apply loop if it hasn't been started
    /// yet. The loop runs for the replica's lifetime (cancelled in
    /// [`PxLocalReplica::shutdown`]); it reads `known_commit_slot`, collects entries
    /// from the acceptor, and applies them via the learner. Missing slots
    /// are skipped (skip-and-continue) so a single gap doesn't block the
    /// entire apply backlog.
    fn ensure_apply_loop(&self) {
        if self.apply_loop_handle.lock().is_some() {
            return;
        }
        let known = Arc::clone(&self.known_commit_slot);
        let learner = Arc::clone(&self.learner);
        let acceptor = Arc::clone(&self.acceptor);
        let notify = Arc::clone(&self.apply_notify);
        let gap_slots = Arc::clone(&self.gap_slots);
        let apply_loop_skip = self.replication_handles.get().map(|h| h.apply_loop_skip.clone());
        let cancel = self.apply_loop_cancel.clone();
        let handle = tokio::spawn(async move {
            apply_loop_task(
                known,
                learner,
                acceptor,
                notify,
                gap_slots,
                apply_loop_skip,
                cancel,
            )
            .await;
        });
        *self.apply_loop_handle.lock() = Some(handle);
    }

    /// R65: record a gap slot that needs `FetchGap`. Called by the
    /// `ChosenNotice` handler (missing/stale value) and the apply loop
    /// (missing slot in committed range). Idempotent.
    pub(crate) fn record_gap(&self, slot: SlotIndex) {
        self.gap_slots.lock().insert(slot);
    }

    /// R65: increment the `chosen_notice.stale_ballot` counter.
    pub(crate) fn incr_chosen_notice_stale(&self) {
        if let Some(h) = self.replication_handles.get() {
            h.chosen_notice_stale_ballot.inc();
        }
    }

    /// R65: increment the `chosen_notice.missing_value` counter.
    pub(crate) fn incr_chosen_notice_missing(&self) {
        if let Some(h) = self.replication_handles.get() {
            h.chosen_notice_missing_value.inc();
        }
    }

    /// R65: increment the `fetchgap.sent` counter by `n`.
    pub(crate) fn incr_fetchgap_sent(&self, n: u64) {
        if let Some(h) = self.replication_handles.get() {
            for _ in 0..n {
                h.fetchgap_sent.inc();
            }
        }
    }

    /// R65: update replication gauges from current state. Called
    /// periodically by the `FetchGap` driver.
    pub(crate) fn update_replication_gauges(&self) {
        if let Some(h) = self.replication_handles.get() {
            h.gap_count.set(self.gap_slots.lock().len() as u64);
            h.fetchgap_inflight
                .set(self.fetchgap_inflight.load(Ordering::Acquire));
            h.known_commit_slot
                .set(self.known_commit_slot.load(Ordering::Acquire));
        }
    }

    /// R65: drain up to `MAX_INFLIGHT_FETCHGAP` gap slots for `FetchGap`
    /// sending. Returns the slots that were drained (removed from the
    /// set). The caller is responsible for sending `FetchGap` for each
    /// and decrementing `fetchgap_inflight` on completion.
    pub(crate) fn drain_gaps_for_fetchgap(&self) -> Vec<SlotIndex> {
        let max = MAX_INFLIGHT_FETCHGAP.saturating_sub(self.fetchgap_inflight.load(Ordering::Acquire));
        if max == 0 {
            return Vec::new();
        }
        #[allow(clippy::cast_possible_truncation)]
        let max_usize = max as usize;
        let mut gaps = self.gap_slots.lock();
        let mut result = Vec::with_capacity(max_usize);
        while result.len() < max_usize {
            if let Some(&slot) = gaps.first() {
                gaps.remove(&slot);
                result.push(slot);
            } else {
                break;
            }
        }
        if !result.is_empty() {
            self.fetchgap_inflight
                .fetch_add(u64::try_from(result.len()).unwrap_or(u64::MAX), Ordering::AcqRel);
        }
        result
    }

    /// R63: stop the background apply loop. Called in [`PxLocalReplica::shutdown`].
    pub(super) fn stop_apply_loop(&self) {
        self.apply_loop_cancel.cancel();
        if let Some(handle) = self.apply_loop_handle.lock().take() {
            handle.abort();
        }
    }

    /// Receive a peer-side `ChosenNotice` for `(slot, term)`.
    ///
    /// Advances the `(last_chosen_slot, last_chosen_term)` high-water mark only
    /// (never the contiguous-chosen / contiguous-applied watermarks, since a
    /// `ChosenNotice` carries no payload to apply). The high-water mark is the
    /// follower's signal that committed slots exist past its applied frontier,
    /// which drives `repair_once` and heartbeat catch-up to fetch the real
    /// values via the background apply loop.
    ///
    /// W7: this used to be neutered (always `false`) because the election
    /// log-up-to-date check read `last_chosen_slot`, so advancing it from a
    /// payload-less notice could let a value-missing replica win leadership
    /// (the missing-key / resurrection bug). The check
    /// now compares the **durable acceptor log tip** instead
    /// ([`PxLocalReplica::candidate_log_up_to_date`] → [`PxLocalReplica::accepted_log_tip`]), so the
    /// notice no longer influences electability and the advance is safe again.
    ///
    /// Returns `true` if the high-water mark advanced.
    pub fn note_chosen(&self, slot: SlotIndex, term: PxTerm) -> bool {
        let advanced = self.learner.note_chosen(slot, term);
        trace!(
            replica_l_id = self.id,
            slot,
            term,
            advanced,
            "note_chosen: advanced chosen high-water mark"
        );
        advanced
    }

    /// Read the currently accepted value at a slot (for verification).
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn accepted_at(&self, slot: u64) -> Option<PxLogEntry> {
        self.acceptor.accepted_at(slot)
    }

    /// Read the currently promised ballot at a slot (for verification).
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn promised_at(&self, slot: u64) -> Option<PxBallot> {
        self.acceptor.promised_at(slot)
    }

    // ---------------- Learner / acceptor frontier accessors ----------------
    //
    // Learner watermarks and acceptor cursor accessors.

    /// Highest slot ever seen as chosen by the local learner (gaps allowed).
    #[must_use]
    pub fn last_chosen_slot(&self) -> SlotIndex {
        self.learner.last_chosen_slot()
    }

    /// Term of the entry at [`PxLocalReplica::last_chosen_slot`].
    #[must_use]
    pub fn last_chosen_term(&self) -> PxTerm {
        self.learner.last_chosen_term()
    }

    /// Highest contiguous-chosen slot.
    #[must_use]
    pub fn contiguous_chosen(&self) -> SlotIndex {
        self.learner.contiguous_chosen()
    }

    /// Highest contiguous-applied slot.
    #[must_use]
    pub fn contiguous_applied(&self) -> SlotIndex {
        self.learner.contiguous_applied()
    }

    /// R35 apply fence: wait until the local applied frontier reaches
    /// `slot`. Delegates to [`PxLearner::await_applied`]. Used by the
    /// Linearizable read path after the leadership barrier so a read does
    /// not return a just-chosen-but-not-yet-applied value when R17
    /// (`async_engine_apply`) is on.
    pub async fn await_apply_fence(&self, slot: SlotIndex) {
        self.learner.await_applied(slot).await;
    }

    /// Highest slot ever opened on this replica's acceptor.
    #[must_use]
    pub fn highest_seen_slot(&self) -> SlotIndex {
        self.acceptor.highest_seen_slot()
    }

    #[must_use]
    pub fn accepted_log_tip(&self) -> (SlotIndex, PxTerm) {
        self.acceptor.accepted_log_tip().unwrap_or((0, 0))
    }

    // ---------------- Election handler internals ----------------

    /// Compute the responder's frontier triple (used by `PreVote` /
    /// `RequestVote` / `Heartbeat` replies).
    pub(super) fn frontier_triple(&self) -> (SlotIndex, PxTerm, SlotIndex) {
        (
            self.contiguous_chosen(),
            self.last_chosen_term(),
            self.highest_seen_slot(),
        )
    }
}

/// R63: background apply loop task. Reads `known_commit_slot`, collects
/// entries from the acceptor for the contiguous range, and applies them via
/// the learner. Missing slots are skipped (skip-and-continue) so a single
/// gap doesn't block the entire apply backlog — `contiguous_applied` stays
/// at the gap until it's filled; subsequent available slots are applied via
/// `advance_applied_frontier` (which handles out-of-order applies).
async fn apply_loop_task(
    known: Arc<AtomicU64>,
    learner: Arc<PxLearner>,
    acceptor: Arc<PxAcceptor>,
    notify: Arc<tokio::sync::Notify>,
    gap_slots: Arc<Mutex<BTreeSet<SlotIndex>>>,
    apply_loop_skip: Option<Arc<Counter>>,
    cancel: tokio_util::sync::CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        // R65: target is max(known_commit_slot, last_chosen_slot).
        // `known_commit_slot` covers the continuous prefix (from
        // heartbeat), `last_chosen_slot` covers out-of-order chosen
        // slots (from ChosenNotice with ballot match).
        let target = known.load(Ordering::Acquire).max(learner.last_chosen_slot());
        let current = learner.contiguous_applied();
        if current >= target {
            notify.notified().await;
            continue;
        }
        let mut next = current.saturating_add(1);
        let end = target.min(next.saturating_add(MAX_APPLY_PER_BATCH).saturating_sub(1));
        let before = current;
        while next <= end {
            if cancel.is_cancelled() {
                return;
            }
            // R65: chosen-ness check — only apply slots that are genuinely
            // chosen. A slot is chosen if any of:
            // - ≤ known_commit_slot (heartbeat's committed_safe_slot =
            //   Raft commit_index: continuous chosen prefix)
            // - ≤ contiguous_chosen (from ChosenNotice/learn)
            // - in the out-of-order chosen set (individual ChosenNotice)
            // Accepted-but-not-chosen slots are skipped. This prevents
            // applying values the leader hasn't confirmed.
            let known_val = known.load(Ordering::Acquire);
            if next <= known_val || learner.is_chosen(next) {
                if let Some(entry) = acceptor.accepted_at(next) {
                    // Gap 2: only advance `contiguous_applied` if the apply succeeded.
                    if learner.apply_entry(entry.slot, &entry.payload).await.is_ok() {
                        learner.advance_applied_frontier(entry.slot);
                    }
                } else {
                    // R65: chosen (≤ known_commit_slot or in chosen set)
                    // but acceptor has no value — record gap for FetchGap.
                    gap_slots.lock().insert(next);
                    if let Some(c) = &apply_loop_skip {
                        c.inc();
                    }
                }
            }
            next += 1;
        }
        // If contiguous_applied didn't advance (stuck at a gap with only
        // out-of-order applies), wait for a wake-up before retrying —
        // avoids busy-looping on gaps that haven't been filled yet.
        if learner.contiguous_applied() == before {
            notify.notified().await;
        }
    }
}
