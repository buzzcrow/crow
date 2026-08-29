// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::missing_fields_in_debug)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tracing::warn;

use crate::cluster::group::PxGroup;
use crate::common::config::AdmissionPolicy;
use crate::metrics::{Counter, LatencySummary};
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::PxNodeId;

/// Inflight proposal admission gate. Owns a single semaphore of
/// `max_inflight` permits and supports both fail-fast (Reject) and
/// blocking (Queue) admission policies.
pub(crate) struct InflightAdmission {
    pub(crate) semaphore: tokio::sync::Semaphore,
    pub(crate) window: usize,
    pub(crate) policy: AdmissionPolicy,
    /// Cumulative count of proposals that entered the queue (did not
    /// get a fast-path permit).
    pub(crate) total_enqueued: AtomicU64,
    /// Cumulative wait time in microseconds.
    pub(crate) total_wait_us: AtomicU64,
    /// Current number of proposals waiting on `acquire().await`.
    pub(crate) waiting: AtomicU64,
    /// Registry handles for metrics-log export of window-full events.
    pub(crate) handles: OnceLock<InflightRegistryHandles>,
}

/// Metrics handles for inflight admission events.
pub(crate) struct InflightRegistryHandles {
    /// Counter: total proposals that hit the slow path (window full).
    pub(crate) enqueued: Arc<Counter>,
    /// Summary: wait time in microseconds for queued proposals.
    pub(crate) wait_us: Arc<LatencySummary>,
}

impl InflightAdmission {
    pub(crate) fn new(max_inflight: usize, policy: AdmissionPolicy) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(max_inflight),
            window: max_inflight,
            policy,
            total_enqueued: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
            waiting: AtomicU64::new(0),
            handles: OnceLock::new(),
        }
    }

    /// Total permits.
    pub(crate) fn total_permits(&self) -> usize {
        self.window
    }

    /// Currently occupied permits.
    pub(crate) fn occupied(&self) -> u64 {
        let avail = self.semaphore.available_permits();
        u64::try_from(self.window.saturating_sub(avail)).unwrap_or(0)
    }

    /// Acquire a permit. Returns `None` if Reject mode and the window is
    /// full. In Queue mode, blocks until a permit is available.
    pub(crate) async fn acquire_permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        // Fast path: try to acquire without blocking.
        if let Ok(permit) = self.semaphore.try_acquire() {
            return Some(permit);
        }
        // Slow path depends on policy.
        match self.policy {
            AdmissionPolicy::Reject => None,
            AdmissionPolicy::Queue => {
                self.total_enqueued.fetch_add(1, Ordering::Relaxed);
                self.waiting.fetch_add(1, Ordering::Relaxed);
                let t0 = std::time::Instant::now();
                let permit = self.semaphore.acquire().await.expect("inflight semaphore closed");
                let wait_us = t0.elapsed().as_micros();
                self.waiting.fetch_sub(1, Ordering::Relaxed);
                self.total_wait_us
                    .fetch_add(u64::try_from(wait_us).unwrap_or(u64::MAX), Ordering::Relaxed);
                if let Some(h) = self.handles.get() {
                    h.enqueued.inc();
                    h.wait_us.observe(u64::try_from(wait_us).unwrap_or(u64::MAX));
                }
                Some(permit)
            }
        }
    }

    /// Try to acquire all permits (test helper).
    #[cfg(feature = "test-util")]
    pub(crate) fn try_acquire_all(&self) -> Vec<tokio::sync::SemaphorePermit<'_>> {
        let mut held = Vec::new();
        while let Ok(p) = self.semaphore.try_acquire() {
            held.push(p);
        }
        held
    }
}

impl std::fmt::Debug for InflightAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightAdmission")
            .field("window", &self.window)
            .field("policy", &self.policy)
            .field("occupied", &self.occupied())
            .field("waiting", &self.waiting.load(Ordering::Relaxed))
            .finish()
    }
}

/// Logging context for a remote reply fold. The per-remote `EpochMismatch`
/// arm emits a `warn!` with peer attribution, so the fold method needs the
/// fields that vary per remote.
pub(crate) struct RemoteFoldCtx {
    pub(crate) group_id: u64,
    pub(crate) slot: u64,
    pub(crate) remote_id: PxNodeId,
    pub(crate) proposer_epoch: u64,
    /// "prepare" or "accept" — selects the warn message text.
    pub(crate) phase: &'static str,
}

/// Accumulator for the prepare/accept reply fold. Replaces the triplicated
/// inline `match` over `PxPrepareReply` / `PxAcceptReply` in
/// `run_prepare_phase` and `run_accept_phase` (both R16a and R16b paths).
///
/// `accepted` counts promises (prepare) or accepts (accept) from voting
/// replicas only. `local_folded` is set once the local reply has been
/// observed (Ok or Err) so the E1 quorum short-circuit can gate
/// `Chosen`/`Proceed` on the local reply being counted first (W6).
pub(crate) struct ReplyFold {
    pub(crate) accepted: usize,
    pub(crate) highest_rejected_round: Option<u64>,
    pub(crate) highest_seen_term: Option<u64>,
    pub(crate) epoch_mismatch: Option<u64>,
    pub(crate) adopted: Option<PxLogEntry>,
    pub(crate) local_folded: bool,
}

impl ReplyFold {
    pub(crate) fn new() -> Self {
        Self {
            accepted: 0,
            highest_rejected_round: None,
            highest_seen_term: None,
            epoch_mismatch: None,
            adopted: None,
            local_folded: false,
        }
    }

    pub(crate) fn note_rejected(&mut self, current_promised: PxBallot) {
        let candidate = current_promised.round;
        self.highest_rejected_round = Some(
            self.highest_rejected_round
                .map_or(candidate, |r| r.max(candidate)),
        );
    }

    pub(crate) fn note_term_stale(&mut self, new_term: u64) {
        self.highest_seen_term = Some(self.highest_seen_term.map_or(new_term, |t| t.max(new_term)));
    }

    /// Fold the local prepare reply. `EpochMismatch` is unreachable on the
    /// local acceptor.
    pub(crate) fn fold_prepare_local(&mut self, voting: bool, reply: PxPrepareReply) {
        match reply {
            PxPrepareReply::Promised { accepted, .. } => {
                if voting {
                    self.accepted += 1;
                }
                if let Some(prev) = accepted {
                    PxGroup::consider_accepted(&mut self.adopted, prev);
                }
            }
            PxPrepareReply::Rejected { current_promised, .. } => {
                self.note_rejected(current_promised);
            }
            PxPrepareReply::TermStale { new_term, .. } => {
                self.note_term_stale(new_term);
            }
            PxPrepareReply::EpochMismatch { .. } => {
                unreachable!("local on_prepare does not produce EpochMismatch")
            }
        }
        self.local_folded = true;
    }

    /// Fold a remote prepare reply. `EpochMismatch` is real and logged with
    /// peer attribution.
    pub(crate) fn fold_prepare_remote(&mut self, voting: bool, ctx: &RemoteFoldCtx, reply: PxPrepareReply) {
        match reply {
            PxPrepareReply::Promised { accepted, .. } => {
                if voting {
                    self.accepted += 1;
                }
                if let Some(prev) = accepted {
                    PxGroup::consider_accepted(&mut self.adopted, prev);
                }
            }
            PxPrepareReply::Rejected { current_promised, .. } => {
                self.note_rejected(current_promised);
            }
            PxPrepareReply::TermStale { new_term, .. } => {
                self.note_term_stale(new_term);
            }
            PxPrepareReply::EpochMismatch { responder_epoch } => {
                warn!(
                    group_id = ctx.group_id,
                    slot = ctx.slot,
                    remote_id = ctx.remote_id,
                    proposer_epoch = ctx.proposer_epoch,
                    responder_epoch,
                    "{} rejected by membership-epoch fence",
                    ctx.phase
                );
                self.epoch_mismatch = Some(responder_epoch);
            }
        }
    }

    /// Fold the local accept reply (R16a `Ok` arm and R16b infallible path
    /// share this — the reply-arm logic is identical). `EpochMismatch` is
    /// unreachable on the local acceptor.
    pub(crate) fn fold_accept_local(&mut self, voting: bool, reply: &PxAcceptReply) {
        match reply {
            PxAcceptReply::Accepted { .. } => {
                if voting {
                    self.accepted += 1;
                }
            }
            PxAcceptReply::Rejected { current_promised, .. } => {
                self.note_rejected(*current_promised);
            }
            PxAcceptReply::TermStale { new_term, .. } => {
                self.note_term_stale(*new_term);
            }
            PxAcceptReply::EpochMismatch { .. } => {
                unreachable!("local on_accept does not produce EpochMismatch")
            }
        }
        self.local_folded = true;
    }

    /// Fold a remote accept reply. `EpochMismatch` is real and logged with
    /// peer attribution.
    pub(crate) fn fold_accept_remote(&mut self, voting: bool, ctx: &RemoteFoldCtx, reply: &PxAcceptReply) {
        match reply {
            PxAcceptReply::Accepted { .. } => {
                if voting {
                    self.accepted += 1;
                }
            }
            PxAcceptReply::Rejected { current_promised, .. } => {
                self.note_rejected(*current_promised);
            }
            PxAcceptReply::TermStale { new_term, .. } => {
                self.note_term_stale(*new_term);
            }
            PxAcceptReply::EpochMismatch { responder_epoch } => {
                warn!(
                    group_id = ctx.group_id,
                    slot = ctx.slot,
                    remote_id = ctx.remote_id,
                    proposer_epoch = ctx.proposer_epoch,
                    responder_epoch,
                    "{} rejected by membership-epoch fence",
                    ctx.phase
                );
                self.epoch_mismatch = Some(*responder_epoch);
            }
        }
    }
}
