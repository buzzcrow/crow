// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use crate::paxos::roles::PxBallot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxPaxosError {
    NotLeader {
        leader_hint: String,
    },
    PrepareRejected {
        promised: PxBallot,
    },
    AcceptRejected {
        promised: PxBallot,
    },
    ForeignValueChosen {
        slot: u64,
    },
    QuorumUnavailable {
        phase: PxPaxosPhase,
    },
    /// At least one voting peer rejected the request because its
    /// `membership_epoch` didn't exactly match the proposer's -- e.g. an
    /// in-flight membership mutation hasn't fully propagated yet.
    /// Distinct from `QuorumUnavailable` purely for diagnostics (same
    /// retry action: same slot, no ballot bump); this is an expected,
    /// bounded, self-healing stall, not a ballot conflict.
    MembershipEpochMismatch {
        responder_epoch: u64,
    },
    TransportFailure {
        phase: PxPaxosPhase,
        message: String,
    },
    Busy,
    /// Stale leader detected by a peer (peer's `current_term > req.term`).
    /// The proposer must step down to follower and adopt `current_term`.
    /// Classified `FailFatal` for the in-flight proposal; the group-level
    /// driver triggers `become_follower(current_term)`. Two-fence rule:
    /// term fencing on both election and Paxos ballot.
    TermStale {
        current_term: u64,
    },
    /// Driver-side step-down trigger: this leader could not renew its lease
    /// (`now - last_quorum_heartbeat_at >= lease_duration`). Not raised by
    /// `propose`; raised by the election driver to convert in-flight proposals
    /// into `NotLeader`.
    LeaseUnrenewable,
    InternalInvariantViolation {
        message: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PxPaxosPhase {
    Prepare,
    Accept,
    Learn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxRetryAction {
    RetrySameSlot {
        min_round: Option<u64>,
        force_prepare: bool,
    },
    RetryNextSlot,
    Redirect {
        leader_hint: String,
    },
    FailRetryable,
    FailFatal,
}

impl PxPaxosError {
    #[must_use]
    pub fn retry_action(&self) -> PxRetryAction {
        match self {
            Self::NotLeader { leader_hint } => PxRetryAction::Redirect {
                leader_hint: leader_hint.clone(),
            },
            Self::PrepareRejected { promised } => PxRetryAction::RetrySameSlot {
                min_round: Some(promised.round),
                force_prepare: true,
            },
            Self::AcceptRejected { promised } => PxRetryAction::RetrySameSlot {
                min_round: Some(promised.round + 1),
                force_prepare: true,
            },
            Self::ForeignValueChosen { .. } => PxRetryAction::RetryNextSlot,
            Self::QuorumUnavailable { .. }
            | Self::TransportFailure { .. }
            | Self::MembershipEpochMismatch { .. } => PxRetryAction::RetrySameSlot {
                min_round: None,
                force_prepare: false,
            },
            Self::Busy => PxRetryAction::FailRetryable,
            Self::TermStale { .. } | Self::LeaseUnrenewable | Self::InternalInvariantViolation { .. } => {
                PxRetryAction::FailFatal
            }
        }
    }

    #[must_use]
    pub fn keyword(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "not_leader",
            Self::PrepareRejected { .. } => "prepare_rejected",
            Self::AcceptRejected { .. } => "accept_rejected",
            Self::ForeignValueChosen { .. } => "foreign_value_chosen",
            Self::QuorumUnavailable { .. } => "quorum_unavailable",
            Self::MembershipEpochMismatch { .. } => "membership_epoch_mismatch",
            Self::TransportFailure { .. } => "transport_failure",
            Self::Busy => "busy",
            Self::TermStale { .. } => "term_stale",
            Self::LeaseUnrenewable => "lease_unrenewable",
            Self::InternalInvariantViolation { .. } => "internal_invariant_violation",
        }
    }
}
