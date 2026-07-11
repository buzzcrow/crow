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
    TransportFailure {
        phase: PxPaxosPhase,
        message: String,
    },
    Busy,
    /// Stale leader detected by a peer (peer's `current_term > req.term`).
    /// The proposer must step down to follower and adopt `current_term`.
    /// Classified `FailFatal` for the in-flight proposal; the group-level
    /// driver triggers `become_follower(current_term)`. See
    /// `doc/design/design-leader-election.md` §2.3 + §9 term fencing.
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
            Self::QuorumUnavailable { .. } | Self::TransportFailure { .. } => PxRetryAction::RetrySameSlot {
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
            Self::TransportFailure { .. } => "transport_failure",
            Self::Busy => "busy",
            Self::TermStale { .. } => "term_stale",
            Self::LeaseUnrenewable => "lease_unrenewable",
            Self::InternalInvariantViolation { .. } => "internal_invariant_violation",
        }
    }
}
