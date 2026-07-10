use crate::paxos::roles::Ballot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxPaxosError {
    NotLeader {
        leader_hint: String,
    },
    PrepareRejected {
        promised: Ballot,
    },
    AcceptRejected {
        promised: Ballot,
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
            Self::QuorumUnavailable { .. } | Self::TransportFailure { .. } => {
                PxRetryAction::RetrySameSlot {
                    min_round: None,
                    force_prepare: false,
                }
            }
            Self::Busy => PxRetryAction::FailRetryable,
            Self::InternalInvariantViolation { .. } => PxRetryAction::FailFatal,
        }
    }

    pub fn keyword(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "not_leader",
            Self::PrepareRejected { .. } => "prepare_rejected",
            Self::AcceptRejected { .. } => "accept_rejected",
            Self::ForeignValueChosen { .. } => "foreign_value_chosen",
            Self::QuorumUnavailable { .. } => "quorum_unavailable",
            Self::TransportFailure { .. } => "transport_failure",
            Self::Busy => "busy",
            Self::InternalInvariantViolation { .. } => "internal_invariant_violation",
        }
    }
}
