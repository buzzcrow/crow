//! Paxos protocol messages exchanged between `PxAcceptor`, proposer, and learner.

use crate::paxos::slot_list::SlotIndex;
use crate::paxos::slot_node::{PxBallot, PxLogEntry};

/// Reply to a Phase-1 `Prepare`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxPrepareReply {
    /// Promise accepted. If the acceptor previously accepted a value at this slot,
    /// the proposer must adopt it (classic Paxos value-recovery rule).
    Promised {
        slot: SlotIndex,
        accepted: Option<PxLogEntry>,
    },
    /// Promise rejected because the slot already promised at a higher-or-equal ballot.
    /// The proposer should retry with a strictly higher ballot.
    Rejected {
        slot: SlotIndex,
        current_promised: PxBallot,
    },
}

/// Reply to a Phase-2 `Accept`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PxAcceptReply {
    /// Value accepted at the entry's `(slot, ballot)`.
    Accepted { slot: SlotIndex, ballot: PxBallot },
    /// Rejected because the slot promised a higher ballot.
    Rejected {
        slot: SlotIndex,
        current_promised: PxBallot,
    },
}
