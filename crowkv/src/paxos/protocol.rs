//! Paxos protocol messages exchanged between `PxAcceptor`, proposer, and learner.
//!
//! Request types (`PrepareRequest`, `AcceptRequest`) are the RPC wire-format
//! counterparts to the in-memory handler signatures on `PxAcceptor`.
//! They carry the same semantic data but are struct-shaped for protobuf
//! serialization. See `doc/design/design-rpc.md` §2 for wire field numbers.

use crate::paxos::slot_list::SlotIndex;
use crate::paxos::slot_node::{PxBallot, PxLogEntry};

/// Phase-1 `Prepare` request sent by the proposer (leader) to all acceptors.
///
/// Wire shape (protobuf, field numbers per `design-rpc.md` §2.1):
/// ```text
/// message Prepare {
///   uint32 version = 1;
///   uint64 slot    = 2;
///   uint64 round   = 3;
///   uint64 leader_id = 4;
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareRequest {
    pub version: u32,
    pub slot: SlotIndex,
    pub ballot: PxBallot,
}

/// Phase-2 `Accept` request sent by the proposer after a quorum of promises.
///
/// Wire shape (protobuf, field numbers per `design-rpc.md` §2.3):
/// ```text
/// message Accept {
///   uint32 version = 1;
///   uint64 slot    = 2;
///   uint64 round   = 3;
///   uint64 leader_id = 4;
///   uint64 term    = 5;
///   AcceptedValue value = 6;
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptRequest {
    pub version: u32,
    pub entry: PxLogEntry,
}

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
