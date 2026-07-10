//! Per-slot Paxos machinery: acceptor (P1 M1), proposer / replicator / repair (P1 M3).

pub mod acceptor;
pub mod types;

pub use types::{PxBallot, PxGroupId, PxNodeId, PxSlot, PxTerm};
