//! Per-slot Paxos machinery: acceptor (P1 M1), proposer / replicator / repair (P1 M3).

pub mod acceptor;
pub mod leader;
pub mod protocol;
pub mod slot_list;
pub mod slot_node;

pub use leader::PxTerm;
pub use slot_list::SlotIndex;
pub use slot_node::{PxBallot, PxLogEntry};
