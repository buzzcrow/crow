//! Per-slot Paxos machinery: acceptor (P1 M1), minimal proposer + RPC integration (P1 M2),
//! full proposer / replicator / repair (P1 M4).

pub type PxNodeId = u64;
pub type PxGroupId = u64;

pub mod acceptor;
pub mod error;
pub mod slot_list;
pub mod slot_node;

pub mod learner;
pub mod roles;

pub use roles::PxLogEntry;
