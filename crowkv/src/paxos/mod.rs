//! Per-slot Paxos machinery: acceptor (P1 M1), minimal proposer + RPC integration (P1 M2),
//! full proposer / replicator / repair (P1 M4).

pub type PxNodeId = u64;
pub type PxGroupId = u64;

/// Election term, monotonically increasing across the cluster.
///
/// `0` is the pre-election sentinel (no leader has been elected yet). The
/// election driver bumps the term once on every `become_candidate` transition.
/// `term` fences stale-leader `Accept` / `Prepare` requests under the
/// `(term, ballot)` two-fence rule.
pub type PxTerm = u64;

pub mod acceptor;
pub mod error;
pub mod slot_list;
pub mod slot_node;

pub mod learner;
pub mod roles;

pub use roles::PxLogEntry;
