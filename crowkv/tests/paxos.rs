//! Paxos integration tests.
//!
//! This file serves as the entrypoint for all Paxos-related integration tests,
//! which are organized as submodules in the `paxos/` directory.

#[path = "testkit/mod.rs"]
mod testkit;

#[path = "paxos/acceptor.rs"]
mod acceptor;

#[path = "paxos/kv_slot_retry.rs"]
mod kv_slot_retry;

#[path = "paxos/learner_note_chosen.rs"]
mod learner_note_chosen;

#[path = "paxos/election_metrics.rs"]
mod election_metrics;

#[path = "paxos/paxos_error.rs"]
mod paxos_error;

#[path = "paxos/preemption_retry.rs"]
mod preemption_retry;

#[path = "paxos/slot_list.rs"]
mod slot_list;
