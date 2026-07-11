//! Paxos integration tests.
//!
//! This file serves as the entrypoint for all Paxos-related integration tests,
//! which are organized as submodules in the `paxos/` directory.

#[path = "testkit/mod.rs"]
mod testkit;

#[path = "paxos/acceptor_test.rs"]
mod acceptor;

#[path = "paxos/kv_slot_retry_test.rs"]
mod kv_slot_retry;

#[path = "paxos/learner_note_chosen_test.rs"]
mod learner_note_chosen;

#[path = "paxos/election_metrics_test.rs"]
mod election_metrics;

#[path = "paxos/election_test.rs"]
mod election;

#[path = "paxos/paxos_error_test.rs"]
mod paxos_error;

#[path = "paxos/preemption_retry_test.rs"]
mod preemption_retry;

#[path = "paxos/slot_list_test.rs"]
mod slot_list;
