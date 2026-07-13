//! Paxos module-unit tests.
//!
//! Entrypoint for isolated unit tests of the consensus roles in `paxos/`:
//! the acceptor promise/accept fence, the learner note-chosen path, and the
//! error classifier. Layered consensus behaviour lives in the `replica`,
//! `group`, and `store` test binaries; the slot list has its own `slot`
//! subsystem binary.

#[path = "paxos/acceptor_test.rs"]
mod acceptor;

#[path = "paxos/learner_test.rs"]
mod learner;

#[path = "paxos/learner_dedup_test.rs"]
mod learner_dedup;

#[path = "paxos/learner_async_test.rs"]
mod learner_async;

#[path = "paxos/roles_test.rs"]
mod roles;

#[path = "paxos/error_test.rs"]
mod error;
