// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Paxos module-unit tests.
//!
//! Entrypoint for isolated unit tests of the consensus roles in `paxos/`:
//! the acceptor promise/accept fence, the learner note-chosen path, and the
//! error classifier. Layered consensus behaviour lives in the `replica`,
//! `group`, and `store` test binaries; the slot list has its own `slot`
//! subsystem binary.

#[path = "paxos_test/acceptor_test.rs"]
mod acceptor;

#[path = "paxos_test/learner_test.rs"]
mod learner;

#[path = "paxos_test/learner_dedup_test.rs"]
mod learner_dedup;

#[path = "paxos_test/learner_async_test.rs"]
mod learner_async;

#[path = "paxos_test/roles_test.rs"]
mod roles;

#[path = "paxos_test/error_test.rs"]
mod error;
