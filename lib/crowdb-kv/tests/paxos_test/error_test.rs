// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for the `paxos::error` classifier: error keyword mapping and
//! retry-action derivation. Cluster-level error propagation (not-leader hint,
//! preemption retry, crowdb-rpc boundary rejection) lives in the group layer at
//! `tests/group/paxos_error_test.rs`.

use crowdb_kv::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crowdb_kv::paxos::roles::PxBallot;

#[test]
fn paxos_error_classifier_maps_prepare_rejection_to_same_slot_prepare() {
    let error = PxPaxosError::PrepareRejected {
        promised: PxBallot::new(10, 2),
    };

    assert_eq!(error.keyword(), "prepare_rejected");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: Some(10),
            force_prepare: true,
        }
    );
}

#[test]
fn paxos_error_classifier_maps_accept_rejection_to_classic_repair() {
    let error = PxPaxosError::AcceptRejected {
        promised: PxBallot::new(10, 2),
    };

    assert_eq!(error.keyword(), "accept_rejected");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: Some(11),
            force_prepare: true,
        }
    );
}

#[test]
fn paxos_error_classifier_keeps_transport_on_same_slot_without_ballot_bump() {
    let error = PxPaxosError::TransportFailure {
        phase: PxPaxosPhase::Accept,
        message: "timeout".to_string(),
    };

    assert_eq!(error.keyword(), "transport_failure");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: None,
            force_prepare: false,
        }
    );
}

#[test]
fn not_leader_maps_to_redirect() {
    let error = PxPaxosError::NotLeader {
        leader_hint: "node-2".to_string(),
    };
    assert_eq!(error.keyword(), "not_leader");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::Redirect {
            leader_hint: "node-2".to_string(),
        }
    );
}

#[test]
fn foreign_value_chosen_maps_to_retry_next_slot() {
    let error = PxPaxosError::ForeignValueChosen { slot: 42 };
    assert_eq!(error.keyword(), "foreign_value_chosen");
    assert_eq!(error.retry_action(), PxRetryAction::RetryNextSlot);
}

#[test]
fn quorum_unavailable_maps_to_retry_same_slot_no_bump() {
    let error = PxPaxosError::QuorumUnavailable {
        phase: PxPaxosPhase::Prepare,
    };
    assert_eq!(error.keyword(), "quorum_unavailable");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: None,
            force_prepare: false,
        }
    );
}

#[test]
fn membership_epoch_mismatch_maps_to_retry_same_slot_no_bump() {
    let error = PxPaxosError::MembershipEpochMismatch { responder_epoch: 7 };
    assert_eq!(error.keyword(), "membership_epoch_mismatch");
    assert_eq!(
        error.retry_action(),
        PxRetryAction::RetrySameSlot {
            min_round: None,
            force_prepare: false,
        }
    );
}

#[test]
fn busy_maps_to_fail_retryable() {
    let error = PxPaxosError::Busy;
    assert_eq!(error.keyword(), "busy");
    assert_eq!(error.retry_action(), PxRetryAction::FailRetryable);
}

#[test]
fn term_stale_maps_to_fail_fatal() {
    let error = PxPaxosError::TermStale { current_term: 5 };
    assert_eq!(error.keyword(), "term_stale");
    assert_eq!(error.retry_action(), PxRetryAction::FailFatal);
}

#[test]
fn lease_unrenewable_maps_to_fail_fatal() {
    let error = PxPaxosError::LeaseUnrenewable;
    assert_eq!(error.keyword(), "lease_unrenewable");
    assert_eq!(error.retry_action(), PxRetryAction::FailFatal);
}

#[test]
fn internal_invariant_violation_maps_to_fail_fatal() {
    let error = PxPaxosError::InternalInvariantViolation {
        message: "unexpected state".to_string(),
    };
    assert_eq!(error.keyword(), "internal_invariant_violation");
    assert_eq!(error.retry_action(), PxRetryAction::FailFatal);
}
