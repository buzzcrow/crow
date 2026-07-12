//! Unit tests for the `paxos::error` classifier: error keyword mapping and
//! retry-action derivation. Cluster-level error propagation (not-leader hint,
//! preemption retry, gRPC boundary rejection) lives in the group layer at
//! `tests/group/paxos_error_test.rs`.

use crowkv::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crowkv::paxos::roles::PxBallot;

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
