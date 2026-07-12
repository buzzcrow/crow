//! `PxRemoteReplica` connection failure and error handling tests.

use crowkv::cluster::replica::{PxReplicaError, ReplicaClient};
use crowkv::cluster::PxRemoteReplica;
use crowkv::paxos::roles::PxBallot;

#[tokio::test]
async fn send_prepare_to_unreachable_endpoint_returns_error() {
    // Point at a port that is not listening
    let remote = PxRemoteReplica::new(99, "127.0.0.1:1".to_string());
    let ballot = PxBallot::new(1, 0);

    let result = remote.send_prepare(1, ballot, 0, 1).await;
    assert!(result.is_err(), "should fail when remote is unreachable");
    let err = result.unwrap_err();
    match &err {
        PxReplicaError::Internal(msg) => {
            assert!(
                msg.to_lowercase().contains("unavailable"),
                "expected Unavailable in error: {msg}"
            );
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[tokio::test]
async fn send_accept_to_unreachable_endpoint_returns_error() {
    let remote = PxRemoteReplica::new(99, "127.0.0.1:1".to_string());
    let entry = crowkv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(1, 0),
        term: 0,
        payload: bytes::Bytes::from_static(b"test"),
    };

    let result = remote.send_accept(&entry, None, None, 1).await;
    assert!(result.is_err(), "should fail when remote is unreachable");
    let err = result.unwrap_err();
    match &err {
        PxReplicaError::Internal(msg) => {
            assert!(
                msg.to_lowercase().contains("unavailable"),
                "expected Unavailable in error: {msg}"
            );
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[tokio::test]
async fn send_prepare_to_invalid_endpoint_returns_error() {
    let remote = PxRemoteReplica::new(99, "not-a-real-host:99999".to_string());
    let ballot = PxBallot::new(1, 0);

    let result = remote.send_prepare(1, ballot, 0, 1).await;
    assert!(result.is_err(), "should fail for invalid endpoint");
}
