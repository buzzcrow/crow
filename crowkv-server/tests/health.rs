//! Health-report tests for `PxKvStore::health()`.
//!
//! HTTP /health integration (200 vs 503 status mapping, full hierarchical
//! JSON shape) is covered end-to-end in the real-process test suite.

use std::sync::Arc;
use std::time::Duration;

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::cluster::HealthStatus;

fn make_store_single_replica() -> Arc<PxKvStore> {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    store.add_group(group);
    store
}

#[tokio::test]
async fn health_unhealthy_before_start() {
    // gRPC server has not been started — listen handle absent → Unhealthy.
    let store = make_store_single_replica();
    let report = store.health();
    assert_eq!(report.status, HealthStatus::Unhealthy, "{report:?}");
    assert!(
        report.messages.iter().any(|m| m.contains("not running")),
        "expected 'not running' message, got: {report_messages:?}",
        report_messages = report.messages
    );
}

#[tokio::test]
async fn health_ok_after_start_single_replica() {
    let store = make_store_single_replica();
    assert!(store.start().await);
    let report = store.health();
    // Single voting replica + quorum 1 → Ok.
    assert_eq!(report.status, HealthStatus::Ok, "{report:?}");
}

#[tokio::test]
async fn health_unhealthy_after_shutdown() {
    let store = make_store_single_replica();
    assert!(store.start().await);
    let _ = store.shutdown(Duration::from_secs(2)).await;
    let report = store.health();
    assert_eq!(report.status, HealthStatus::Unhealthy);
    assert!(
        report.messages.iter().any(|m| m.contains("shut down")),
        "expected 'shut down' message, got: {:?}",
        report.messages
    );
}

#[tokio::test]
async fn health_status_worst_of_combinator() {
    use HealthStatus::{Degraded, Ok, Unhealthy};
    assert_eq!(HealthStatus::worst(Ok, Ok), Ok);
    assert_eq!(HealthStatus::worst(Ok, Degraded), Degraded);
    assert_eq!(HealthStatus::worst(Degraded, Ok), Degraded);
    assert_eq!(HealthStatus::worst(Ok, Unhealthy), Unhealthy);
    assert_eq!(HealthStatus::worst(Degraded, Unhealthy), Unhealthy);
    assert_eq!(HealthStatus::worst(Unhealthy, Unhealthy), Unhealthy);
}
