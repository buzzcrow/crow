// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology status tests for `PxKvStore::status()`. Covers composition
//! of per-layer statuses, cheap kv-store stats, and per-remote metrics surface.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::group_election::LeaderElection;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv::cluster::remote_replica::PxRemoteReplica;
use crowdb_kv::cluster::replica::{HeartbeatRequestPayload, ReplicaClient};
use crowdb_kv::cluster::KvServer;
use crowdb_kv::common::config::PxElectionConfig;
use crowdb_kv::metrics::{MetricPoint, MetricsRegistry};
use crowdb_kv::rpc::PxRpcTransport;

#[test]
fn status_empty_store() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let snap = store.status();
    assert_eq!(snap.store_id, 1);
    assert!(snap.groups.is_empty());
}

#[test]
fn status_single_group_no_remotes() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        7,
        PxLocalReplica::new(3, PxLocalReplicaRole::Leader),
    ));
    let snap = store.status();
    assert_eq!(snap.groups.len(), 1);
    let g = &snap.groups[0];
    assert_eq!(g.group_id, 7);
    assert_eq!(g.local_replica.id, 3);
    assert_eq!(g.local_replica.role, "leader");
    assert!(g.local_replica.voting);
    assert!(g.remotes.is_empty());
}

#[test]
fn status_with_remote_omits_unregistered_metrics() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:65500".to_string()));
    store.add_group(group);
    let snap = store.status();
    let g = &snap.groups[0];
    assert_eq!(g.remotes.len(), 1);
    let r = &g.remotes[0];
    assert_eq!(r.id, 2);
    assert_eq!(r.endpoint, "127.0.0.1:65500");
    assert!(r.metrics.is_none());
}

/// A live peer store so a heartbeat round-trip succeeds; a second remote
/// points at a closed port so its RPC fails. Two successful heartbeats and
/// one failed one must show up in each remote's `RemoteStatus.metrics` under
/// the exact registry names, counted once, and must not shift on re-read.
#[tokio::test]
async fn remote_status_reports_rpc_success_and_error_totals_by_exact_name() {
    let _net = crate::common::net_lock::lock().await;

    let peer = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    let mut peer_group = PxGroup::new(1, PxLocalReplica::new(2, PxLocalReplicaRole::Follower));
    peer_group.set_election_config(PxElectionConfig {
        election_driver_disabled: true,
        ..PxElectionConfig::DEFAULT
    });
    peer.add_group(peer_group);
    peer.start().await.expect("peer store should start");
    let peer_endpoint = peer.listen_addr().expect("peer started").to_string();

    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let mut store = PxKvStore::new(1, "127.0.0.1:0".parse().unwrap());
    store.set_metrics_registry(registry.clone());
    let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    group.set_election_config(PxElectionConfig {
        election_driver_disabled: true,
        ..PxElectionConfig::DEFAULT
    });
    let live = PxRemoteReplica::new(2, peer_endpoint);
    live.set_rpc_transport(Arc::new(PxRpcTransport::new()));
    let dead = PxRemoteReplica::new(3, format!("127.0.0.1:{}", crate::common::net_lock::unique_port()));
    dead.set_rpc_transport(Arc::new(PxRpcTransport::new()));
    group.add_remote_replica(live);
    group.add_remote_replica(dead);
    store.add_group(group);
    let store = Arc::new(store);
    let group = store.get_group(1).expect("group");
    let live = group.get_remote_replica(2).expect("live remote");
    let dead = group.get_remote_replica(3).expect("dead remote");

    let hb = HeartbeatRequestPayload {
        term: 1,
        leader_id: 1,
        prev_log_slot: 0,
        prev_log_term: 0,
        committed_safe_slot: 0,
        lease_grant_until_ms_mono: 0,
        t_send_ms_mono: 0,
    };
    for _ in 0..2 {
        live.send_heartbeat(hb, 1).await.expect("heartbeat to live peer");
    }
    assert!(
        dead.send_heartbeat(hb, 1).await.is_err(),
        "heartbeat to a closed port must fail"
    );

    let snap = store.status();
    let remotes = &snap.groups[0].remotes;
    let live_status = remotes.iter().find(|r| r.id == 2).expect("live remote present");
    let live_metrics = live_status
        .metrics
        .as_ref()
        .expect("successful RPC totals must appear");
    assert_eq!(
        live_metrics.rpc_count,
        Some(2),
        "two successful heartbeats, counted once"
    );
    assert_eq!(live_metrics.err_count, Some(0));
    let dead_status = remotes.iter().find(|r| r.id == 3).expect("dead remote present");
    let dead_metrics = dead_status
        .metrics
        .as_ref()
        .expect("failed RPC total must appear");
    // The latency summary is registered up-front when the registry is wired,
    // so zero successful RPCs show as Some(0), not None — the curated view
    // reports the registered value verbatim.
    assert_eq!(
        dead_metrics.rpc_count,
        Some(0),
        "summary registered, zero observations"
    );
    assert_eq!(dead_metrics.err_count, Some(1));

    assert_registry_rpc_totals(&registry);

    // Re-reading status neither resets nor accumulates the totals.
    let again = store.status();
    let live_again = again.groups[0]
        .remotes
        .iter()
        .find(|r| r.id == 2)
        .unwrap()
        .metrics
        .as_ref()
        .unwrap();
    assert_eq!(live_again.rpc_count, Some(2));
    assert_eq!(live_again.err_count, Some(0));

    peer.stop();
    peer.join().await;
}

/// Registry-level cross-check for `remote_status_reports_rpc_*`: the live
/// peer's summary has 2 observations and 0 errors; the dead peer's summary
/// is registered with 0 observations and 1 error. All four names exist
/// because `set_metrics_registry` pre-registers both handles per peer.
fn assert_registry_rpc_totals(registry: &Mutex<MetricsRegistry>) {
    let reg = registry.lock().unwrap();
    assert!(matches!(
        reg.snapshot_named("s.1.g.1.rpc.l@2", 1.0),
        Some(MetricPoint::Summary { total: 2, .. })
    ));
    assert!(matches!(
        reg.snapshot_named("s.1.g.1.rpc.errors.c@3", 1.0),
        Some(MetricPoint::Counter { total: 1, .. })
    ));
    // Dead remote's summary is registered with zero observations.
    assert!(matches!(
        reg.snapshot_named("s.1.g.1.rpc.l@3", 1.0),
        Some(MetricPoint::Summary { total: 0, .. })
    ));
    // Live remote's error counter is registered with zero observations.
    assert!(matches!(
        reg.snapshot_named("s.1.g.1.rpc.errors.c@2", 1.0),
        Some(MetricPoint::Counter { total: 0, .. })
    ));
}

/// Registry wired after the group was added: the status metrics are never
/// registered, so the curated view must omit them and the exact-name
/// lookups behind the status path must not register them as a side effect.
#[test]
fn status_lookup_omits_missing_metrics_and_does_not_register_them() {
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let mut store = PxKvStore::new(1, "127.0.0.1:0".parse().unwrap());
    let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    group.add_remote_replica(PxRemoteReplica::new(2, "127.0.0.1:65500".to_string()));
    store.add_group(group);
    store.set_metrics_registry(registry.clone());

    let snap = store.status();
    let g = &snap.groups[0];
    let election = g
        .local_replica
        .election
        .as_ref()
        .expect("local replica always carries election state");
    assert_eq!(election.election_count, None, "unregistered counter is omitted");
    assert_eq!(election.step_downs_admin, None);
    assert_eq!(election.step_downs_higher_term, None);
    assert_eq!(election.step_downs_lease_unrenewable, None);
    assert!(
        g.remotes[0].metrics.is_none(),
        "unregistered remote metrics are omitted"
    );

    let reg = registry.lock().unwrap();
    assert!(
        reg.snapshot("").is_empty(),
        "status lookup must not register metrics"
    );
    assert!(reg.snapshot_named("s.1.g.1.paxos.elections.c", 1.0).is_none());
    assert!(reg.snapshot_named("s.1.g.1.rpc.l@2", 1.0).is_none());
    assert!(reg.snapshot_named("s.1.g.1.rpc.errors.c@2", 1.0).is_none());
}

/// The election driver's canonical step-down sequence bumps the admin
/// step-down counter exactly once, and the topology view mirrors the
/// registry snapshot by exact name.
#[tokio::test]
async fn topology_view_carries_registry_step_down_counters_once() {
    let _net = crate::common::net_lock::lock().await;

    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let mut store = PxKvStore::new(1, "127.0.0.1:0".parse().unwrap());
    store.set_metrics_registry(registry.clone());
    store.add_group(PxGroup::new(
        1,
        PxLocalReplica::new(1, PxLocalReplicaRole::Follower),
    ));
    let store = Arc::new(store);

    // Single-voter group: the driver self-elects (quorum 1).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if store.get_group(1).expect("group").local_replica().is_leader() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no self-election within 10s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let before = store.status();
    let before_view = before.groups[0]
        .local_replica
        .election
        .expect("election view present");
    assert_eq!(
        before_view.election_count,
        Some(1),
        "one election bumped the registry once"
    );
    assert_eq!(before_view.step_downs_admin, Some(0));
    assert_eq!(before_view.step_downs_higher_term, Some(0));
    assert_eq!(before_view.step_downs_lease_unrenewable, Some(0));
    assert_eq!(before.groups[0].local_replica.role, "leader");

    let reply = store.get_group(1).unwrap().step_down_if_leader("metrics test");
    assert!(reply.accepted, "leader must accept its own admin step-down");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let after = loop {
        let snap = store.status();
        let view = snap.groups[0]
            .local_replica
            .election
            .expect("election view present");
        if view.step_downs_admin == Some(1) {
            break snap;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "admin counter not bumped within 2s"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let group = &after.groups[0];
    let view = group.local_replica.election.expect("election view present");
    assert_eq!(group.local_replica.role, "follower");
    assert_eq!(view.election_count, Some(1), "step-down must not bump elections");
    assert_eq!(view.step_downs_higher_term, Some(0));
    assert_eq!(view.step_downs_lease_unrenewable, Some(0));
    assert_eq!(
        view.current_term, before_view.current_term,
        "admin step-down preserves term"
    );
    assert!(view.lease_remaining_ms.is_none(), "follower has no lease");

    // A second step-down is rejected by the strict fence — no second bump.
    let rejected = store.get_group(1).unwrap().step_down_if_leader("again");
    assert!(!rejected.accepted, "already follower → reject");
    let final_view = store.status().groups[0].local_replica.election.unwrap();
    assert_eq!(final_view.step_downs_admin, Some(1));

    let reg = registry.lock().unwrap();
    assert!(matches!(
        reg.snapshot_named("s.1.g.1.paxos.step_downs.admin.c", 1.0),
        Some(MetricPoint::Counter { total: 1, .. })
    ));
    assert!(matches!(
        reg.snapshot_named("s.1.g.1.paxos.elections.c", 1.0),
        Some(MetricPoint::Counter { total: 1, .. })
    ));
}
