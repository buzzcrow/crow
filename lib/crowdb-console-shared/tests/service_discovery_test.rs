// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E tests for `ServiceDiscoveryClient` against a real kv-server
//! cluster (group-0). Tests cover: registration + discovery, cache
//! TTL, round-robin selection, empty registry, expired instances,
//! invalidate, and `discover_by_endpoint`.

use std::time::Duration;

use crowdb_kv_client::{Error, ServiceDiscoveryClient};
use crowdb_protocol::common::{DiskGroupUsageSummary, DiskdbExtra, InstanceValue, ServiceExtra};
use crowdb_protocol::common_type::InstanceId;
use crowdb_protocol::key::InstanceKey;
use crowdb_test_harness::cluster::KvCluster;

/// Register a diskdb instance and verify it's discoverable.
#[tokio::test]
async fn discover_registered_diskdb_instance() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone());

    svc.register_diskdb(
        1,
        "127.0.0.1:11000",
        &[1, 2],
        &[DiskGroupUsageSummary {
            disk_group_id: 1,
            capacity_bytes: 1000,
            used_bytes: 0,
            free_bytes: 1000,
            disk_count: 3,
            allocatable_disk_count: 3,
        }],
    )
    .await
    .expect("register diskdb");

    let instances = discovery.discover_all("diskdb").await.expect("discover_all");
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].1.rpc_endpoint, "127.0.0.1:11000");
}

/// Cache returns the same result within TTL without re-querying.
#[tokio::test]
async fn cache_hit_within_ttl() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone());

    for i in 1..=3 {
        svc.register_diskdb(i, &format!("127.0.0.1:1100{i}"), &[i], &[])
            .await
            .expect("register");
    }

    let first = discovery.discover_all("diskdb").await.expect("first");
    assert_eq!(first.len(), 3);

    // Unregister one — the cache should still return 3 (stale within TTL).
    let _ = svc.unregister("diskdb", 3).await;
    let cached = discovery.discover_all("diskdb").await.expect("cached");
    assert_eq!(cached.len(), 3, "cache should be stale within TTL");
}

/// After cache invalidation, the next call re-queries group-0.
#[tokio::test]
async fn invalidate_forces_refresh() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone());

    for i in 1..=3 {
        svc.register_diskdb(i, &format!("127.0.0.1:1100{i}"), &[i], &[])
            .await
            .expect("register");
    }

    let first = discovery.discover_all("diskdb").await.expect("first");
    assert_eq!(first.len(), 3);

    let _ = svc.unregister("diskdb", 3).await;
    discovery.invalidate(Some("diskdb"));

    let refreshed = discovery.discover_all("diskdb").await.expect("refreshed");
    assert_eq!(refreshed.len(), 2, "invalidate should force re-query");
}

/// Round-robin selection cycles through all instances.
#[tokio::test]
async fn round_robin_selection() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone());

    for i in 1..=3 {
        svc.register_diskdb(i, &format!("127.0.0.1:1100{i}"), &[i], &[])
            .await
            .expect("register");
    }

    let mut endpoints = std::collections::HashSet::new();
    for _ in 0..3 {
        let instance = discovery.discover_one("diskdb").await.expect("discover_one");
        endpoints.insert(instance.rpc_endpoint);
    }
    assert_eq!(endpoints.len(), 3, "round-robin should return all 3");
}

/// Empty registry returns `NoLivingInstances` for `discover_one`.
#[tokio::test]
async fn empty_registry_returns_error() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc);

    let err = discovery.discover_one("diskdb").await.unwrap_err();
    assert!(
        matches!(err, Error::NoLivingInstances { ref service } if service == "diskdb"),
        "expected `NoLivingInstances`, got {err:?}"
    );
}

/// `discover_by_endpoint` returns the matching instance.
#[tokio::test]
async fn discover_by_endpoint_found() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone());

    svc.register_diskdb(1, "127.0.0.1:11001", &[1], &[])
        .await
        .expect("register");
    svc.register_diskdb(2, "127.0.0.1:11002", &[2], &[])
        .await
        .expect("register");

    let result = discovery
        .discover_by_endpoint("diskdb", "127.0.0.1:11002")
        .await
        .expect("`discover_by_endpoint`");
    assert!(result.is_some());
    assert_eq!(result.unwrap().instance_id, 2);
}

/// `discover_by_endpoint` returns None for a non-existent endpoint.
#[tokio::test]
async fn discover_by_endpoint_not_found() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone());

    svc.register_diskdb(1, "127.0.0.1:11001", &[1], &[])
        .await
        .expect("register");

    let result = discovery
        .discover_by_endpoint("diskdb", "127.0.0.1:99999")
        .await
        .expect("`discover_by_endpoint`");
    assert!(result.is_none());
}

/// Cache TTL = 0 disables caching (every call queries group-0).
#[tokio::test]
async fn zero_ttl_disables_caching() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone()).with_cache_ttl(Duration::from_millis(0));

    svc.register_diskdb(1, "127.0.0.1:11001", &[1], &[])
        .await
        .expect("register");

    let first = discovery.discover_all("diskdb").await.expect("first");
    assert_eq!(first.len(), 1);

    svc.register_diskdb(2, "127.0.0.1:11002", &[2], &[])
        .await
        .expect("register 2");
    let second = discovery.discover_all("diskdb").await.expect("second");
    assert_eq!(second.len(), 2, "TTL=0 should not cache");
}

/// Expired instances (missed heartbeat) are filtered out.
#[tokio::test]
async fn expired_instance_filtered() {
    let cluster = KvCluster::start().await;
    let svc = cluster.make_service_registry_client();
    let discovery = ServiceDiscoveryClient::new(svc.clone()).with_cache_ttl(Duration::from_millis(0));

    // Register an instance with a very old heartbeat (epoch = 0).
    let old_value = InstanceValue {
        instance_id: 1,
        rpc_endpoint: "127.0.0.1:11001".into(),
        last_heartbeat_ms: 0,
        extra: Some(ServiceExtra {
            diskdb: Some(DiskdbExtra {
                owned_dg_ids: vec![1],
                group_usages: vec![],
            }),
            kv_server: None,
        }),
    };
    let kv = svc.kv();
    let key = InstanceKey {
        service: "diskdb".into(),
        instance_id: 1,
    };
    let payload = serde_json::to_vec(&old_value).unwrap();
    kv.put(0, 0, key.to_path().as_bytes(), &payload, None)
        .await
        .expect("put old heartbeat");

    // Register a fresh instance.
    svc.register_diskdb(2, "127.0.0.1:11002", &[2], &[])
        .await
        .expect("register fresh");

    let instances = discovery.discover_all("diskdb").await.expect("discover");
    let live_ids: Vec<InstanceId> = instances.iter().map(|(id, _)| *id).collect();
    assert!(!live_ids.contains(&1), "expired instance should be filtered out");
    assert!(live_ids.contains(&2), "fresh instance should be present");
}
