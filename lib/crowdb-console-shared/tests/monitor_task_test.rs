// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `MonitorTask` integration test against three in-process fake servers.
//!
//! Brings up three mini axum HTTP servers each exposing the legacy
//! `/health` + `/topology` shape, spawns the monitor at a 250 ms ping
//! interval, asserts every node becomes `Up`, then shuts one of them
//! down and asserts the cache reflects the change within roughly 2×
//! ping intervals.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{routing::get, Json, Router};
use crowdb_console_shared::cluster::NodeHealth;
use crowdb_console_shared::monitor::{spawn, MonitorConfig, ProbeTarget};
use serde_json::json;

struct FakeServer {
    addr: SocketAddr,
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

async fn start_fake(node_id: u64, store_id: u64) -> FakeServer {
    let id = node_id;
    let app = Router::new()
        .route(
            "/health",
            get(|| async { Json(json!({ "status": "ok", "messages": [] })) }),
        )
        .route(
            "/topology",
            get(move || {
                let id = id;
                async move {
                    Json(json!({
                        "stores": [{
                            "store_id": store_id,
                            "listen_addr": "127.0.0.1:0",
                            "groups": [{
                                "group_id": 7u64,
                                "local_replica_id": match id { 1 => 100u64, 2 => 200, _ => 300 },
                                "leader_id": 100u64,
                                "force_classic": false,
                                "local_replica": {
                                    "id": match id { 1 => 100u64, 2 => 200, _ => 300 },
                                    "role": if id == 1 { "Leader" } else { "Follower" },
                                    "voting": true,
                                    "kv_store": { "key_count": 0 }
                                },
                                "remotes": []
                            }]
                        }]
                    }))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    FakeServer {
        addr,
        shutdown: tx,
        handle,
    }
}

async fn wait_for<F>(deadline: Duration, mut check: F) -> bool
where
    F: FnMut() -> futures::future::BoxFuture<'static, bool>,
{
    let start = tokio::time::Instant::now();
    while start.elapsed() < deadline {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn monitor_tracks_three_nodes_and_marks_one_down() {
    let s1 = start_fake(1, 1).await;
    let s2 = start_fake(2, 1).await;
    let s3 = start_fake(3, 1).await;

    let targets = vec![
        ProbeTarget {
            node_id: 1,
            mgmt_url: format!("http://{}", s1.addr),
        },
        ProbeTarget {
            node_id: 2,
            mgmt_url: format!("http://{}", s2.addr),
        },
        ProbeTarget {
            node_id: 3,
            mgmt_url: format!("http://{}", s3.addr),
        },
    ];
    let handle = spawn(
        targets,
        MonitorConfig {
            ping_interval: Duration::from_millis(250),
            probe_timeout: Duration::from_millis(500),
        },
    );
    let cache = Arc::clone(handle.cache());

    // Every node should reach Up well within 2× ping interval after the
    // initial tick (which fires immediately).
    let c = Arc::clone(&cache);
    let all_up = wait_for(Duration::from_secs(3), move || {
        let c = Arc::clone(&c);
        Box::pin(async move {
            let snap = c.snapshot().await;
            snap.len() == 3 && snap.values().all(|r| r.health == NodeHealth::Up)
        })
    })
    .await;
    assert!(
        all_up,
        "expected all three nodes up; cache: {:?}",
        cache.snapshot().await
    );

    // Resolve group should aggregate all 3 replicas and pick replica 100 as leader.
    let view = cache.resolve_group(1, 7).await.expect("group exists");
    assert_eq!(view.replicas.len(), 3);
    assert_eq!(view.leader_id(), Some(100));

    // Tear n2 down; cache should mark it Down within ~2× ping interval.
    let _ = s2.shutdown.send(());
    let _ = s2.handle.await;
    let c = Arc::clone(&cache);
    let n2_down = wait_for(Duration::from_secs(3), move || {
        let c = Arc::clone(&c);
        Box::pin(async move {
            let snap = c.snapshot().await;
            snap.get(&2).is_some_and(|r| r.health == NodeHealth::Down)
        })
    })
    .await;
    assert!(
        n2_down,
        "n2 never marked Down; cache: {:?}",
        cache.snapshot().await
    );

    // n1 and n3 should still be Up.
    let snap = cache.snapshot().await;
    assert_eq!(snap[&1].health, NodeHealth::Up);
    assert_eq!(snap[&3].health, NodeHealth::Up);

    handle.shutdown();
    let _ = s1.shutdown.send(());
    let _ = s3.shutdown.send(());
    let _ = s1.handle.await;
    let _ = s3.handle.await;
}

#[tokio::test]
async fn invalidate_triggers_out_of_cycle_refresh() {
    let s1 = start_fake(1, 1).await;
    // Use a very long ping interval so only the initial tick + explicit
    // invalidate would touch the node.
    let handle = spawn(
        vec![ProbeTarget {
            node_id: 1,
            mgmt_url: format!("http://{}", s1.addr),
        }],
        MonitorConfig {
            ping_interval: Duration::from_secs(60),
            probe_timeout: Duration::from_millis(500),
        },
    );
    let cache = Arc::clone(handle.cache());

    let c = Arc::clone(&cache);
    let up = wait_for(Duration::from_secs(3), move || {
        let c = Arc::clone(&c);
        Box::pin(async move {
            c.snapshot()
                .await
                .get(&1)
                .is_some_and(|r| r.health == NodeHealth::Up)
        })
    })
    .await;
    assert!(up);

    // Drop the cache entry manually and confirm invalidate refills it.
    cache.drop_node(&1).await;
    assert!(!cache.snapshot().await.contains_key(&1));

    handle.invalidate(1u64);
    let c = Arc::clone(&cache);
    let refilled = wait_for(Duration::from_secs(3), move || {
        let c = Arc::clone(&c);
        Box::pin(async move {
            c.snapshot()
                .await
                .get(&1)
                .is_some_and(|r| r.health == NodeHealth::Up)
        })
    })
    .await;
    assert!(refilled, "invalidate should refresh n1");

    handle.shutdown();
    let _ = s1.shutdown.send(());
    let _ = s1.handle.await;
}
