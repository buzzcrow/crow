// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Aggregator integration test using a stand-in HTTP server that mimics
//! `crowkv-server`'s `/health` + `/topology` endpoints.

use std::net::SocketAddr;

use axum::{routing::get, Json, Router};
use crowkv_console_shared::topology::aggregate;
use serde_json::json;

async fn fake_server(port_tx: tokio::sync::oneshot::Sender<SocketAddr>) {
    let app = Router::new()
        .route(
            "/health",
            get(|| async {
                Json(json!({
                    "status": "ok",
                    "messages": []
                }))
            }),
        )
        .route(
            "/topology",
            get(|| async {
                Json(json!({
                    "stores": [
                        {
                            "store_id": 1,
                            "listen_addr": "127.0.0.1:55001",
                            "groups": [
                                {
                                    "group_id": 7,
                                    "local_replica_id": 1,
                                    "leader_id": 1,
                                    "force_classic": false,
                                    "local_replica": {
                                        "id": 1,
                                        "role": "Leader",
                                        "voting": true,
                                        "kv_store": { "key_count": 0 }
                                    },
                                    "remotes": []
                                }
                            ]
                        }
                    ]
                }))
            }),
        );

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    port_tx.send(addr).unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tokio::test]
async fn aggregate_one_server() {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(fake_server(tx));
    let addr = rx.await.expect("server addr");

    // Give axum a tick to be ready.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let snapshot = aggregate(&[format!("http://{addr}")]).await.unwrap();

    assert_eq!(snapshot.servers.len(), 1);
    let s = &snapshot.servers[0];
    assert!(s.error.is_none(), "unexpected error: {:?}", s.error);
    assert_eq!(s.health.as_ref().unwrap().status, "ok");
    assert_eq!(s.stores.len(), 1);
    assert_eq!(s.stores[0].store_id, 1);
    assert_eq!(s.stores[0].groups.len(), 1);
    assert_eq!(s.stores[0].groups[0].group_id, 7);

    server.abort();
}

#[tokio::test]
async fn aggregate_unreachable_server_does_not_fail_overall() {
    // Bind a port, drop the listener so nothing answers on it.
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let snapshot = aggregate(&[format!("http://{addr}")]).await.unwrap();
    assert_eq!(snapshot.servers.len(), 1);
    assert!(snapshot.servers[0].error.is_some());
}
