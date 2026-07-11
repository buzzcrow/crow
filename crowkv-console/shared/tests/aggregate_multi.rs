//! C2: aggregator over multiple servers; kill one and assert the surviving
//! server is still reported with one error entry.

use std::net::SocketAddr;

use axum::{routing::get, Json, Router};
use crowkv_console_shared::topology::aggregate;
use serde_json::json;

async fn spawn_fake() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/health", get(|| async { Json(json!({ "status": "ok", "messages": [] })) }))
        .route("/topology", get(|| async { Json(json!({ "stores": [] })) }));

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, handle)
}

#[tokio::test]
async fn aggregate_two_servers_one_killed() {
    let (a_addr, a_handle) = spawn_fake().await;
    let (b_addr, b_handle) = spawn_fake().await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Kill the second server before aggregating.
    b_handle.abort();
    // Give the OS a moment to release the listener and force connection refused.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let urls = vec![format!("http://{a_addr}"), format!("http://{b_addr}")];
    let snap = aggregate(&urls).await.unwrap();

    assert_eq!(snap.servers.len(), 2, "snapshot must include both inputs");
    assert!(snap.servers[0].error.is_none(), "first server should be ok: {:?}", snap.servers[0].error);
    assert!(snap.servers[1].error.is_some(), "second server should be marked errored");

    // Order must match input.
    assert_eq!(snap.servers[0].mgmt_url, urls[0]);
    assert_eq!(snap.servers[1].mgmt_url, urls[1]);

    a_handle.abort();
}
