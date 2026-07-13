//! A2: `?recursive=` validation is honoured by every two-tree GET.
//!
//! The handlers currently embed their natural immediate children
//! inline (e.g. `GroupView` carries `replicas`), so for now the
//! value-add of the extractor is **validation**: malformed or
//! out-of-range values surface as `400 Validation` instead of being
//! silently ignored. Deeper `Expandable` walks land per-handler as
//! needed.
//!
//! This test fires every GET endpoint against a fresh `crowkv-web`
//! with `?recursive=nope` and asserts the response is a 400 with the
//! `not an integer or "all"` body produced by
//! `crowkv_console_shared::expand::RecursiveDepth::parse`.

use std::net::SocketAddr;
use std::time::Duration;

use crowkv_web::{router, AppState};

async fn spawn_web() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(vec!["http://127.0.0.1:1".into()]);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn malformed_recursive_yields_400_on_every_get() {
    let addr = spawn_web().await;
    let http = reqwest::Client::new();

    // Every GET under the two-tree contract. Some of these return 404
    // for unknown ids before the extractor fires — so we use
    // `?recursive=nope` which must be caught by the extractor first
    // and surface as 400 regardless of whether the id exists.
    let paths = [
        // Physical tree.
        "/api/racks",
        "/api/racks/r1",
        "/api/racks/r1/nodes",
        "/api/nodes",
        "/api/nodes/n1",
        "/api/nodes/n1/server",
        "/api/nodes/n1/stores",
        "/api/nodes/n1/stores/1",
        "/api/nodes/n1/stores/1/groups",
        "/api/nodes/n1/stores/1/groups/1",
        // Logical tree.
        "/api/stores",
        "/api/stores/1",
        "/api/stores/1/groups",
        "/api/stores/1/groups/1",
        "/api/stores/1/groups/1/replicas",
        "/api/stores/1/groups/1/replicas/1",
    ];

    for p in paths {
        let url = format!("http://{addr}{p}?recursive=nope");
        let resp = http.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), 400, "expected 400 for {p}, got {}", resp.status());
        let body: serde_json::Value = resp.json().await.unwrap();
        let err = body["error"].as_str().unwrap_or_default();
        assert!(
            err.contains("not an integer") || err.contains("exceeds"),
            "{p} body: {body}"
        );
    }
}

#[tokio::test]
async fn out_of_range_recursive_yields_400() {
    let addr = spawn_web().await;
    let http = reqwest::Client::new();

    // `MAX_DEPTH` is 8 (see `shared::expand`); anything larger must
    // surface as 400 instead of being silently clamped.
    let resp = http
        .get(format!("http://{addr}/api/racks?recursive=99"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("exceeds maximum"));
}

#[tokio::test]
async fn absent_and_valid_recursive_values_are_accepted() {
    let addr = spawn_web().await;
    let http = reqwest::Client::new();

    // `/api/racks` always succeeds (the list may be empty).
    for q in [
        "",
        "?recursive=0",
        "?recursive=1",
        "?recursive=8",
        "?recursive=all",
        "?recursive=ALL",
    ] {
        let url = format!("http://{addr}/api/racks{q}");
        let resp = http.get(&url).send().await.unwrap();
        assert_eq!(
            resp.status(),
            200,
            "expected 200 for {url}, got {}",
            resp.status()
        );
    }
}

// ── Physical-tree recursive walk (rack → node → store → group) ─────

/// Spawn a web instance seeded with a rack `r1` + node `n1` plus a
/// monitor-cache record for `n1` carrying one store (id 7) with one
/// group (id 9). Returns the listening address.
async fn spawn_web_with_seeded_physical_tree() -> SocketAddr {
    use crowkv_console_shared::cluster::{LocalReplicaInfo, NodeGroup, NodeStore, ReplicaRole, ReplicaState};
    use crowkv_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry};
    use crowkv_console_shared::monitor::NodeRecord;

    let mut cfg = ConsoleConfig::default();
    cfg.add_rack(RackEntry {
        id: "r1".into(),
        name: "rack-1".into(),
    })
    .unwrap();
    cfg.add_node(NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);

    let mut rec = NodeRecord::default();
    rec.stores.insert(
        7,
        NodeStore {
            node_id: "n1".into(),
            store_id: 7,
            listen_addr: None,
            groups: vec![NodeGroup {
                node_id: "n1".into(),
                store_id: 7,
                group_id: 9,
                local: LocalReplicaInfo {
                    replica_id: 100,
                    role: ReplicaRole::Leader,
                    state: ReplicaState::Running,
                    engine_healthy: true,
                    crowtree_stats: None,
                },
                remotes: vec![],
                leader_hint: Some(100),
            }],
        },
    );
    state.monitor_cache.set_node_report("n1".into(), rec).await;

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, crowkv_web::router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn list_racks_recursive_inlines_nodes_and_records_truncation() {
    let addr = spawn_web_with_seeded_physical_tree().await;
    let http = reqwest::Client::new();

    // recursive=1: inline nodes; clip below.
    let resp = http
        .get(format!("http://{addr}/api/racks?recursive=1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let items = body["items"].as_array().expect("items present");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], "r1");
    let nodes = items[0]["nodes"].as_array().expect("nodes inlined at depth 1");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["id"], "n1");
    assert!(nodes[0].get("stores").is_none(), "depth 1 stops before stores");
    // The node hosts a store in the monitor cache, so the walk must
    // record a truncation path under `node:n1`.
    let trunc = body["truncated_at"].as_array().expect("truncated_at present");
    assert_eq!(trunc.len(), 1, "exactly one truncation at depth 1");
    let path: Vec<&str> = trunc[0]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    assert_eq!(path, vec!["rack:r1", "node:n1"]);
}

#[tokio::test]
async fn list_racks_flat_shape_preserved_at_depth_zero() {
    let addr = spawn_web_with_seeded_physical_tree().await;
    let http = reqwest::Client::new();

    // Absent and recursive=0 both produce a flat array (legacy shape).
    for q in ["", "?recursive=0"] {
        let resp = http
            .get(format!("http://{addr}/api/racks{q}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        let arr = body
            .as_array()
            .unwrap_or_else(|| panic!("flat array expected for q={q:?}, got {body}"));
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], "r1");
    }
}

#[tokio::test]
async fn get_rack_recursive_all_inlines_full_subtree() {
    let addr = spawn_web_with_seeded_physical_tree().await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{addr}/api/racks/r1?recursive=all"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["id"], "r1");
    let groups = body["nodes"][0]["stores"][0]["groups"]
        .as_array()
        .expect("groups inlined under store under node");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group_id"], 9);
    assert_eq!(groups[0]["replica_id"], 100);
    let trunc = body["truncated_at"].as_array().expect("truncated_at present");
    assert!(trunc.is_empty(), "recursive=all reaches every leaf");
}

#[tokio::test]
async fn list_rack_nodes_recursive_inlines_stores() {
    let addr = spawn_web_with_seeded_physical_tree().await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{addr}/api/racks/r1/nodes?recursive=2"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let stores = items[0]["stores"].as_array().expect("stores inlined at depth 2");
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0]["store_id"], 7);
    // Groups clipped at depth 2 (one hop = stores, two hops = groups
    // since this endpoint addresses nodes directly).
    assert!(stores[0]["groups"].is_array(), "depth 2 reaches groups");
}
