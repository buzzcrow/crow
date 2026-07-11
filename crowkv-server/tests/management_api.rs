//! Real-process integration tests for the `crowkv-server` HTTP management API.

mod testkit;

use serde_json::Value;

use testkit::process::{start_test_server, ServerHandle};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn start_server() -> ServerHandle {
    start_test_server(&["--stores", "0", "--groups", "1", "--replica", "1"]).await.expect("start crowkv-server")
}

async fn add_store(base: &str, store_id: u64, group_id: u64, replica_id: u64) -> reqwest::Response {
    client()
        .post(format!("{base}/stores"))
        .json(&serde_json::json!({
            "store_id": store_id,
            "group_id": group_id,
            "replica_id": replica_id
        }))
        .send()
        .await
        .unwrap()
}

async fn add_group(base: &str, store_id: u64, group_id: u64, replica_id: u64) -> reqwest::Response {
    client()
        .post(format!("{base}/stores/{store_id}/groups"))
        .json(&serde_json::json!({
            "group_id": group_id,
            "replica_id": replica_id
        }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn health_check() {
    let server = start_server().await;
    let resp: Value = client().get(format!("{}/health", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(resp["status"], "ok");
}

#[cfg(feature = "swagger-ui")]
#[tokio::test]
async fn openapi_json_is_served() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/openapi.json", server.base_url()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["openapi"], "3.1.0");
    assert!(resp["paths"]["/health"].is_object());
    assert!(resp["paths"]["/topology"].is_object());
}

#[tokio::test]
async fn list_stores_with_initial_cli_store() {
    let server = start_server().await;
    let resp: Value = client().get(format!("{}/stores", server.base_url())).send().await.unwrap().json().await.unwrap();
    let stores = resp["stores"].as_array().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0]["store_id"], 0);
    assert!(stores[0]["group_count"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn get_store_not_found() {
    let server = start_server().await;
    let resp = client().get(format!("{}/stores/99", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn add_store_via_api() {
    let server = start_server().await;
    let resp = add_store(server.base_url(), 5, 10, 2).await;
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["store_id"], 5);
    assert_eq!(body["group_count"], 1);

    let list: Value = client().get(format!("{}/stores", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(list["stores"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn add_store_conflict() {
    let server = start_server().await;
    let resp = add_store(server.base_url(), 0, 1, 1).await;
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn remove_store_via_api() {
    let server = start_server().await;
    let resp = client().delete(format!("{}/stores/0", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client().get(format!("{}/stores/0", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn remove_store_not_found() {
    let server = start_server().await;
    let resp = client().delete(format!("{}/stores/99", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn list_groups() {
    let server = start_server().await;
    let groups: Vec<Value> = client().get(format!("{}/stores/0/groups", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group_id"], 1);
}

#[tokio::test]
async fn add_group_via_api() {
    let server = start_server().await;
    let resp = add_group(server.base_url(), 0, 2, 1).await;
    assert_eq!(resp.status(), 201);

    let groups: Vec<Value> = client().get(format!("{}/stores/0/groups", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(groups.len(), 2);
}

#[tokio::test]
async fn add_group_conflict() {
    let server = start_server().await;
    let resp = add_group(server.base_url(), 0, 1, 1).await;
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn add_group_store_not_found() {
    let server = start_server().await;
    let resp = add_group(server.base_url(), 99, 1, 1).await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn remove_group_via_api() {
    let server = start_server().await;
    let resp = client().delete(format!("{}/stores/0/groups/1", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let groups: Vec<Value> = client().get(format!("{}/stores/0/groups", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(groups.len(), 0);
}

#[tokio::test]
async fn remove_group_not_found() {
    let server = start_server().await;
    let resp = client().delete(format!("{}/stores/0/groups/99", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn list_remotes_empty() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["remotes"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn add_and_list_remotes() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .json(&serde_json::json!([
            {"replica_id": 2, "endpoint": "192.168.1.2:28001"},
            {"replica_id": 3, "endpoint": "192.168.1.3:28001"}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["remotes"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn add_remote_rejects_local_replica() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .json(&serde_json::json!([{ "replica_id": 1, "endpoint": "127.0.0.1:9999" }]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn remove_remote_replica() {
    let server = start_server().await;
    client()
        .post(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .json(&serde_json::json!([
            {"replica_id": 2, "endpoint": "192.168.1.2:28001"},
            {"replica_id": 3, "endpoint": "192.168.1.3:28001"}
        ]))
        .send()
        .await
        .unwrap();

    let resp = client().delete(format!("{}/stores/0/groups/1/remotes/2", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let resp: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let remotes = resp["remotes"].as_array().unwrap();
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0]["replica_id"], 3);
}

#[tokio::test]
async fn remove_remote_rejects_local_replica() {
    let server = start_server().await;
    let resp = client().delete(format!("{}/stores/0/groups/1/remotes/1", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn remove_remote_not_found() {
    let server = start_server().await;
    let resp = client().delete(format!("{}/stores/0/groups/1/remotes/99", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn topology_export() {
    let server = start_server().await;
    let resp: Value = client().get(format!("{}/topology", server.base_url())).send().await.unwrap().json().await.unwrap();
    let stores = resp["stores"].as_array().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0]["store_id"], 0);
    let groups = stores[0]["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group_id"], 1);
    assert_eq!(groups[0]["local_replica_id"], 1);
}

#[tokio::test]
async fn topology_alias_top() {
    let server = start_server().await;
    let resp: Value = client().get(format!("{}/top", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert!(!resp["stores"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn batch_add_remotes_from_topology() {
    let server_a = start_test_server(&["--stores", "0", "--groups", "1", "--replica", "1"]).await.expect("start server A");
    let server_b = start_test_server(&["--stores", "0", "--groups", "1", "--replica", "2"]).await.expect("start server B");

    let topo_b: Value = client().get(format!("{}/topology", server_b.base_url())).send().await.unwrap().json().await.unwrap();

    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes/batch", server_a.base_url()))
        .json(&topo_b)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let remotes: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server_a.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r = remotes["remotes"].as_array().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["replica_id"], 2);
}

#[tokio::test]
async fn progressive_setup_multiple_stores_groups_replicas() {
    let server = start_server().await;

    for sid in 1..3u64 {
        let resp = add_store(server.base_url(), sid, 1, sid + 1).await;
        assert_eq!(resp.status(), 201, "store {sid} should be created");
    }

    for sid in 0..3u64 {
        for gid in [2u64, 3] {
            let resp = add_group(server.base_url(), sid, gid, sid + 1).await;
            assert_eq!(resp.status(), 201, "group {gid} in store {sid}");
        }
    }

    for sid in 0..3u64 {
        let detail: Value = client().get(format!("{}/stores/{sid}", server.base_url())).send().await.unwrap().json().await.unwrap();
        assert_eq!(detail["groups"].as_array().unwrap().len(), 3);
    }

    let topo: Value = client().get(format!("{}/topology", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(topo["stores"].as_array().unwrap().len(), 3);

    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes/batch", server.base_url()))
        .json(&topo)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let remotes: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(remotes["remotes"].as_array().unwrap().len(), 2);

    let resp = client().delete(format!("{}/stores/2", server.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    let list: Value = client().get(format!("{}/stores", server.base_url())).send().await.unwrap().json().await.unwrap();
    assert_eq!(list["stores"].as_array().unwrap().len(), 2);
}
