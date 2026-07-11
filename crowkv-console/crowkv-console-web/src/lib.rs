//! `CrowKV` Console web backend.
//!
//! C1 status: `/healthz`, `/api/cluster/snapshot`, and a placeholder SPA
//! shell at `/` that fetches the snapshot and renders it as a JSON tree.
//! Real React frontend, Swagger UI mount, and per-resource routes land in
//! later phases.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Html,
    routing::{delete, get},
    Json, Router,
};
use crowkv_console_core::{
    clients::grpc::{GetOutcome, KvClient},
    clients::http::ServerClient,
    error::Error,
    mgmt::{AddGroupRequest, AddStoreRequest, GroupSummary, RemoteReplicaInfo, StoreDetail, StoreSummary},
    topology, ClusterSnapshot,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
pub struct AppState {
    /// Default servers used when a request doesn't specify any. C2 will
    /// load these from `~/.crowkv/console.toml`.
    pub default_servers: Arc<Vec<String>>,
}

impl AppState {
    #[must_use]
    pub fn new(default_servers: Vec<String>) -> Self {
        Self {
            default_servers: Arc::new(default_servers),
        }
    }
}

/// Compile-time path to the vendored Swagger UI assets (committed under
/// `crowkv-console/static/swagger-ui`). The directory layout puts the
/// `static/` tree as a sibling of the `crowkv-console-web` crate.
const SWAGGER_UI_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../static/swagger-ui");

/// Build the Axum router used by both the binary and integration tests.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/", get(index_page))
        .route("/api/cluster/snapshot", get(cluster_snapshot))
        // C8: vendored Swagger UI + management-API OpenAPI proxy. The
        // assets live under `crowkv-console/static/swagger-ui/` and the
        // proxy pulls `/openapi.json` from the upstream `crowkv-server`
        // selected by `?server=<url>` (or the first registered server).
        .nest_service("/api/swagger", tower_http::services::ServeDir::new(SWAGGER_UI_DIR))
        .route("/api/openapi.json", get(http_openapi_proxy))
        // C5 management proxy routes. Each takes a `?server=<url>` query
        // parameter selecting which `crowkv-server` to forward to. When
        // omitted, the first entry in `default_servers` is used.
        .route("/api/stores", get(http_list_stores).post(http_add_store))
        .route("/api/stores/:sid", get(http_get_store).delete(http_remove_store))
        .route("/api/stores/:sid/groups", get(http_list_groups).post(http_add_group))
        .route("/api/stores/:sid/groups/:gid", delete(http_remove_group))
        .route("/api/stores/:sid/groups/:gid/remotes", get(http_list_remotes).post(http_add_remotes))
        .route("/api/stores/:sid/groups/:gid/remotes/:rid", delete(http_remove_remote))
        // C6 KV data plane.
        .route("/api/stores/:sid/groups/:gid/kv/get", get(http_kv_get))
        .route("/api/stores/:sid/groups/:gid/kv/put", axum::routing::post(http_kv_put))
        .route("/api/stores/:sid/groups/:gid/kv/delete", axum::routing::post(http_kv_delete))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct KvGetQuery {
    /// `?server=<url>` like the other routes.
    #[serde(default)]
    server: Option<String>,
    /// Key as UTF-8 string. For binary, use `key_hex`.
    #[serde(default)]
    key: Option<String>,
    /// Hex-encoded raw key. Wins over `key` when present.
    #[serde(default)]
    key_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KvWriteBody {
    /// Key as UTF-8 string. For binary, use `key_hex`.
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    key_hex: Option<String>,
    /// Value as UTF-8 string. For binary, use `value_hex`. Optional for delete.
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    value_hex: Option<String>,
    #[serde(default)]
    client_id: u64,
    #[serde(default)]
    seq: u64,
}

#[derive(Debug, Serialize)]
struct KvGetResponse {
    found: bool,
    revision: u64,
    /// UTF-8 lossy decoding of the value; absent when `found=false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    value_utf8: Option<String>,
    /// Hex-encoded raw bytes of the value; absent when `found=false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    value_hex: Option<String>,
}

#[derive(Debug, Serialize)]
struct KvWriteResponse {
    ok: bool,
    revision: u64,
}

fn decode_key(utf8: Option<String>, hex_enc: Option<String>) -> Result<Vec<u8>, (StatusCode, Json<ErrorBody>)> {
    if let Some(h) = hex_enc {
        return decode_hex(&h);
    }
    if let Some(s) = utf8 {
        return Ok(s.into_bytes());
    }
    Err(err_400("missing `key` or `key_hex`"))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, (StatusCode, Json<ErrorBody>)> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(err_400("hex string has odd length"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        let pair = std::str::from_utf8(chunk).map_err(|_| err_400("non-ascii hex"))?;
        let b = u8::from_str_radix(pair, 16).map_err(|_| err_400("invalid hex digit"))?;
        out.push(b);
    }
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

async fn resolve_kv_endpoint(state: &AppState, server_override: Option<String>, sid: u64) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let mgmt_url = if let Some(s) = server_override {
        s
    } else {
        state
            .default_servers
            .first()
            .cloned()
            .ok_or_else(|| err_400("missing ?server=<url> and no default server configured"))?
    };
    let client = ServerClient::new(mgmt_url.clone()).map_err(|e| err_500(format!("client build: {e}")))?;
    let detail = client.get_store(sid).await.map_err(map_err)?;
    let listen = detail.listen_addr.ok_or_else(|| err_502("upstream store has no listen_addr yet"))?;
    let port = listen.rsplit(':').next().unwrap_or("");
    let host = mgmt_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split(':')
        .next()
        .unwrap_or("127.0.0.1");
    Ok(format!("{host}:{port}"))
}

async fn http_kv_get(State(state): State<AppState>, Query(q): Query<KvGetQuery>, Path((sid, gid)): Path<(u64, u64)>) -> Result<Json<KvGetResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(q.key, q.key_hex)?;
    let endpoint = resolve_kv_endpoint(&state, q.server, sid).await?;
    let mut client = KvClient::connect(endpoint).await.map_err(map_err)?;
    match client.get(gid, &key).await.map_err(map_err)? {
        GetOutcome::NotFound => Ok(Json(KvGetResponse {
            found: false,
            revision: 0,
            value_utf8: None,
            value_hex: None,
        })),
        GetOutcome::Found { value, revision } => Ok(Json(KvGetResponse {
            found: true,
            revision,
            value_utf8: Some(String::from_utf8_lossy(&value).into_owned()),
            value_hex: Some(hex_encode(&value)),
        })),
    }
}

async fn http_kv_put(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<KvWriteBody>,
) -> Result<Json<KvWriteResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(body.key, body.key_hex)?;
    let value = if let Some(h) = body.value_hex {
        decode_hex(&h)?
    } else if let Some(v) = body.value {
        v.into_bytes()
    } else {
        return Err(err_400("missing `value` or `value_hex`"));
    };
    let endpoint = resolve_kv_endpoint(&state, sel.server, sid).await?;
    let mut client = KvClient::connect(endpoint).await.map_err(map_err)?;
    let out = client.put(gid, &key, &value, body.client_id, body.seq).await.map_err(map_err)?;
    Ok(Json(KvWriteResponse { ok: true, revision: out.revision }))
}

async fn http_kv_delete(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<KvWriteBody>,
) -> Result<Json<KvWriteResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(body.key, body.key_hex)?;
    let endpoint = resolve_kv_endpoint(&state, sel.server, sid).await?;
    let mut client = KvClient::connect(endpoint).await.map_err(map_err)?;
    let out = client.delete(gid, &key, body.client_id, body.seq).await.map_err(map_err)?;
    Ok(Json(KvWriteResponse { ok: true, revision: out.revision }))
}

#[derive(Debug, Deserialize, Default)]
struct ServerSelector {
    /// Single `?server=<url>` query parameter; falls back to
    /// `AppState::default_servers[0]` when absent.
    #[serde(default)]
    server: Option<String>,
}

fn pick_server(state: &AppState, sel: &ServerSelector) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    if let Some(s) = &sel.server {
        return Ok(s.clone());
    }
    if let Some(first) = state.default_servers.first() {
        return Ok(first.clone());
    }
    Err(err_400("missing ?server=<url> and no default server configured"))
}

fn build_client(state: &AppState, sel: &ServerSelector) -> Result<ServerClient, (StatusCode, Json<ErrorBody>)> {
    let url = pick_server(state, sel)?;
    ServerClient::new(url).map_err(|e| err_500(format!("client build: {e}")))
}

#[derive(serde::Serialize)]
struct ErrorBody {
    error: String,
}

fn err_400(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::BAD_REQUEST, Json(ErrorBody { error: msg.into() }))
}
fn err_500(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: msg.into() }))
}
fn err_502(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::BAD_GATEWAY, Json(ErrorBody { error: msg.into() }))
}

/// Map a `crowkv-console-core` `Error` into a JSON error response. Any
/// `ServerRpc` failure (the only kind these mgmt helpers return) is
/// surfaced as a `502 Bad Gateway` so the frontend can distinguish
/// "console is broken" from "the upstream server returned 4xx/5xx".
#[allow(clippy::needless_pass_by_value)]
fn map_err(e: Error) -> (StatusCode, Json<ErrorBody>) {
    err_502(format!("{e}"))
}

/// Forward `GET /openapi.json` to the upstream `crowkv-server` selected
/// by `?server=<url>` (or the first registered default). The response
/// body is the raw upstream JSON; the proxy is required because Swagger
/// UI runs in the browser and would otherwise hit cross-origin /
/// authentication restrictions when calling the upstream directly.
async fn http_openapi_proxy(State(state): State<AppState>, Query(sel): Query<ServerSelector>) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let base = pick_server(&state, &sel)?;
    let url = format!("{}/openapi.json", base.trim_end_matches('/'));
    let resp = reqwest::get(&url).await.map_err(|e| err_502(format!("openapi proxy: {e}")))?;
    if !resp.status().is_success() {
        return Err(err_502(format!("openapi proxy: upstream {}", resp.status())));
    }
    let value = resp.json::<serde_json::Value>().await.map_err(|e| err_502(format!("openapi proxy: parse: {e}")))?;
    Ok(Json(value))
}

async fn http_list_stores(State(state): State<AppState>, Query(sel): Query<ServerSelector>) -> Result<Json<Vec<StoreSummary>>, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.list_stores().await.map(Json).map_err(map_err)
}

async fn http_add_store(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Json(req): Json<AddStoreRequest>,
) -> Result<(StatusCode, Json<StoreSummary>), (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    let summary = client.add_store(&req).await.map_err(map_err)?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn http_get_store(State(state): State<AppState>, Query(sel): Query<ServerSelector>, Path(sid): Path<u64>) -> Result<Json<StoreDetail>, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.get_store(sid).await.map(Json).map_err(map_err)
}

async fn http_remove_store(State(state): State<AppState>, Query(sel): Query<ServerSelector>, Path(sid): Path<u64>) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.remove_store(sid).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn http_list_groups(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path(sid): Path<u64>,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.list_groups(sid).await.map(Json).map_err(map_err)
}

async fn http_add_group(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path(sid): Path<u64>,
    Json(req): Json<AddGroupRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.add_group(sid, &req).await.map_err(map_err)?;
    Ok(StatusCode::CREATED)
}

async fn http_remove_group(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.remove_group(sid, gid).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn http_list_remotes(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<Vec<RemoteReplicaInfo>>, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.list_remotes(sid, gid).await.map(Json).map_err(map_err)
}

async fn http_add_remotes(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(remotes): Json<Vec<RemoteReplicaInfo>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.add_remotes(sid, gid, &remotes).await.map_err(map_err)?;
    Ok(StatusCode::CREATED)
}

async fn http_remove_remote(
    State(state): State<AppState>,
    Query(sel): Query<ServerSelector>,
    Path((sid, gid, rid)): Path<(u64, u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    let client = build_client(&state, &sel)?;
    client.remove_remote(sid, gid, rid).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn healthz() -> &'static str {
    "ok"
}

#[derive(Debug, Deserialize, Default)]
struct SnapshotQuery {
    /// Repeatable `?server=<url>` query parameter; falls back to
    /// `AppState::default_servers` when absent.
    #[serde(default)]
    server: Vec<String>,
}

async fn cluster_snapshot(State(state): State<AppState>, Query(q): Query<SnapshotQuery>) -> Json<ClusterSnapshot> {
    let servers: Vec<String> = if q.server.is_empty() { (*state.default_servers).clone() } else { q.server };
    let snap = topology::aggregate(&servers).await.unwrap_or_default();
    Json(snap)
}

async fn index_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>CrowKV Console (C1)</title>
    <style>
        body { font-family: ui-monospace, monospace; margin: 16px; background: #0b0d10; color: #d8dee9; }
        h1 { font-size: 16px; color: #88c0d0; }
        pre { background: #161a1f; padding: 12px; border-radius: 6px; overflow: auto; }
        input { background: #1c2129; color: #d8dee9; border: 1px solid #2e3440; padding: 4px 8px; }
        button { background: #2e3440; color: #d8dee9; border: 1px solid #4c566a; padding: 4px 12px; cursor: pointer; }
    </style>
</head>
<body>
    <h1>CrowKV Console &middot; cluster/snapshot (C1 placeholder UI)</h1>
    <p>
        Server URL:
        <input id="srv" size="60" placeholder="http://127.0.0.1:9910" />
        <button onclick="reload()">Refresh</button>
    </p>
    <pre id="out">loading...</pre>
    <script>
        async function reload() {
            const srv = document.getElementById('srv').value.trim();
            const url = srv ? '/api/cluster/snapshot?server=' + encodeURIComponent(srv)
                            : '/api/cluster/snapshot';
            const r = await fetch(url);
            const j = await r.json();
            document.getElementById('out').textContent = JSON.stringify(j, null, 2);
        }
        reload();
    </script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::{router, AppState};

    #[test]
    fn router_builds() {
        let _ = router(AppState::default());
    }
}
