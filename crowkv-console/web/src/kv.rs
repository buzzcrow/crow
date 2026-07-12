use crate::error::{err_400, err_502, map_err, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowkv_console_shared::clients::grpc::{GetOutcome, KvClient, ScanOutcome};
use crowkv_console_shared::cluster::{GroupHealth, ReplicaRole};
use crowkv_console_shared::error::Error as SharedError;
use hex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::time::{sleep, Duration};

#[derive(Debug, Deserialize)]
pub struct KvGetQuery {
    /// Key as UTF-8 string. For binary, use `key_hex`.
    #[serde(default)]
    key: Option<String>,
    /// Hex-encoded raw key. Wins over `key` when present.
    #[serde(default)]
    key_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KvWriteBody {
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
pub struct KvGetResponse {
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
pub struct KvWriteResponse {
    ok: bool,
    revision: u64,
}

/// Decode a key from either UTF-8 or hex encoding.
///
/// # Errors
/// Returns an error if neither encoding is provided or if hex decoding fails.
pub fn decode_key(
    utf8: Option<String>,
    hex_enc: Option<String>,
) -> Result<Vec<u8>, (axum::http::StatusCode, Json<ErrorBody>)> {
    if let Some(h) = hex_enc {
        return decode_hex(&h);
    }
    if let Some(s) = utf8 {
        return Ok(s.into_bytes());
    }
    Err(err_400("missing `key` or `key_hex`"))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, (axum::http::StatusCode, Json<ErrorBody>)> {
    hex::decode(s.trim()).map_err(|e| err_400(format!("invalid hex: {e}")))
}

/// Resolve the gRPC endpoint for a group's leader via the monitor cache.
/// Falls back to any healthy replica if no leader hint is available.
///
/// Before returning, the monitor cache is refreshed for every node hosting
/// the group until a leader is observed, so KV reads are not forwarded
/// based on stale topology immediately after a restart.
///
/// # Panics
/// Panics if the `RwLock` is poisoned.
///
/// # Errors
/// Returns `404` if the group is unknown, or `502` if the leader's
/// node has no gRPC URL configured.
pub async fn resolve_kv_endpoint(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    for attempt in 0..5 {
        if let Some(view) = state.monitor_cache.resolve_group(sid, gid).await {
            if view.state == GroupHealth::Healthy {
                if let Some(leader) = view.leader() {
                    return kv_endpoint_for_node(state, sid, &leader.node_id).await;
                }
            }
        }
        if attempt == 4 {
            break;
        }
        refresh_group_nodes(state, sid, gid).await;
        sleep(Duration::from_millis(50 * (1 + attempt))).await;
    }

    Err((
        StatusCode::NOT_FOUND,
        Json(ErrorBody {
            error: format!("group {gid} in store {sid} not found or has no healthy leader"),
        }),
    ))
}

async fn kv_endpoint_for_node(
    state: &AppState,
    sid: u64,
    node_id: &str,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    let node_id = node_id.to_string();

    // Each `PxKvStore` listens on its own gRPC port (ephemeral when created
    // via the management API with `port: None`), reported as the store's
    // `listen_addr`. KV requests must target that per-store endpoint — the
    // node's configured `grpc_url` is a different listener and does not host
    // this store's groups. Combine the node host (from `grpc_url`) with the
    // store's listen port; fall back to `grpc_url` if the cache has no
    // `listen_addr` yet.
    let store_port = {
        let snap = state.monitor_cache.snapshot().await;
        snap.get(&node_id)
            .and_then(|rec| rec.stores.get(&sid))
            .and_then(|ns| ns.listen_addr.as_ref())
            .and_then(|addr| port_of(addr))
            .filter(|p| *p != 0)
    };

    let cfg = state.config.read().unwrap();
    let grpc_url = cfg
        .server_for_node(&node_id)
        .and_then(|s| s.grpc_url.clone())
        .ok_or_else(|| err_502(format!("leader node {node_id} has no gRPC endpoint configured")))?;

    match store_port {
        Some(port) => Ok(format!("http://{}:{port}", host_of(&grpc_url))),
        None => Ok(grpc_url),
    }
}

async fn group_candidate_endpoints(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<Vec<String>, (StatusCode, Json<ErrorBody>)> {
    let view = state.monitor_cache.resolve_group(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found or has no replicas"),
            }),
        )
    })?;

    let mut replicas = view.replicas;
    replicas.sort_by_key(|replica| replica.role != ReplicaRole::Leader);

    let mut seen = HashSet::new();
    let mut endpoints = Vec::new();
    for replica in replicas {
        if let Ok(endpoint) = kv_endpoint_for_node(state, sid, &replica.node_id).await {
            let canonical = canonical_endpoint(&endpoint);
            if seen.insert(canonical) {
                endpoints.push(endpoint);
            }
        }
    }

    if endpoints.is_empty() {
        return Err(err_502(format!(
            "group {gid} in store {sid} has no gRPC endpoints configured"
        )));
    }
    Ok(endpoints)
}

#[derive(Debug, Serialize)]
pub struct EndpointResponse {
    /// gRPC URL of the group's current leader (`http://host:port`),
    /// ready to hand to `KvClient::connect`.
    grpc_url: String,
}

/// `GET /api/stores/:sid/groups/:gid/endpoint`. Resolve the gRPC
/// endpoint of the group's leader via the monitor cache, so a direct
/// gRPC client (the CLI bench engine) can dial it without touching any
/// registry. Same resolution as the KV data plane uses internally.
///
/// # Errors
/// `404` if the group is unknown / has no replicas; `502` if the
/// leader's node has no gRPC endpoint configured.
pub async fn http_kv_endpoint(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<EndpointResponse>, (StatusCode, Json<ErrorBody>)> {
    refresh_group_nodes(&state, sid, gid).await;
    let grpc_url = resolve_kv_endpoint(&state, sid, gid).await?;
    Ok(Json(EndpointResponse { grpc_url }))
}

/// Extract the port from a `host:port` (or `scheme://host:port`) string.
fn port_of(addr: &str) -> Option<u16> {
    addr.rsplit(':').next()?.trim().parse::<u16>().ok()
}

/// Extract the host from a `scheme://host:port` or `host:port` string,
/// defaulting to `127.0.0.1` when it cannot be parsed.
fn host_of(grpc_url: &str) -> String {
    let without_scheme = grpc_url.split_once("://").map_or(grpc_url, |(_, rest)| rest);
    let host = without_scheme.split(':').next().unwrap_or("").trim();
    if host.is_empty() || host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        host.to_string()
    }
}

fn endpoint_from_hint(hint: &str) -> Option<String> {
    let trimmed = hint.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn canonical_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint)
        .trim()
        .to_string()
}

fn status_is_not_leader(status: &str) -> bool {
    status.to_ascii_lowercase().contains("not leader")
}

fn enqueue_back(queue: &mut VecDeque<String>, pending: &mut HashSet<String>, endpoint: String) {
    let canonical = canonical_endpoint(&endpoint);
    if pending.insert(canonical) {
        queue.push_back(endpoint);
    }
}

fn enqueue_front(queue: &mut VecDeque<String>, pending: &mut HashSet<String>, endpoint: String) {
    let canonical = canonical_endpoint(&endpoint);
    if pending.insert(canonical) {
        queue.push_front(endpoint);
    }
}

/// Refresh the monitor cache for every node hosting a replica of
/// `(sid, gid)`. Called on `NotLeader` and on initial endpoint resolution so
/// the next `leader_for` call observes a post-election view. If the cache
/// has no record for the group yet, fall back to the persisted config
/// replica list so a restarted web console can still find the nodes to
/// refresh.
async fn refresh_group_nodes(state: &AppState, sid: u64, gid: u64) {
    let node_ids: Vec<String> = if let Some(view) = state.monitor_cache.resolve_group(sid, gid).await {
        view.replicas.into_iter().map(|r| r.node_id).collect()
    } else {
        let cfg = state.config.read().unwrap();
        cfg.groups
            .iter()
            .find(|g| g.store_id == sid && g.group_id == gid)
            .map(|g| g.replicas.iter().map(|r| r.node_id.clone()).collect())
            .unwrap_or_default()
    };
    for node_id in &node_ids {
        refresh_node_cache(state, node_id).await;
    }
}

/// Run one KV gRPC operation against the currently-cached leader.
/// On [`SharedError::NotLeader`], refresh every hosting node's entry
/// in the monitor cache, re-resolve the endpoint, and retry the op
/// exactly once. Any other error propagates unchanged.
///
/// `op` receives a mutable `KvClient` bound to the current leader's
/// endpoint. It must be re-runnable (idempotent under `(client_id,
/// seq)` at the upstream, which every KV request is).
async fn with_leader_retry<F, Fut, T>(
    state: &AppState,
    sid: u64,
    gid: u64,
    mut op: F,
) -> Result<T, (StatusCode, Json<ErrorBody>)>
where
    F: FnMut(KvClient) -> Fut,
    Fut: std::future::Future<Output = Result<T, SharedError>>,
{
    let mut queue = VecDeque::new();
    let mut pending = HashSet::new();
    let mut attempt_counts: HashMap<String, usize> = HashMap::new();
    let mut last_err: Option<SharedError> = None;

    enqueue_back(
        &mut queue,
        &mut pending,
        resolve_kv_endpoint(state, sid, gid).await?,
    );
    for endpoint in group_candidate_endpoints(state, sid, gid).await? {
        enqueue_back(&mut queue, &mut pending, endpoint);
    }

    while let Some(endpoint) = queue.pop_front() {
        let canonical = canonical_endpoint(&endpoint);
        pending.remove(&canonical);
        let count = attempt_counts.entry(canonical.clone()).or_default();
        if *count >= state.kv_retry.max_attempts_per_endpoint {
            continue;
        }
        *count += 1;

        let client = match KvClient::connect(endpoint.clone()).await {
            Ok(client) => client,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };

        match op(client).await {
            Ok(v) => return Ok(v),
            Err(SharedError::NotLeader { hint }) => {
                last_err = Some(SharedError::NotLeader { hint: hint.clone() });
                KvClient::invalidate_cache(&endpoint);
                refresh_group_nodes(state, sid, gid).await;
                sleep(Duration::from_millis(state.kv_retry.not_leader_backoff_ms)).await;
                if let Some(endpoint) = endpoint_from_hint(&hint) {
                    enqueue_front(&mut queue, &mut pending, endpoint);
                }
                if let Ok(endpoints) = group_candidate_endpoints(state, sid, gid).await {
                    for endpoint in endpoints {
                        enqueue_back(&mut queue, &mut pending, endpoint);
                    }
                }
            }
            Err(SharedError::UpstreamRpc { node_id, status }) => {
                let err = SharedError::UpstreamRpc {
                    node_id: node_id.clone(),
                    status: status.clone(),
                };
                if node_id == "<grpc>" && !status_is_not_leader(&status) {
                    return Err(map_err(err));
                }
                last_err = Some(err);
                KvClient::invalidate_cache(&endpoint);
                refresh_group_nodes(state, sid, gid).await;
                sleep(Duration::from_millis(state.kv_retry.not_leader_backoff_ms)).await;
                enqueue_front(&mut queue, &mut pending, endpoint);
                if let Ok(endpoints) = group_candidate_endpoints(state, sid, gid).await {
                    for endpoint in endpoints {
                        enqueue_back(&mut queue, &mut pending, endpoint);
                    }
                }
            }
            Err(SharedError::NodeUnreachable { node_id, reason }) => {
                last_err = Some(SharedError::NodeUnreachable { node_id, reason });
                KvClient::invalidate_cache(&endpoint);
                refresh_group_nodes(state, sid, gid).await;
                if let Ok(endpoints) = group_candidate_endpoints(state, sid, gid).await {
                    for endpoint in endpoints {
                        enqueue_back(&mut queue, &mut pending, endpoint);
                    }
                }
            }
            Err(e) => return Err(map_err(e)),
        }
    }

    Err(map_err(last_err.unwrap_or_else(|| SharedError::UpstreamRpc {
        node_id: "<grpc>".into(),
        status: format!("no reachable leader for group {gid} in store {sid}"),
    })))
}

/// Get a value from the KV store.
///
/// # Errors
/// Returns an error if the key decoding, endpoint resolution, or gRPC call fails.
pub async fn http_kv_get(
    State(state): State<AppState>,
    Query(q): Query<KvGetQuery>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<KvGetResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(q.key, q.key_hex)?;
    let outcome = with_leader_retry(&state, sid, gid, |mut client| {
        let key = key.clone();
        async move { client.get(gid, &key).await }
    })
    .await?;
    match outcome {
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
            value_hex: Some(hex::encode(&value)),
        })),
    }
}

#[derive(Debug, Deserialize)]
pub struct KvScanQuery {
    /// UTF-8 prefix; mutually exclusive with `prefix_hex`. Empty means
    /// "every key in the group".
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    prefix_hex: Option<String>,
    /// `0` = no limit. Defaults to 100 to keep the JSON payload small
    /// for human consumers.
    #[serde(default = "default_scan_limit")]
    limit: u32,
}

fn default_scan_limit() -> u32 {
    100
}

#[derive(Debug, Serialize)]
pub struct KvScanItemView {
    key_utf8: String,
    key_hex: String,
    value_utf8: String,
    value_hex: String,
}

#[derive(Debug, Serialize)]
pub struct KvScanResponseView {
    items: Vec<KvScanItemView>,
    truncated: bool,
}

/// Scan keys in the KV store.
///
/// # Errors
/// Returns an error if the endpoint resolution or gRPC call fails.
pub async fn http_kv_scan(
    State(state): State<AppState>,
    Query(q): Query<KvScanQuery>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<KvScanResponseView>, (StatusCode, Json<ErrorBody>)> {
    let prefix = match (q.prefix_hex.as_ref(), q.prefix.as_ref()) {
        (Some(h), _) => decode_hex(h)?,
        (None, Some(s)) => s.as_bytes().to_vec(),
        (None, None) => Vec::new(),
    };
    let limit = q.limit;
    let ScanOutcome { items, truncated } = with_leader_retry(&state, sid, gid, |mut client| {
        let prefix = prefix.clone();
        async move { client.scan(gid, &prefix, limit).await }
    })
    .await?;
    let items = items
        .into_iter()
        .map(|(k, v)| KvScanItemView {
            key_utf8: String::from_utf8_lossy(&k).into_owned(),
            key_hex: hex::encode(&k),
            value_utf8: String::from_utf8_lossy(&v).into_owned(),
            value_hex: hex::encode(&v),
        })
        .collect();
    Ok(Json(KvScanResponseView { items, truncated }))
}

/// Put a value into the KV store.
///
/// # Errors
/// Returns an error if the key/value decoding, endpoint resolution, or gRPC call fails.
pub async fn http_kv_put(
    State(state): State<AppState>,
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
    let client_id = body.client_id;
    let seq = body.seq;
    let out = with_leader_retry(&state, sid, gid, |mut client| {
        let key = key.clone();
        let value = value.clone();
        async move { client.put(gid, &key, &value, client_id, seq).await }
    })
    .await?;
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}

/// Delete a value from the KV store.
///
/// # Errors
/// Returns an error if the key decoding, endpoint resolution, or gRPC call fails.
pub async fn http_kv_delete(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<KvWriteBody>,
) -> Result<Json<KvWriteResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(body.key, body.key_hex)?;
    let client_id = body.client_id;
    let seq = body.seq;
    let out = with_leader_retry(&state, sid, gid, |mut client| {
        let key = key.clone();
        async move { client.delete(gid, &key, client_id, seq).await }
    })
    .await?;
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}
