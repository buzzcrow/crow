use crate::error::{err_400, err_502, map_err, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowkv_console_shared::clients::grpc::{GetOutcome, KvClient, ScanOutcome};
use crowkv_console_shared::error::Error as SharedError;
use hex;
use serde::{Deserialize, Serialize};

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
    let (_rid, node_id) = state.monitor_cache.leader_for(sid, gid).await.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found or has no replicas"),
            }),
        )
    })?;
    let cfg = state.config.read().unwrap();
    let grpc_url = cfg
        .server_for_node(&node_id)
        .and_then(|s| s.grpc_url.clone())
        .ok_or_else(|| err_502(format!("leader node {node_id} has no gRPC endpoint configured")))?;
    // KvClient::connect expects "http://host:port" or "host:port".
    Ok(grpc_url)
}

/// Refresh the monitor cache for every node currently hosting a
/// replica of `(sid, gid)`. Called on `NotLeader` so the next
/// `leader_for` call can observe a post-election view.
async fn refresh_group_nodes(state: &AppState, sid: u64, gid: u64) {
    let Some(view) = state.monitor_cache.resolve_group(sid, gid).await else {
        return;
    };
    for r in &view.replicas {
        refresh_node_cache(state, &r.node_id).await;
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
    let endpoint = resolve_kv_endpoint(state, sid, gid).await?;
    let client = KvClient::connect(endpoint).await.map_err(map_err)?;
    match op(client).await {
        Ok(v) => Ok(v),
        Err(SharedError::NotLeader { .. }) => {
            refresh_group_nodes(state, sid, gid).await;
            let endpoint = resolve_kv_endpoint(state, sid, gid).await?;
            let client = KvClient::connect(endpoint).await.map_err(map_err)?;
            op(client).await.map_err(map_err)
        }
        Err(e) => Err(map_err(e)),
    }
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
