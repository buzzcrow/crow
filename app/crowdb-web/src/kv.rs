// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{err_400, err_502, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupHealth, NodeId};
use crowdb_kv_client::{GetOutcome, ReadMode, ScanOutcome};
use hex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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

/// Resolve the crowdb-rpc endpoint for a group's leader via the monitor cache.
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
/// node has no crowdb-rpc URL configured.
pub async fn resolve_kv_endpoint(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    for attempt in 0..5 {
        if let Some(view) = state.monitor_cache.resolve_group(sid, gid).await {
            // A degraded group (one node down in a 3-node cluster) can still
            // make progress as long as a quorum and a leader exist. Route to
            // the leader whenever we know one; only refuse if the group is
            // unavailable (lost quorum) or has no leader at all.
            if view.state != GroupHealth::Unavailable && view.state != GroupHealth::Unknown {
                // Use leader_for (not view.leader) so stale leader records
                // from dead nodes are skipped — leader_for checks node
                // health and falls back to the first Up replica.
                if let Some((_rid, node_id)) = state.monitor_cache.leader_for(sid, gid).await {
                    return kv_endpoint_for_node(state, sid, node_id).await;
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
    node_id: NodeId,
) -> Result<String, (StatusCode, Json<ErrorBody>)> {
    // Each `PxKvStore` listens on its own crowdb-rpc port (ephemeral when created
    // via the management API with `port: None`), reported as the store's
    // `listen_addr`. KV requests must target that per-store endpoint — the
    // node's configured `rpc_url` is a different listener and does not host
    // this store's groups. Combine the node host (from `rpc_url`) with the
    // store's listen port; fall back to `rpc_url` if the cache has no
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
    let rpc_url = cfg
        .server_for_node(node_id)
        .and_then(|s| s.rpc_url.clone())
        .ok_or_else(|| {
            err_502(format!(
                "leader node {node_id} has no crowdb-rpc endpoint configured"
            ))
        })?;

    match store_port {
        Some(port) => Ok(format!("http://{}:{port}", host_of(&rpc_url))),
        None => Ok(rpc_url),
    }
}

#[derive(Debug, Serialize)]
pub struct EndpointResponse {
    /// crowdb-rpc URL of the group's current leader (`http://host:port`),
    /// ready to hand to `KvClient::connect`.
    rpc_url: String,
}

/// `GET /api/stores/:sid/groups/:gid/endpoint`. Resolve the crowdb-rpc
/// endpoint of the group's leader via the monitor cache, so a direct
/// crowdb-rpc client (the CLI bench engine) can dial it without touching any
/// registry. Same resolution as the KV data plane uses internally.
///
/// # Errors
/// `404` if the group is unknown / has no replicas; `502` if the
/// leader's node has no crowdb-rpc endpoint configured.
pub async fn http_kv_endpoint(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<EndpointResponse>, (StatusCode, Json<ErrorBody>)> {
    refresh_group_nodes(&state, sid, gid).await;
    let rpc_url = resolve_kv_endpoint(&state, sid, gid).await?;
    Ok(Json(EndpointResponse { rpc_url }))
}

/// Extract the port from a `host:port` (or `scheme://host:port`) string.
fn port_of(addr: &str) -> Option<u16> {
    addr.rsplit(':').next()?.trim().parse::<u16>().ok()
}

/// Extract the host from a `scheme://host:port` or `host:port` string,
/// defaulting to `127.0.0.1` when it cannot be parsed.
fn host_of(rpc_url: &str) -> String {
    let without_scheme = rpc_url.split_once("://").map_or(rpc_url, |(_, rest)| rest);
    let host = without_scheme.split(':').next().unwrap_or("").trim();
    if host.is_empty() || host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        host.to_string()
    }
}

/// Node ids hosting a replica of `(sid, gid)`, per the monitor cache, or (if
/// the cache has no record for the group yet) the persisted config replica
/// list -- so a restarted web console can still find the nodes to query.
async fn group_node_ids(state: &AppState, sid: u64, gid: u64) -> Vec<NodeId> {
    if let Some(view) = state.monitor_cache.resolve_group(sid, gid).await {
        view.replicas.into_iter().map(|r| r.node_id).collect()
    } else {
        let cfg = state.config.read().unwrap();
        cfg.groups
            .iter()
            .find(|g| g.store_id == sid && g.group_id == gid)
            .map(|g| g.replicas.iter().map(|r| r.node_id).collect())
            .unwrap_or_default()
    }
}

/// Refresh the monitor cache for every node hosting a replica of
/// `(sid, gid)`. Called on initial endpoint resolution so the next
/// `leader_for` call observes a post-election view.
async fn refresh_group_nodes(state: &AppState, sid: u64, gid: u64) {
    for node_id in &group_node_ids(state, sid, gid).await {
        refresh_node_cache(state, *node_id).await;
    }
}

/// `crowdb-kv-server` management-API base URLs (`ServerEntry::url`, e.g.
/// `http://host:rest_port`) for every node hosting a replica of `(sid,
/// gid)`. This is [`CrowdbClient`]'s discovery input (`GET /topology` on
/// each seed): any one reachable replica's own `/topology` response
/// carries the real leader's endpoint via its `remotes` list, so seeding
/// with every known replica's mgmt URL is enough for `CrowdbClient` to
/// self-heal a stale/dead leader without this module doing any endpoint
/// bookkeeping itself (C1-C2).
///
/// # Errors
/// `404` if the group is unknown / has no replicas; `502` if none of its
/// replica nodes have a configured management URL.
async fn mgmt_seeds_for_group(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<Vec<String>, (StatusCode, Json<ErrorBody>)> {
    let node_ids = group_node_ids(state, sid, gid).await;
    if node_ids.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorBody {
                error: format!("group {gid} in store {sid} not found or has no replicas"),
            }),
        ));
    }

    let cfg = state.config.read().unwrap();
    let mut seen = HashSet::new();
    let mut seeds = Vec::new();
    for node_id in &node_ids {
        // Skip stopped servers — no runtime pid means the server process
        // is not running. Including its URL as a seed only wastes time
        // (connection-refused) during topology refresh.
        if state.runtime_pid(*node_id).is_none() {
            continue;
        }
        if let Some(server) = cfg.server_for_node(*node_id) {
            if seen.insert(server.url.clone()) {
                seeds.push(server.url.clone());
            }
        }
    }
    drop(cfg);

    if seeds.is_empty() {
        return Err(err_502(format!(
            "group {gid} in store {sid} has no configured server management URL"
        )));
    }
    Ok(seeds)
}

/// Map a [`crowdb_kv_client::Error`] to a JSON error response. Every variant
/// here is either a discovery/transport failure or a retry-budget
/// exhaustion, which mirrors the old `with_leader_retry`'s uniform "give up
/// and surface 502" outcome once its own candidate-endpoint queue was
/// drained -- there is no 4xx case since `mgmt_seeds_for_group` already
/// rejected an unknown group before a [`CrowdbClient`] is even constructed.
#[allow(clippy::needless_pass_by_value)]
fn map_kv_client_err(e: crowdb_kv_client::Error) -> (StatusCode, Json<ErrorBody>) {
    err_502(format!("{e}"))
}

/// Get a value from the KV store.
///
/// # Errors
/// Returns an error if the key decoding, endpoint resolution, or crowdb-rpc call fails.
pub async fn http_kv_get(
    State(state): State<AppState>,
    Query(q): Query<KvGetQuery>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<KvGetResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(q.key, q.key_hex)?;
    let seeds = mgmt_seeds_for_group(&state, sid, gid).await?;
    let client = state.kv_client().await;
    client.set_mgmt_seeds(seeds);
    if let Ok(endpoint) = resolve_kv_endpoint(&state, sid, gid).await {
        client.seed_leader(sid, gid, endpoint);
    }
    let outcome = client
        .get(sid, gid, &key, ReadMode::Linearizable, None)
        .await
        .map_err(map_kv_client_err)?;
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
    /// Exclusive lower bound for pagination (UTF-8). Mutually exclusive
    /// with `start_after_hex`. Empty means "start from the beginning".
    #[serde(default)]
    start_after: Option<String>,
    #[serde(default)]
    start_after_hex: Option<String>,
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
/// Returns an error if the endpoint resolution or crowdb-rpc call fails.
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
    let start_after = match (q.start_after_hex.as_ref(), q.start_after.as_ref()) {
        (Some(h), _) => decode_hex(h)?,
        (None, Some(s)) => s.as_bytes().to_vec(),
        (None, None) => Vec::new(),
    };
    let limit = q.limit;
    let seeds = mgmt_seeds_for_group(&state, sid, gid).await?;
    let client = state.kv_client().await;
    client.set_mgmt_seeds(seeds);
    if let Ok(endpoint) = resolve_kv_endpoint(&state, sid, gid).await {
        client.seed_leader(sid, gid, endpoint);
    }
    let ScanOutcome { items, truncated, .. } = client
        .scan(
            sid,
            gid,
            &prefix,
            &start_after,
            &[],
            limit,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await
        .map_err(map_kv_client_err)?;
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
/// Returns an error if the key/value decoding, endpoint resolution, or crowdb-rpc call fails.
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
    let seeds = mgmt_seeds_for_group(&state, sid, gid).await?;
    let client = state.kv_client().await;
    client.set_mgmt_seeds(seeds);
    if let Ok(endpoint) = resolve_kv_endpoint(&state, sid, gid).await {
        client.seed_leader(sid, gid, endpoint);
    }
    let out = client
        .put(sid, gid, &key, &value, Some((client_id, seq)))
        .await
        .map_err(map_kv_client_err)?;
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}

/// Delete a value from the KV store.
///
/// # Errors
/// Returns an error if the key decoding, endpoint resolution, or crowdb-rpc call fails.
pub async fn http_kv_delete(
    State(state): State<AppState>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(body): Json<KvWriteBody>,
) -> Result<Json<KvWriteResponse>, (StatusCode, Json<ErrorBody>)> {
    let key = decode_key(body.key, body.key_hex)?;
    let client_id = body.client_id;
    let seq = body.seq;
    let seeds = mgmt_seeds_for_group(&state, sid, gid).await?;
    let client = state.kv_client().await;
    client.set_mgmt_seeds(seeds);
    if let Ok(endpoint) = resolve_kv_endpoint(&state, sid, gid).await {
        client.seed_leader(sid, gid, endpoint);
    }
    let out = client
        .delete(sid, gid, &key, Some((client_id, seq)))
        .await
        .map_err(map_kv_client_err)?;
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}
