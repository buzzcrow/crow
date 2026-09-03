// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV data-plane handlers — delegate to `ops::kv_data::*`.

use crate::error::{err_400, err_502, map_config_err, ErrorBody};
use crate::mgmt::refresh_node_cache;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use crowdb_console_shared::cluster::{GroupHealth, NodeId};
use crowdb_console_shared::ops;
use crowdb_kv_client::{GetOutcome, ScanOutcome};
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
/// Polls until a self-reported leader is observed among Up nodes (no
/// first-healthy fallback) so KV writes are not routed to a follower —
/// a follower rejects with "not leader" and triggers a slow retry cycle
/// in the KV client (~2s per round). After all polling attempts, falls
/// back to [`leader_for`](MonitorCache::leader_for) (which includes the
/// first-healthy fallback) as a last resort so the caller can still
/// attempt the op.
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
                // Use strict_leader_for so we only return when a node
                // self-reports as Leader and is Up — routing to a follower
                // triggers "not leader" + slow retry in the KV client.
                if let Some((_rid, node_id)) = state.monitor_cache.strict_leader_for(sid, gid).await {
                    let endpoint = kv_endpoint_for_node(state, sid, node_id).await?;
                    // For non-group-0 stores, verify the endpoint uses the
                    // per-store listen port (not the node's default rpc_url).
                    // If the monitor cache doesn't have listen_addr yet, keep
                    // polling — sending to the wrong port triggers a 4-5s
                    // retry cycle in the KV client.
                    if sid == 0 || endpoint_has_store_port(state, sid, node_id, &endpoint).await {
                        return Ok(endpoint);
                    }
                }
            }
        }
        if attempt == 4 {
            break;
        }
        refresh_group_nodes(state, sid, gid).await;
        sleep(Duration::from_millis(50 * (1 + attempt))).await;
    }

    // Last-resort fallback: use leader_for (first-healthy fallback) so the
    // caller can attempt the op rather than failing immediately. The KV
    // client's retry loop will handle "not leader" if this is a follower.
    if let Some((_rid, node_id)) = state.monitor_cache.leader_for(sid, gid).await {
        let endpoint = kv_endpoint_for_node(state, sid, node_id).await?;
        if sid == 0 || endpoint_has_store_port(state, sid, node_id, &endpoint).await {
            return Ok(endpoint);
        }
    }

    Err((
        StatusCode::NOT_FOUND,
        Json(ErrorBody {
            error: format!("group {gid} in store {sid} not found or has no healthy leader"),
        }),
    ))
}

/// Check whether `endpoint`'s port matches the store's `listen_addr`
/// port from the monitor cache. Returns `false` if the cache has no
/// `listen_addr` for this store (meaning the endpoint fell back to
/// the node's default `rpc_url`, which is wrong for non-group-0 stores).
async fn endpoint_has_store_port(state: &AppState, sid: u64, node_id: NodeId, endpoint: &str) -> bool {
    let snap = state.monitor_cache.snapshot().await;
    let Some(listen_addr) = snap
        .get(&node_id)
        .and_then(|rec| rec.stores.get(&sid))
        .and_then(|ns| ns.listen_addr.as_ref())
    else {
        return false;
    };
    let Some(listen_port) = port_of(listen_addr) else {
        return false;
    };
    port_of(endpoint) == Some(listen_port)
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
    let node_ids = group_node_ids(state, sid, gid).await;
    futures::future::join_all(node_ids.iter().map(|&nid| refresh_node_cache(state, nid))).await;
}

/// `crowdb-kv-server` management-API base URLs (`ServerEntry::url`, e.g.
/// `http://host:rest_port`) for every node hosting a replica of `(sid,
/// gid)`. This is [`CrowdbKvClient`]'s discovery input (`GET /topology` on
/// each seed): any one reachable replica's own `/topology` response
/// carries the real leader's endpoint via its `remotes` list, so seeding
/// with every known replica's mgmt URL is enough for `CrowdbKvClient` to
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

/// Build an `OpContext` for a KV data-plane request on `(sid, gid)`.
///
/// Fails fast with `502` if no KV servers are deployed — the shared
/// `CrowdbKvClient` would have no seeds for topology discovery and
/// every op would retry for ~5s before failing. Returning a clear
/// error immediately is better than a silent timeout.
///
/// Seeds + leader hint are synced from the current config + monitor
/// cache so the shared client's topology cache is fresh for this call.
async fn kv_op_context(
    state: &AppState,
    sid: u64,
    gid: u64,
) -> Result<crowdb_console_shared::ops::OpContext, (StatusCode, Json<ErrorBody>)> {
    let t0 = std::time::Instant::now();
    // Fail fast: if no KV servers are deployed, the client cannot
    // discover any leader. Don't let it retry for seconds.
    let all_seeds: Vec<String> = {
        let cfg = state.config.read().unwrap();
        cfg.servers
            .iter()
            .filter(|s| s.service_type == crowdb_console_shared::config::ServiceType::Kv)
            .map(|s| s.url.clone())
            .collect()
    };
    if all_seeds.is_empty() {
        tracing::warn!("kv_op_context: no KV servers deployed — fail-fast 502 (store={sid}, group={gid})");
        return Err(err_502(
            "no KV servers deployed — cluster not initialized; run cluster init first",
        ));
    }
    // Validate the target group exists + has replicas.
    let _ = mgmt_seeds_for_group(state, sid, gid).await?;
    let ctx = state.op_context().await.map_err(|e| err_502(format!("{e}")))?;
    ctx.kv().set_mgmt_seeds(all_seeds);
    let t_resolve = std::time::Instant::now();
    if let Ok(endpoint) = resolve_kv_endpoint(state, sid, gid).await {
        tracing::debug!(
            "kv_op_context: resolve_kv_endpoint store={sid} group={gid} endpoint={endpoint} in {}ms (total {}ms)",
            t_resolve.elapsed().as_millis(),
            t0.elapsed().as_millis()
        );
        ctx.kv().seed_leader(sid, gid, endpoint);
    } else {
        tracing::warn!(
            "kv_op_context: resolve_kv_endpoint failed for store={sid} group={gid} in {}ms (total {}ms)",
            t_resolve.elapsed().as_millis(),
            t0.elapsed().as_millis()
        );
    }
    Ok(ctx)
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
    let ctx = kv_op_context(&state, sid, gid).await?;
    let t_get = std::time::Instant::now();
    let outcome = ops::kv_data::get(&ctx, sid, gid, &key)
        .await
        .map_err(map_config_err)?;
    tracing::debug!(
        "http_kv_get: store={sid} group={gid} key={} get in {}ms",
        String::from_utf8_lossy(&key),
        t_get.elapsed().as_millis()
    );
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
    let ctx = kv_op_context(&state, sid, gid).await?;
    let ScanOutcome { items, truncated, .. } =
        ops::kv_data::scan(&ctx, sid, gid, &prefix, &start_after, limit)
            .await
            .map_err(map_config_err)?;
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
    let ctx = kv_op_context(&state, sid, gid).await?;
    let t_put = std::time::Instant::now();
    let out = ops::kv_data::put(&ctx, sid, gid, &key, &value, Some((body.client_id, body.seq)))
        .await
        .map_err(map_config_err)?;
    tracing::debug!(
        "http_kv_put: store={sid} group={gid} key={} put in {}ms",
        String::from_utf8_lossy(&key),
        t_put.elapsed().as_millis()
    );
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
    let ctx = kv_op_context(&state, sid, gid).await?;
    let out = ops::kv_data::delete(&ctx, sid, gid, &key, Some((body.client_id, body.seq)))
        .await
        .map_err(map_config_err)?;
    Ok(Json(KvWriteResponse {
        ok: true,
        revision: out.revision,
    }))
}
