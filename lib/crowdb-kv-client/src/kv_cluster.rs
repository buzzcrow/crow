// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc, clippy::cast_possible_truncation)]

//! [`KVClusterMetaClient`] and [`KVClusterAdmin`].
//!
//! `KVClusterMetaClient` reads/writes the KV-cluster topology
//! records (store/group/replica) in group 0 under `/kv/...` text-path
//! keys. `KVClusterAdmin` is the control-plane surface for cluster
//! management: each lifecycle method calls the kv-server HTTP endpoint
//! AND writes the `/kv/...` record; query methods delegate to the
//! kv-server HTTP mgmt API.
//!
//! See `doc/design/kv/design-crowdb-kv-group0.md` §2.1, §2.2, §3.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::common::{GroupValue, ReplicaValue, StoreValue};
use crowdb_protocol::common_type::{GroupId, ReplicaId, StoreId};
use crowdb_protocol::key::{KvGroupKey, KvReplicaKey, KvStoreKey, TextKey};
use crowdb_protocol::mgmt::{
    AddGroupRequest, AddStoreRequest, GroupSummary, RemoteReplicaInfo, StepDownRequest, StepDownResult,
    StoreDetail, StoreSummary, SystemInitRequest, SystemInitResponse, WipeResult,
};

use crate::client::{GetOutcome, ScanOutcome};
use crate::{CrowdbClient, Error, Result};

const G0_STORE: u64 = 0;
const G0_GROUP: u64 = 0;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ── helpers ─────────────────────────────────────────────────────

async fn put_json<T: serde::Serialize>(kv: &CrowdbClient, key: &str, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value).map_err(|e| Error::SysdataDecode {
        key: key.to_string(),
        reason: e.to_string(),
    })?;
    kv.put(G0_STORE, G0_GROUP, key.as_bytes(), &payload, None)
        .await
        .map(|_| ())
}

async fn get_json<T: serde::de::DeserializeOwned>(kv: &CrowdbClient, key: &str) -> Result<Option<T>> {
    match kv
        .get(
            G0_STORE,
            G0_GROUP,
            key.as_bytes(),
            crate::ReadMode::Linearizable,
            None,
        )
        .await?
    {
        GetOutcome::Found { value, .. } => {
            let v: T = serde_json::from_slice(&value).map_err(|e| Error::SysdataDecode {
                key: key.to_string(),
                reason: e.to_string(),
            })?;
            Ok(Some(v))
        }
        GetOutcome::NotFound => Ok(None),
    }
}

async fn scan_prefix<T: serde::de::DeserializeOwned>(
    kv: &CrowdbClient,
    prefix: &str,
) -> Result<Vec<(String, T)>> {
    let mut out: Vec<(String, T)> = Vec::new();
    let mut start_after: Vec<u8> = Vec::new();
    loop {
        let ScanOutcome { items, truncated, .. } = kv
            .scan(
                G0_STORE,
                G0_GROUP,
                prefix.as_bytes(),
                &start_after,
                &[],
                0,
                crate::ReadMode::Linearizable,
                None,
                false,
                None,
            )
            .await?;
        for (k, v) in &items {
            let key_str = std::str::from_utf8(k)
                .map_err(|e| Error::SysdataDecode {
                    key: prefix.to_string(),
                    reason: e.to_string(),
                })?
                .to_string();
            let val: T = serde_json::from_slice(v).map_err(|e| Error::SysdataDecode {
                key: key_str.clone(),
                reason: e.to_string(),
            })?;
            out.push((key_str, val));
        }
        if !truncated || items.is_empty() {
            break;
        }
        if let Some((last_key, _)) = items.last() {
            start_after = last_key.to_vec();
        } else {
            break;
        }
    }
    Ok(out)
}

// ── KVClusterMetaClient ─────────────────────────────────────────

/// Client for the KV-cluster topology records (store/group/replica)
/// in group 0.
///
/// All methods target store 0, group 0. The wrapped `CrowdbClient`
/// must have its topology seeded with a group-0 leader endpoint.
pub struct KVClusterMetaClient {
    kv: Arc<CrowdbClient>,
}

impl KVClusterMetaClient {
    /// Wrap a `CrowdbClient` for group-0 KV-cluster topology access.
    #[must_use]
    pub fn new(kv: CrowdbClient) -> Self {
        Self { kv: Arc::new(kv) }
    }

    /// Wrap an already-shared `CrowdbClient`.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbClient>) -> Self {
        Self { kv }
    }

    /// Access the underlying `CrowdbClient`.
    #[must_use]
    pub fn kv(&self) -> &CrowdbClient {
        &self.kv
    }

    // ── store ───────────────────────────────────────────────────

    /// Add or replace a store record.
    pub async fn add_store(&self, store_id: StoreId, node_ids: &[u64]) -> Result<()> {
        let key = KvStoreKey { store_id };
        let value = StoreValue {
            store_id,
            node_ids: node_ids.to_vec(),
        };
        put_json(&self.kv, &key.to_path(), &value).await
    }

    /// Read a store record.
    pub async fn get_store(&self, store_id: StoreId) -> Result<Option<StoreValue>> {
        let key = KvStoreKey { store_id };
        get_json(&self.kv, &key.to_path()).await
    }

    /// Remove a store record.
    pub async fn remove_store(&self, store_id: StoreId) -> Result<()> {
        let key = KvStoreKey { store_id };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all store records (prefix scan `/kv/store/`).
    pub async fn list_stores(&self) -> Result<Vec<StoreValue>> {
        let entries = scan_prefix::<StoreValue>(&self.kv, &KvStoreKey::prefix_all()).await?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }

    // ── group ───────────────────────────────────────────────────

    /// Add or replace a group record.
    pub async fn add_group(&self, store_id: StoreId, group_id: GroupId) -> Result<()> {
        let key = KvGroupKey { store_id, group_id };
        let value = GroupValue { store_id, group_id };
        put_json(&self.kv, &key.to_path(), &value).await
    }

    /// Read a group record.
    pub async fn get_group(&self, store_id: StoreId, group_id: GroupId) -> Result<Option<GroupValue>> {
        let key = KvGroupKey { store_id, group_id };
        get_json(&self.kv, &key.to_path()).await
    }

    /// Remove a group record.
    pub async fn remove_group(&self, store_id: StoreId, group_id: GroupId) -> Result<()> {
        let key = KvGroupKey { store_id, group_id };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all group records in a store (prefix scan
    /// `/kv/group/<store_id>/`).
    pub async fn list_groups_in_store(&self, store_id: StoreId) -> Result<Vec<GroupValue>> {
        let entries =
            scan_prefix::<GroupValue>(&self.kv, &KvGroupKey::text_prefix_for_store(store_id)).await?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }

    // ── replica ─────────────────────────────────────────────────

    /// Add or replace a replica record.
    pub async fn add_replica(&self, value: &ReplicaValue) -> Result<()> {
        let key = KvReplicaKey {
            store_id: value.store_id,
            group_id: value.group_id,
            replica_id: value.replica_id,
        };
        put_json(&self.kv, &key.to_path(), value).await
    }

    /// Read a replica record.
    pub async fn get_replica(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        replica_id: ReplicaId,
    ) -> Result<Option<ReplicaValue>> {
        let key = KvReplicaKey {
            store_id,
            group_id,
            replica_id,
        };
        get_json(&self.kv, &key.to_path()).await
    }

    /// Remove a replica record.
    pub async fn remove_replica(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        replica_id: ReplicaId,
    ) -> Result<()> {
        let key = KvReplicaKey {
            store_id,
            group_id,
            replica_id,
        };
        self.kv
            .delete(G0_STORE, G0_GROUP, key.to_path().as_bytes(), None)
            .await
            .map(|_| ())
    }

    /// List all replica records in a group (prefix scan
    /// `/kv/replica/<store_id>/<group_id>/`).
    pub async fn list_replicas_in_group(
        &self,
        store_id: StoreId,
        group_id: GroupId,
    ) -> Result<Vec<ReplicaValue>> {
        let entries =
            scan_prefix::<ReplicaValue>(&self.kv, &KvReplicaKey::text_prefix_for_group(store_id, group_id))
                .await?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }
}

// ── KVClusterAdmin ──────────────────────────────────────────────

/// Control-plane surface for KV-cluster management.
///
/// Each lifecycle method (`add_store`, `add_group`, `add_remote`,
/// `remove_*`, `step_down`, `system_init`) calls the kv-server HTTP
/// management endpoint AND writes the corresponding `/kv/...` record
/// via the embedded [`KVClusterMetaClient`]. Query methods
/// (`list_stores`, `list_groups`, `list_remote_replicas`) delegate to
/// the kv-server HTTP mgmt API for live runtime state.
///
/// The `base_url` is the kv-server's HTTP management base URL
/// (e.g. `http://127.0.0.1:9910`).
pub struct KVClusterAdmin {
    meta: KVClusterMetaClient,
    http: reqwest::Client,
    base_url: String,
}

impl KVClusterAdmin {
    /// Create a new `KVClusterAdmin` wrapping a `KVClusterMetaClient`
    /// and an HTTP client bound to `base_url`.
    #[must_use]
    pub fn new(meta: KVClusterMetaClient, base_url: &str) -> Self {
        Self {
            meta,
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Access the embedded [`KVClusterMetaClient`].
    #[must_use]
    pub fn meta(&self) -> &KVClusterMetaClient {
        &self.meta
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    // ── lifecycle: store ────────────────────────────────────────

    /// `POST /stores` + write `/kv/store/<store_id>` record.
    pub async fn add_store(&self, req: &AddStoreRequest) -> Result<StoreSummary> {
        let resp: StoreSummary = self.post_json("/stores", req).await?;
        // Write the topology record after the server creates the store.
        self.meta.add_store(req.store_id, &[]).await?;
        Ok(resp)
    }

    /// `DELETE /stores/{sid}` + remove `/kv/store/<store_id>` record.
    pub async fn remove_store(&self, store_id: StoreId) -> Result<()> {
        self.delete_path(&format!("/stores/{store_id}")).await?;
        self.meta.remove_store(store_id).await
    }

    // ── lifecycle: group ────────────────────────────────────────

    /// `POST /stores/{sid}/groups` + write `/kv/group/...` record.
    pub async fn add_group(&self, store_id: StoreId, req: &AddGroupRequest) -> Result<()> {
        self.post_empty(&format!("/stores/{store_id}/groups"), req)
            .await?;
        self.meta.add_group(store_id, req.group_id).await
    }

    /// `DELETE /stores/{sid}/groups/{gid}` + remove `/kv/group/...` record.
    pub async fn remove_group(&self, store_id: StoreId, group_id: GroupId) -> Result<()> {
        self.delete_path(&format!("/stores/{store_id}/groups/{group_id}"))
            .await?;
        self.meta.remove_group(store_id, group_id).await
    }

    // ── lifecycle: remote replica ───────────────────────────────

    /// `POST /stores/{sid}/groups/{gid}/remotes` + write
    /// `/kv/replica/...` record for each new remote.
    pub async fn add_remote_replicas(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        remotes: &[RemoteReplicaInfo],
    ) -> Result<()> {
        self.post_empty(&format!("/stores/{store_id}/groups/{group_id}/remotes"), remotes)
            .await?;
        for r in remotes {
            let value = ReplicaValue {
                store_id,
                group_id,
                replica_id: r.replica_id,
                node_id: 0,
                role: String::new(),
                voting: true,
                endpoint: r.endpoint.clone(),
            };
            self.meta.add_replica(&value).await?;
        }
        Ok(())
    }

    /// `DELETE /stores/{sid}/groups/{gid}/remotes/{rid}` + remove
    /// `/kv/replica/...` record.
    pub async fn remove_remote_replica(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        replica_id: ReplicaId,
    ) -> Result<()> {
        self.delete_path(&format!(
            "/stores/{store_id}/groups/{group_id}/remotes/{replica_id}"
        ))
        .await?;
        self.meta.remove_replica(store_id, group_id, replica_id).await
    }

    // ── lifecycle: step-down ────────────────────────────────────

    /// `POST /stores/{sid}/groups/{gid}/step-down`.
    pub async fn step_down(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        req: &StepDownRequest,
    ) -> Result<StepDownResult> {
        self.post_json(&format!("/stores/{store_id}/groups/{group_id}/step-down"), req)
            .await
    }

    // ── lifecycle: system init ──────────────────────────────────

    // ── lifecycle: wipe user data ────────────────────────────────

    /// `POST /stores/{sid}/groups/{gid}/wipe-user-data`.
    ///
    /// Drops and recreates the WAL + engine user data for the group
    /// on the receiving node, preserving group0 sysdata + store/
    /// group/replica topology. `accepted` is `false` when the
    /// replica had no WAL wired (no-op). The caller is responsible
    /// for waiting for re-election + health after wiping every node.
    pub async fn wipe_user_data(&self, store_id: StoreId, group_id: GroupId) -> Result<WipeResult> {
        self.post_json(
            &format!("/stores/{store_id}/groups/{group_id}/wipe-user-data"),
            &serde_json::json!({}),
        )
        .await
    }

    // ── lifecycle: system init ──────────────────────────────────

    /// `POST /system/init` — bootstrap the system group (store 0, group 0).
    pub async fn system_init(&self, req: &SystemInitRequest) -> Result<SystemInitResponse> {
        self.post_json("/system/init", req).await
    }

    // ── query: live runtime state (from kv-server HTTP) ─────────

    /// `GET /stores` — list all stores with live runtime state.
    pub async fn list_stores(&self) -> Result<Vec<StoreSummary>> {
        let r: crowdb_protocol::mgmt::StoreListResponse = self.get_json("/stores").await?;
        Ok(r.stores)
    }

    /// `GET /stores/{sid}` — store detail with groups.
    pub async fn get_store(&self, store_id: StoreId) -> Result<StoreDetail> {
        self.get_json(&format!("/stores/{store_id}")).await
    }

    /// `GET /stores/{sid}/groups` — list groups in a store.
    pub async fn list_groups(&self, store_id: StoreId) -> Result<Vec<GroupSummary>> {
        self.get_json(&format!("/stores/{store_id}/groups")).await
    }

    /// `GET /stores/{sid}/groups/{gid}/remotes` — list remote replicas.
    pub async fn list_remote_replicas(
        &self,
        store_id: StoreId,
        group_id: GroupId,
    ) -> Result<Vec<RemoteReplicaInfo>> {
        let r: crowdb_protocol::mgmt::RemoteListResponse = self
            .get_json(&format!("/stores/{store_id}/groups/{group_id}/remotes"))
            .await?;
        Ok(r.remotes)
    }

    /// `GET /health` — raw health string from the kv-server.
    pub async fn health(&self) -> Result<String> {
        let resp = self
            .http
            .get(self.url("/health"))
            .send()
            .await
            .map_err(|e| Error::Mgmt(format!("GET /health: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Mgmt(format!("GET /health: HTTP {status}: {text}")));
        }
        resp.text()
            .await
            .map_err(|e| Error::Mgmt(format!("GET /health: decode: {e}")))
    }

    /// `GET /metrics` — raw metrics text from the kv-server.
    pub async fn metrics(&self) -> Result<String> {
        let resp = self
            .http
            .get(self.url("/metrics"))
            .send()
            .await
            .map_err(|e| Error::Mgmt(format!("GET /metrics: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Mgmt(format!("GET /metrics: HTTP {status}: {text}")));
        }
        resp.text()
            .await
            .map_err(|e| Error::Mgmt(format!("GET /metrics: decode: {e}")))
    }

    // ── transport helpers ───────────────────────────────────────

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let resp = self
            .http
            .get(self.url(path))
            .send()
            .await
            .map_err(|e| Error::Mgmt(format!("GET {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Mgmt(format!("GET {path}: HTTP {status}: {text}")));
        }
        resp.json::<T>()
            .await
            .map_err(|e| Error::Mgmt(format!("GET {path}: decode: {e}")))
    }

    async fn post_json<B: serde::Serialize + ?Sized, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let resp = self
            .http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .map_err(|e| Error::Mgmt(format!("POST {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Mgmt(format!("POST {path}: HTTP {status}: {text}")));
        }
        resp.json::<T>()
            .await
            .map_err(|e| Error::Mgmt(format!("POST {path}: decode: {e}")))
    }

    async fn post_empty<B: serde::Serialize + ?Sized>(&self, path: &str, body: &B) -> Result<()> {
        let resp = self
            .http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .map_err(|e| Error::Mgmt(format!("POST {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Mgmt(format!("POST {path}: HTTP {status}: {text}")));
        }
        Ok(())
    }

    async fn delete_path(&self, path: &str) -> Result<()> {
        let resp = self
            .http
            .delete(self.url(path))
            .send()
            .await
            .map_err(|e| Error::Mgmt(format!("DELETE {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::Mgmt(format!("DELETE {path}: HTTP {status}: {text}")));
        }
        Ok(())
    }
}

// Silence unused-import warning for `now_ms` (used by future
// heartbeat-aware admin methods).
#[allow(dead_code)]
fn _now_ms_used() -> u64 {
    now_ms()
}
