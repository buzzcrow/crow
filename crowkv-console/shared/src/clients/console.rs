//! HTTP client for `crowkv-web`'s two-tree API contract.
//!
//! `ServerClient` (sibling) targets a single `crowkv-server`'s
//! management API and is used by `crowkv-web` itself. `ConsoleClient`
//! lives one layer higher: it talks to `crowkv-web` over the public
//! `/api/...` surface that the SPA and CLI both consume.
//!
//! Key work: store/group/replica orchestrated mutations, monitor-cache
//! reads, and KV data-plane proxying (the leader is resolved by
//! `crowkv-web` server-side, so there is no `?server=` parameter).

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::cluster::{GroupSummary, GroupView, ReplicaView, StoreView};
use crate::config::{NodeEntry, RackEntry, ServerEntry};
use crate::error::{Error, Result};

/// Thin wrapper around `reqwest::Client` bound to one `crowkv-web`
/// console base URL (default `http://127.0.0.1:9920`).
#[derive(Debug, Clone)]
pub struct ConsoleClient {
    base_url: String,
    inner: reqwest::Client,
}

// ── Request bodies (match the handlers in crowkv-web/src/{mgmt,kv}.rs) ──

#[derive(Debug, Clone, Serialize)]
pub struct CreateStoreBody {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    #[serde(default)]
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateGroupBody {
    pub group_id: u64,
    pub replica_id: u64,
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AddRackBody {
    pub id: String,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeployNodeServerBody {
    pub mgmt_port: u16,
    pub grpc_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PingResult {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeployResult {
    pub node_id: String,
    pub mgmt_url: String,
    pub grpc_url: String,
    pub pid: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StopResult {
    pub sent: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AddReplicaBody {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct KvWriteBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_hex: Option<String>,
    #[serde(default)]
    pub client_id: u64,
    #[serde(default)]
    pub seq: u64,
}

// ── Response shapes ──

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KvGetResponse {
    pub found: bool,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub value_utf8: Option<String>,
    #[serde(default)]
    pub value_hex: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KvWriteResponse {
    pub ok: bool,
    pub revision: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KvScanItem {
    pub key_utf8: String,
    pub key_hex: String,
    pub value_utf8: String,
    pub value_hex: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KvScanResponse {
    pub items: Vec<KvScanItem>,
    pub truncated: bool,
}

impl ConsoleClient {
    /// Build a new client. `base_url` may include or omit a trailing slash.
    ///
    /// # Errors
    /// Fails if the underlying `reqwest::Client` cannot be constructed.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let mut base = base_url.into();
        while base.ends_with('/') {
            base.pop();
        }
        let inner = reqwest::Client::builder().timeout(Duration::from_secs(15)).build().map_err(|e| Error::UpstreamRpc {
            node_id: base.clone(),
            status: format!("client build failed: {e}"),
        })?;
        Ok(Self { base_url: base, inner })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    // ── Physical: rack lifecycle ───────────────────────────────────

    /// `GET /api/racks`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn list_racks(&self) -> Result<Vec<RackEntry>> {
        self.get_json("/api/racks").await
    }

    /// `POST /api/racks`.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn add_rack(&self, body: &AddRackBody) -> Result<RackEntry> {
        self.post_json("/api/racks", body).await
    }

    /// `DELETE /api/racks/:rack_id`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn remove_rack(&self, rack_id: &str) -> Result<()> {
        self.delete_path(&format!("/api/racks/{rack_id}")).await
    }

    // ── Physical: node lifecycle ───────────────────────────────────

    /// `GET /api/nodes` (optionally filtered by rack).
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn list_nodes(&self, rack_id: Option<&str>) -> Result<Vec<NodeEntry>> {
        let path = match rack_id {
            Some(r) => format!("/api/nodes?rack_id={r}"),
            None => "/api/nodes".to_string(),
        };
        self.get_json(&path).await
    }

    /// `POST /api/racks/:rack_id/nodes`. Creates the node under the
    /// given rack; the body's `rack_id` is overridden by the path
    /// parameter on the server side.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn add_node(&self, rack_id: &str, entry: &NodeEntry) -> Result<NodeEntry> {
        self.post_json(&format!("/api/racks/{rack_id}/nodes"), entry).await
    }

    /// `DELETE /api/nodes/:node_id`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn remove_node(&self, node_id: &str) -> Result<()> {
        self.delete_path(&format!("/api/nodes/{node_id}")).await
    }

    /// `POST /api/nodes/:node_id/ping`.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn ping_node(&self, node_id: &str) -> Result<PingResult> {
        self.post_json(&format!("/api/nodes/{node_id}/ping"), &serde_json::json!({})).await
    }

    // ── Physical: server lifecycle (one server per node) ──────────

    /// `GET /api/nodes/:node_id/server`. Returns the deployment
    /// record; 404 if no server is deployed.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn get_node_server(&self, node_id: &str) -> Result<ServerEntry> {
        self.get_json(&format!("/api/nodes/{node_id}/server")).await
    }

    /// `POST /api/nodes/:node_id/server/deploy`.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn deploy_node_server(&self, node_id: &str, body: &DeployNodeServerBody) -> Result<DeployResult> {
        self.post_json(&format!("/api/nodes/{node_id}/server/deploy"), body).await
    }

    /// `POST /api/nodes/:node_id/server/stop`.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn stop_node_server(&self, node_id: &str) -> Result<StopResult> {
        self.post_json(&format!("/api/nodes/{node_id}/server/stop"), &serde_json::json!({})).await
    }

    /// Restart the `crowkv-server` running on `node_id`. Stops the
    /// tracked process (if any) and re-deploys on the same ports
    /// recorded in the console config.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn restart_node_server(&self, node_id: &str) -> Result<DeployResult> {
        self.post_json(&format!("/api/nodes/{node_id}/server/restart"), &serde_json::json!({})).await
    }

    // ── Logical store plane ────────────────────────────────────────

    /// `GET /api/stores`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn list_stores(&self) -> Result<Vec<StoreView>> {
        self.get_json("/api/stores").await
    }

    /// `GET /api/stores/:store_id`.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn get_store(&self, sid: u64) -> Result<StoreView> {
        self.get_json(&format!("/api/stores/{sid}")).await
    }

    /// `POST /api/stores`.
    ///
    /// # Errors
    /// Transport, decode, or 4xx/5xx errors surface as `Error::UpstreamRpc`.
    pub async fn add_store(&self, body: &CreateStoreBody) -> Result<Value> {
        self.post_json("/api/stores", body).await
    }

    /// `DELETE /api/stores/:store_id`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn remove_store(&self, sid: u64) -> Result<()> {
        self.delete_path(&format!("/api/stores/{sid}")).await
    }

    // ── Logical group plane ────────────────────────────────────────

    /// `GET /api/stores/:store_id/groups`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn list_groups(&self, sid: u64) -> Result<Vec<GroupSummary>> {
        self.get_json(&format!("/api/stores/{sid}/groups")).await
    }

    /// `GET /api/stores/:store_id/groups/:group_id`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn get_group(&self, sid: u64, gid: u64) -> Result<GroupView> {
        self.get_json(&format!("/api/stores/{sid}/groups/{gid}")).await
    }

    /// `POST /api/stores/:store_id/groups`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn add_group(&self, sid: u64, body: &CreateGroupBody) -> Result<Value> {
        self.post_json(&format!("/api/stores/{sid}/groups"), body).await
    }

    /// `DELETE /api/stores/:store_id/groups/:group_id`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn remove_group(&self, sid: u64, gid: u64) -> Result<()> {
        self.delete_path(&format!("/api/stores/{sid}/groups/{gid}")).await
    }

    // ── Logical replica plane ──────────────────────────────────────

    /// `GET /api/stores/:s/groups/:g/replicas`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn list_replicas(&self, sid: u64, gid: u64) -> Result<Vec<ReplicaView>> {
        self.get_json(&format!("/api/stores/{sid}/groups/{gid}/replicas")).await
    }

    /// `GET /api/stores/:s/groups/:g/replicas/:rid`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn get_replica(&self, sid: u64, gid: u64, rid: u64) -> Result<ReplicaView> {
        self.get_json(&format!("/api/stores/{sid}/groups/{gid}/replicas/{rid}")).await
    }

    /// `POST /api/stores/:s/groups/:g/replicas`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn add_replica(&self, sid: u64, gid: u64, body: &AddReplicaBody) -> Result<Value> {
        self.post_json(&format!("/api/stores/{sid}/groups/{gid}/replicas"), body).await
    }

    /// `DELETE /api/stores/:s/groups/:g/replicas/:rid`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn remove_replica(&self, sid: u64, gid: u64, rid: u64) -> Result<()> {
        self.delete_path(&format!("/api/stores/{sid}/groups/{gid}/replicas/{rid}")).await
    }

    // ── KV data plane ──────────────────────────────────────────────

    /// `GET /api/stores/:s/groups/:g/kv/get?key=…|key_hex=…`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn kv_get(&self, sid: u64, gid: u64, key: &[u8]) -> Result<KvGetResponse> {
        let path = format!("/api/stores/{sid}/groups/{gid}/kv/get?key_hex={}", hex::encode(key));
        self.get_json(&path).await
    }

    /// `POST /api/stores/:s/groups/:g/kv/put`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn kv_put(&self, sid: u64, gid: u64, key: &[u8], value: &[u8], client_id: u64, seq: u64) -> Result<KvWriteResponse> {
        let body = KvWriteBody {
            key: None,
            key_hex: Some(hex::encode(key)),
            value: None,
            value_hex: Some(hex::encode(value)),
            client_id,
            seq,
        };
        self.post_json(&format!("/api/stores/{sid}/groups/{gid}/kv/put"), &body).await
    }

    /// `POST /api/stores/:s/groups/:g/kv/delete`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn kv_delete(&self, sid: u64, gid: u64, key: &[u8], client_id: u64, seq: u64) -> Result<KvWriteResponse> {
        let body = KvWriteBody {
            key: None,
            key_hex: Some(hex::encode(key)),
            value: None,
            value_hex: None,
            client_id,
            seq,
        };
        self.post_json(&format!("/api/stores/{sid}/groups/{gid}/kv/delete"), &body).await
    }

    /// `GET /api/stores/:s/groups/:g/kv/scan?prefix_hex=…&limit=N`.
    ///
    /// # Errors
    /// Transport or non-2xx errors surface as `Error::UpstreamRpc`.
    pub async fn kv_scan(&self, sid: u64, gid: u64, prefix: &[u8], limit: u32) -> Result<KvScanResponse> {
        let path = format!("/api/stores/{sid}/groups/{gid}/kv/scan?prefix_hex={}&limit={limit}", hex::encode(prefix));
        self.get_json(&path).await
    }

    // ── HTTP plumbing ──────────────────────────────────────────────
    //
    // Every helper attaches the current `x-crowkv-corr-id` header (see
    // `crate::corr_id`) and emits one `ops_log::append_http` record on
    // completion. The id comes from the task-local if a `corr_id::scope`
    // wraps the call (web handler / CLI main), otherwise we generate
    // one inline so unit tests still produce well-formed log records.

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{path}", self.base_url);
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self.inner.get(&url).header(crate::corr_id::HEADER, &cid).send().await.map_err(|e| {
            crate::ops_log::append_http(&cid, "GET", &url, 0, started.elapsed().as_millis(), Some(&format!("transport error: {e}")));
            self.rpc_err(format!("GET {path}: {e}"))
        })?;
        let status = resp.status();
        crate::ops_log::append_http(&cid, "GET", &url, status.as_u16(), started.elapsed().as_millis(), None);
        self.decode(resp, path).await
    }

    async fn post_json<B: Serialize, T: serde::de::DeserializeOwned>(&self, path: &str, body: &B) -> Result<T> {
        let url = format!("{}{path}", self.base_url);
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self.inner.post(&url).header(crate::corr_id::HEADER, &cid).json(body).send().await.map_err(|e| {
            crate::ops_log::append_http(&cid, "POST", &url, 0, started.elapsed().as_millis(), Some(&format!("transport error: {e}")));
            self.rpc_err(format!("POST {path}: {e}"))
        })?;
        let status = resp.status();
        crate::ops_log::append_http(&cid, "POST", &url, status.as_u16(), started.elapsed().as_millis(), None);
        self.decode(resp, path).await
    }

    async fn delete_path(&self, path: &str) -> Result<()> {
        let url = format!("{}{path}", self.base_url);
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self.inner.delete(&url).header(crate::corr_id::HEADER, &cid).send().await.map_err(|e| {
            crate::ops_log::append_http(&cid, "DELETE", &url, 0, started.elapsed().as_millis(), Some(&format!("transport error: {e}")));
            self.rpc_err(format!("DELETE {path}: {e}"))
        })?;
        let status = resp.status();
        crate::ops_log::append_http(&cid, "DELETE", &url, status.as_u16(), started.elapsed().as_millis(), None);
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("DELETE {path}: HTTP {status}: {body}")));
        }
        Ok(())
    }

    async fn decode<T: serde::de::DeserializeOwned>(&self, resp: reqwest::Response, path: &str) -> Result<T> {
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("{path}: HTTP {status}: {body}")));
        }
        resp.json::<T>().await.map_err(|e| self.rpc_err(format!("{path}: decode: {e}")))
    }

    fn rpc_err(&self, status: impl Into<String>) -> Error {
        Error::UpstreamRpc {
            node_id: self.base_url.clone(),
            status: status.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ConsoleClient;

    #[test]
    fn trims_trailing_slashes_in_base_url() {
        let c = ConsoleClient::new("http://127.0.0.1:9920///").unwrap();
        assert_eq!(c.base_url(), "http://127.0.0.1:9920");
    }
}
