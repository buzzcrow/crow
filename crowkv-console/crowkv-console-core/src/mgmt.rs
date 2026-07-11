//! Typed bindings for `crowkv-server`'s management API (C5).
//!
//! Mirrors the request / response JSON shapes from
//! `crowkv-server/src/management.rs` and exposes them through a small
//! set of `async` helpers built on top of [`crate::clients::http::ServerClient`].
//! Both the CLI and the web backend call into this module so the wire
//! contract stays in one place.

use serde::{Deserialize, Serialize};

use crate::clients::http::ServerClient;
use crate::error::{Error, Result};

// ── Request / response DTOs ─────────────────────────────────────────

/// `POST /stores` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddStoreRequest {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// `POST /stores/{sid}/groups` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddGroupRequest {
    pub group_id: u64,
    pub replica_id: u64,
}

/// One element of `POST /stores/{sid}/groups/{gid}/remotes` body and
/// the `GET` response. `endpoint` is the `host:port` of the remote
/// replica's gRPC service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteReplicaInfo {
    pub replica_id: u64,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSummary {
    pub store_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
    pub group_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreDetail {
    pub store_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
    #[serde(default)]
    pub groups: Vec<GroupSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSummary {
    pub group_id: u64,
    pub local_replica_id: u64,
    pub leader_id: u64,
    pub remote_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoreListResponse {
    #[serde(default)]
    stores: Vec<StoreSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteListResponse {
    #[serde(default)]
    remotes: Vec<RemoteReplicaInfo>,
}

// ── Client methods ─────────────────────────────────────────────────

impl ServerClient {
    /// `GET /stores`.
    ///
    /// # Errors
    /// Transport / decode failures bubble up as `Error::ServerRpc`.
    pub async fn list_stores(&self) -> Result<Vec<StoreSummary>> {
        let r: StoreListResponse = self.get_json("/stores").await?;
        Ok(r.stores)
    }

    /// `GET /stores/{sid}`.
    ///
    /// # Errors
    /// `Error::NotFound`-style mapping is deferred to callers; server
    /// errors surface as `Error::ServerRpc`.
    pub async fn get_store(&self, sid: u64) -> Result<StoreDetail> {
        self.get_json(&format!("/stores/{sid}")).await
    }

    /// `POST /stores`.
    ///
    /// # Errors
    /// Surfaces the server's structured error response as
    /// `Error::ServerRpc`.
    pub async fn add_store(&self, req: &AddStoreRequest) -> Result<StoreSummary> {
        self.post_json("/stores", req).await
    }

    /// `DELETE /stores/{sid}`.
    ///
    /// # Errors
    /// Transport or non-2xx status codes surface as `Error::ServerRpc`.
    pub async fn remove_store(&self, sid: u64) -> Result<()> {
        self.delete_path(&format!("/stores/{sid}")).await
    }

    /// `GET /stores/{sid}/groups`.
    ///
    /// # Errors
    /// Transport / decode failures surface as `Error::ServerRpc`.
    pub async fn list_groups(&self, sid: u64) -> Result<Vec<GroupSummary>> {
        self.get_json(&format!("/stores/{sid}/groups")).await
    }

    /// `POST /stores/{sid}/groups`.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::ServerRpc`.
    pub async fn add_group(&self, sid: u64, req: &AddGroupRequest) -> Result<()> {
        self.post_empty(&format!("/stores/{sid}/groups"), req).await
    }

    /// `DELETE /stores/{sid}/groups/{gid}`.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::ServerRpc`.
    pub async fn remove_group(&self, sid: u64, gid: u64) -> Result<()> {
        self.delete_path(&format!("/stores/{sid}/groups/{gid}")).await
    }

    /// `GET /stores/{sid}/groups/{gid}/remotes`.
    ///
    /// # Errors
    /// Transport / decode failures surface as `Error::ServerRpc`.
    pub async fn list_remotes(&self, sid: u64, gid: u64) -> Result<Vec<RemoteReplicaInfo>> {
        let r: RemoteListResponse = self.get_json(&format!("/stores/{sid}/groups/{gid}/remotes")).await?;
        Ok(r.remotes)
    }

    /// `POST /stores/{sid}/groups/{gid}/remotes` — add one-or-more
    /// remote replicas in a single call.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::ServerRpc`.
    pub async fn add_remotes(&self, sid: u64, gid: u64, remotes: &[RemoteReplicaInfo]) -> Result<()> {
        self.post_empty(&format!("/stores/{sid}/groups/{gid}/remotes"), remotes).await
    }

    /// `DELETE /stores/{sid}/groups/{gid}/remotes/{rid}`.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::ServerRpc`.
    pub async fn remove_remote(&self, sid: u64, gid: u64, rid: u64) -> Result<()> {
        self.delete_path(&format!("/stores/{sid}/groups/{gid}/remotes/{rid}")).await
    }

    // ── Transport helpers shared by mgmt methods ────────────────────

    async fn post_json<B: Serialize + ?Sized, T: serde::de::DeserializeOwned>(&self, path: &str, body: &B) -> Result<T> {
        let url = format!("{}{path}", self.base_url());
        let resp = self.inner().post(&url).json(body).send().await.map_err(|e| self.rpc_err(format!("POST {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("POST {path}: HTTP {status}: {text}")));
        }
        resp.json::<T>().await.map_err(|e| self.rpc_err(format!("POST {path}: decode: {e}")))
    }

    async fn post_empty<B: Serialize + ?Sized>(&self, path: &str, body: &B) -> Result<()> {
        let url = format!("{}{path}", self.base_url());
        let resp = self.inner().post(&url).json(body).send().await.map_err(|e| self.rpc_err(format!("POST {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("POST {path}: HTTP {status}: {text}")));
        }
        Ok(())
    }

    async fn delete_path(&self, path: &str) -> Result<()> {
        let url = format!("{}{path}", self.base_url());
        let resp = self.inner().delete(&url).send().await.map_err(|e| self.rpc_err(format!("DELETE {path}: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("DELETE {path}: HTTP {status}: {text}")));
        }
        Ok(())
    }

    fn rpc_err(&self, status: impl Into<String>) -> Error {
        Error::ServerRpc {
            server_id: self.base_url().to_string(),
            status: status.into(),
        }
    }
}
