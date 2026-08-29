// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Typed bindings for `crowdb-kv-server`'s management API (C5).
//!
//! The HTTP request/response DTOs live in `crowdb-protocol::mgmt` (the
//! single home for cross-component protocol types). This module re-
//! exports them and adds `async` helper methods on [`ServerClient`]
//! that both the CLI and the web backend call, so the wire contract
//! stays in one place.

// Re-export all DTOs so existing callers (`use crate::mgmt::*`) keep
// working until Stage 4 migrates them to `KVClusterAdmin`.
pub use crowdb_protocol::mgmt::*;

use crate::clients::http::ServerClient;
use crate::error::{Error, Result};

// ── Client methods ─────────────────────────────────────────────────

impl ServerClient {
    /// `GET /stores`.
    ///
    /// # Errors
    /// Transport / decode failures bubble up as `Error::UpstreamRpc`.
    pub async fn list_stores(&self) -> Result<Vec<StoreSummary>> {
        let r: StoreListResponse = self.get_json("/stores").await?;
        Ok(r.stores)
    }

    /// `GET /stores/{sid}`.
    ///
    /// # Errors
    /// `Error::NotFound`-style mapping is deferred to callers; server
    /// errors surface as `Error::UpstreamRpc`.
    pub async fn get_store(&self, sid: u64) -> Result<StoreDetail> {
        self.get_json(&format!("/stores/{sid}")).await
    }

    /// `POST /stores`.
    ///
    /// # Errors
    /// Surfaces the server's structured error response as
    /// `Error::UpstreamRpc`.
    pub async fn add_store(&self, req: &AddStoreRequest) -> Result<StoreSummary> {
        self.post_json("/stores", req).await
    }

    /// `DELETE /stores/{sid}`.
    ///
    /// # Errors
    /// Transport or non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn remove_store(&self, sid: u64) -> Result<()> {
        self.delete_path(&format!("/stores/{sid}")).await
    }

    /// `GET /stores/{sid}/groups`.
    ///
    /// # Errors
    /// Transport / decode failures surface as `Error::UpstreamRpc`.
    pub async fn list_groups(&self, sid: u64) -> Result<Vec<GroupSummary>> {
        self.get_json(&format!("/stores/{sid}/groups")).await
    }

    /// `POST /stores/{sid}/groups`.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn add_group(&self, sid: u64, req: &AddGroupRequest) -> Result<()> {
        self.post_empty(&format!("/stores/{sid}/groups"), req).await
    }

    /// `DELETE /stores/{sid}/groups/{gid}`.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn remove_group(&self, sid: u64, gid: u64) -> Result<()> {
        self.delete_path(&format!("/stores/{sid}/groups/{gid}")).await
    }

    /// `GET /stores/{sid}/groups/{gid}/remotes`.
    ///
    /// # Errors
    /// Transport / decode failures surface as `Error::UpstreamRpc`.
    pub async fn list_remote_replicas(&self, sid: u64, gid: u64) -> Result<Vec<RemoteReplicaInfo>> {
        let r: RemoteListResponse = self
            .get_json(&format!("/stores/{sid}/groups/{gid}/remotes"))
            .await?;
        Ok(r.remotes)
    }

    /// `POST /stores/{sid}/groups/{gid}/remotes` — add one-or-more
    /// remote replicas in a single call.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn add_remote_replicas(&self, sid: u64, gid: u64, remotes: &[RemoteReplicaInfo]) -> Result<()> {
        self.post_empty(&format!("/stores/{sid}/groups/{gid}/remotes"), remotes)
            .await
    }

    /// `DELETE /stores/{sid}/groups/{gid}/remotes/{rid}`.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn remove_remote_replica(&self, sid: u64, gid: u64, rid: u64) -> Result<()> {
        self.delete_path(&format!("/stores/{sid}/groups/{gid}/remotes/{rid}"))
            .await
    }

    /// `POST /stores/{sid}/groups/{gid}/step-down`. Asks the node hosting
    /// this group to step down if it is currently leader. `accepted:
    /// false` in the result means the target was not leader (not an
    /// error) -- callers should treat that as "nothing to do" rather
    /// than retry.
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn step_down(&self, sid: u64, gid: u64, req: &StepDownRequest) -> Result<StepDownResult> {
        self.post_json(&format!("/stores/{sid}/groups/{gid}/step-down"), req)
            .await
    }

    /// `POST /system/init` — bootstrap the system group (store 0, group 0).
    ///
    /// # Errors
    /// Transport / non-2xx status codes surface as `Error::UpstreamRpc`.
    pub async fn system_init(&self, req: &SystemInitRequest) -> Result<SystemInitResponse> {
        self.post_json("/system/init", req).await
    }

    // ── Transport helpers shared by mgmt methods ────────────────────

    async fn post_json<B: serde::Serialize + ?Sized, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = format!("{}{path}", self.base_url());
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self
            .inner()
            .post(&url)
            .header(crate::corr_id::HEADER, &cid)
            .json(body)
            .send()
            .await
            .map_err(|e| {
                crate::ops_log::append_http(
                    &cid,
                    "POST",
                    &url,
                    0,
                    started.elapsed().as_millis(),
                    Some(&format!("transport error: {e}")),
                );
                self.rpc_err(format!("POST {path}: {e}"))
            })?;
        let status = resp.status();
        crate::ops_log::append_http(
            &cid,
            "POST",
            &url,
            status.as_u16(),
            started.elapsed().as_millis(),
            None,
        );
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("POST {path}: HTTP {status}: {text}")));
        }
        resp.json::<T>()
            .await
            .map_err(|e| self.rpc_err(format!("POST {path}: decode: {e}")))
    }

    async fn post_empty<B: serde::Serialize + ?Sized>(&self, path: &str, body: &B) -> Result<()> {
        let url = format!("{}{path}", self.base_url());
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self
            .inner()
            .post(&url)
            .header(crate::corr_id::HEADER, &cid)
            .json(body)
            .send()
            .await
            .map_err(|e| {
                crate::ops_log::append_http(
                    &cid,
                    "POST",
                    &url,
                    0,
                    started.elapsed().as_millis(),
                    Some(&format!("transport error: {e}")),
                );
                self.rpc_err(format!("POST {path}: {e}"))
            })?;
        let status = resp.status();
        crate::ops_log::append_http(
            &cid,
            "POST",
            &url,
            status.as_u16(),
            started.elapsed().as_millis(),
            None,
        );
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("POST {path}: HTTP {status}: {text}")));
        }
        Ok(())
    }

    async fn delete_path(&self, path: &str) -> Result<()> {
        let url = format!("{}{path}", self.base_url());
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self
            .inner()
            .delete(&url)
            .header(crate::corr_id::HEADER, &cid)
            .send()
            .await
            .map_err(|e| {
                crate::ops_log::append_http(
                    &cid,
                    "DELETE",
                    &url,
                    0,
                    started.elapsed().as_millis(),
                    Some(&format!("transport error: {e}")),
                );
                self.rpc_err(format!("DELETE {path}: {e}"))
            })?;
        let status = resp.status();
        crate::ops_log::append_http(
            &cid,
            "DELETE",
            &url,
            status.as_u16(),
            started.elapsed().as_millis(),
            None,
        );
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(self.rpc_err(format!("DELETE {path}: HTTP {status}: {text}")));
        }
        Ok(())
    }

    fn rpc_err(&self, status: impl Into<String>) -> Error {
        Error::UpstreamRpc {
            node_id: self.base_url().to_string(),
            status: status.into(),
        }
    }
}
