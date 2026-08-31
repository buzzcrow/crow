// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP client for `crowdb-kv-server`'s management API.
//!
//! Endpoints used in C1: `/health`, `/topology`. Other endpoints are
//! exposed as additional methods so later phases (`store add`, etc.) can
//! reuse the same client.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::snapshot::{HealthInfo, MetricsResponse, StoreView, TopologyResponse, WipeResult};

/// Thin wrapper around a `reqwest::Client` bound to one `crowdb-kv-server`
/// management base URL (e.g. `http://127.0.0.1:9910`).
#[derive(Debug, Clone)]
pub struct ServerClient {
    base_url: String,
    inner: reqwest::Client,
}

impl ServerClient {
    /// Build a new client. `base_url` may include or omit a trailing slash.
    ///
    /// # Errors
    /// Fails if the underlying `reqwest::Client` cannot be constructed.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let mut base = base_url.into();
        while base.ends_with('/') {
            base.pop();
        }
        let inner = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| Error::UpstreamRpc {
                node_id: base.clone(),
                status: format!("client build failed: {e}"),
            })?;
        Ok(Self {
            base_url: base,
            inner,
        })
    }

    #[must_use]
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Borrow the underlying `reqwest::Client`. Used by sibling modules
    /// (e.g. `crate::mgmt`) to issue POST/DELETE requests without
    /// reinstantiating a client.
    #[must_use]
    pub(crate) fn inner(&self) -> &reqwest::Client {
        &self.inner
    }

    /// `GET /health`.
    ///
    /// # Errors
    /// Returns `Error::UpstreamRpc` for transport / decode failures.
    pub async fn health(&self) -> Result<HealthInfo> {
        self.get_json("/health").await
    }

    /// `GET /topology`.
    ///
    /// # Errors
    /// Returns `Error::UpstreamRpc` for transport / decode failures.
    pub async fn topology(&self) -> Result<Vec<StoreView>> {
        let resp: TopologyResponse = self.get_json("/topology").await?;
        Ok(resp.stores)
    }

    /// `GET /metrics?prefix=...`. Returns a structured snapshot of the
    /// server's registry metrics. `prefix` filters by metric name; pass
    /// an empty string for all metrics.
    ///
    /// # Errors
    /// Returns `Error::UpstreamRpc` for transport / decode failures.
    pub async fn metrics(&self, prefix: &str) -> Result<MetricsResponse> {
        let path = if prefix.is_empty() {
            "/metrics".to_string()
        } else {
            let mut q = form_urlencoded::Serializer::new(String::from("/metrics?"));
            q.append_pair("prefix", prefix);
            q.finish()
        };
        self.get_json(&path).await
    }

    /// `POST /stores/{sid}/groups/{gid}/flush` — drain the local
    /// replica's L0 memtable into L1 on this node. Used by the bench's
    /// `--flush-after-prepopulate` flag and as an admin drain. Returns
    /// `Ok(())` on a 2xx response.
    ///
    /// # Errors
    /// Returns `Error::UpstreamRpc` for transport failures or a non-2xx
    /// response (e.g. 404 when the store/group is not hosted here).
    pub async fn flush(&self, store_id: u64, group_id: u64) -> Result<()> {
        let path = format!("/stores/{store_id}/groups/{group_id}/flush");
        let url = format!("{}{path}", self.base_url);
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self
            .inner
            .post(&url)
            .header(crate::corr_id::HEADER, &cid)
            .send()
            .await
            .map_err(|e| {
                super::log_ops_http(
                    &cid,
                    "POST",
                    &url,
                    0,
                    started.elapsed().as_millis(),
                    Some(&format!("transport error: {e}")),
                );
                Error::UpstreamRpc {
                    node_id: self.base_url.clone(),
                    status: format!("POST {path}: {e}"),
                }
            })?;
        let status = resp.status();
        super::log_ops_http(
            &cid,
            "POST",
            &url,
            status.as_u16(),
            started.elapsed().as_millis(),
            None,
        );
        if !status.is_success() {
            return Err(Error::UpstreamRpc {
                node_id: self.base_url.clone(),
                status: format!("POST {path}: HTTP {status}"),
            });
        }
        Ok(())
    }

    /// `POST /stores/{sid}/groups/{gid}/wipe-user-data` — drop and
    /// recreate the WAL + engine user data for the group on this node.
    /// Used by `bench kv clean` to reset a cluster between write
    /// sub-tests without a full redeploy.
    ///
    /// # Errors
    /// Returns `Error::UpstreamRpc` for transport failures or a non-2xx
    /// response.
    pub async fn wipe_user_data(&self, store_id: u64, group_id: u64) -> Result<WipeResult> {
        let path = format!("/stores/{store_id}/groups/{group_id}/wipe-user-data");
        let url = format!("{}{path}", self.base_url);
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self
            .inner
            .post(&url)
            .header(crate::corr_id::HEADER, &cid)
            .send()
            .await
            .map_err(|e| {
                super::log_ops_http(
                    &cid,
                    "POST",
                    &url,
                    0,
                    started.elapsed().as_millis(),
                    Some(&format!("transport error: {e}")),
                );
                Error::UpstreamRpc {
                    node_id: self.base_url.clone(),
                    status: format!("POST {path}: {e}"),
                }
            })?;
        let status = resp.status();
        super::log_ops_http(
            &cid,
            "POST",
            &url,
            status.as_u16(),
            started.elapsed().as_millis(),
            None,
        );
        if !status.is_success() {
            return Err(Error::UpstreamRpc {
                node_id: self.base_url.clone(),
                status: format!("POST {path}: HTTP {status}"),
            });
        }
        resp.json::<WipeResult>().await.map_err(|e| Error::UpstreamRpc {
            node_id: self.base_url.clone(),
            status: format!("POST {path}: decode error: {e}"),
        })
    }

    pub(crate) async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{path}", self.base_url);
        let cid = crate::corr_id::current_or_new();
        let started = std::time::Instant::now();
        let resp = self
            .inner
            .get(&url)
            .header(crate::corr_id::HEADER, &cid)
            .send()
            .await
            .map_err(|e| {
                super::log_ops_http(
                    &cid,
                    "GET",
                    &url,
                    0,
                    started.elapsed().as_millis(),
                    Some(&format!("transport error: {e}")),
                );
                Error::UpstreamRpc {
                    node_id: self.base_url.clone(),
                    status: format!("GET {path}: {e}"),
                }
            })?;
        let status = resp.status();
        super::log_ops_http(
            &cid,
            "GET",
            &url,
            status.as_u16(),
            started.elapsed().as_millis(),
            None,
        );
        if !status.is_success() {
            return Err(Error::UpstreamRpc {
                node_id: self.base_url.clone(),
                status: format!("GET {path}: HTTP {status}"),
            });
        }
        resp.json::<T>().await.map_err(|e| Error::UpstreamRpc {
            node_id: self.base_url.clone(),
            status: format!("GET {path}: decode: {e}"),
        })
    }
}
