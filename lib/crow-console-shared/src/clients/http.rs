// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP client for `crow-kv-server`'s management API.
//!
//! Endpoints used in C1: `/health`, `/topology`. Other endpoints are
//! exposed as additional methods so later phases (`store add`, etc.) can
//! reuse the same client.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::snapshot::{HealthInfo, StoreView, TopologyResponse};

/// Thin wrapper around a `reqwest::Client` bound to one `crow-kv-server`
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
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Borrow the underlying `reqwest::Client`. Used by sibling modules
    /// (e.g. `crate::mgmt`) to issue POST/DELETE requests without
    /// reinstantiating a client.
    #[must_use]
    pub fn inner(&self) -> &reqwest::Client {
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
                crate::ops_log::append_http(
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
        crate::ops_log::append_http(
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
