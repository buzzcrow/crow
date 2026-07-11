#![allow(clippy::cast_possible_truncation)]

use crate::cluster::group::{ProposeResult, PxGroup};
use crate::cluster::health::HealthReport;
use crate::cluster::kv_server::GrpcTaskState;
use crate::cluster::kv_store::KvStore;
use crate::cluster::shutdown::ShutdownReport;
use crate::cluster::snapshot::StoreSnapshot;
use crate::common::optional_u64;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info};

pub struct PxKvStore {
    pub store_id: u64,
    pub(crate) groups: DashMap<u64, Arc<PxGroup>>,
    pub(crate) server_state: Mutex<GrpcTaskState>,
    pub(crate) listen_addr: SocketAddr,
    /// Set the first time `shutdown()` is invoked. Subsequent calls are no-ops.
    shutdown_started: AtomicBool,
}

impl KvStore for PxKvStore {
    async fn kv_get(&self, group_id: u64, key: Vec<u8>, request_id: u64, request_create_ms: u64) -> crate::rpc::KvResponse {
        let Some(group) = self.get_group(group_id) else {
            return missing_group_response(request_id, request_create_ms);
        };

        let value = group.local_replica().learner.store().get(&key).map(|v| v.value().clone());

        match value {
            Some(v) => crate::rpc::KvResponse {
                version: 1,
                ok: true,
                revision: 0, // TODO: track revision for reads
                error: String::new(),
                not_found: false,
                not_leader_hint: String::new(),
                request_id,
                request_create_ms,
                value: v,
            },
            None => crate::rpc::KvResponse {
                version: 1,
                ok: false,
                revision: 0,
                error: String::new(),
                not_found: true,
                not_leader_hint: String::new(),
                request_id,
                request_create_ms,
                value: Vec::new(),
            },
        }
    }

    async fn kv_put(&self, group_id: u64, key: Vec<u8>, value: Vec<u8>, client_id: u64, seq: u64, request_id: u64, request_create_ms: u64) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&[(key, Some(value))]);
        self.propose_and_respond(group_id, payload, optional_u64(client_id), Some(seq), request_id, request_create_ms)
            .await
    }
    async fn kv_delete(&self, group_id: u64, key: Vec<u8>, client_id: u64, seq: u64, request_id: u64, request_create_ms: u64) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&[(key, None)]);
        self.propose_and_respond(group_id, payload, optional_u64(client_id), Some(seq), request_id, request_create_ms)
            .await
    }
    async fn kv_batch_write(
        &self,
        group_id: u64,
        items: Vec<crate::rpc::KvBatchItem>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_batch_items(&items);
        self.propose_and_respond(group_id, payload, optional_u64(client_id), Some(seq), request_id, request_create_ms)
            .await
    }
}

impl PxKvStore {
    #[must_use]
    pub fn new(store_id: u64, listen_addr: SocketAddr) -> Self {
        Self {
            store_id,
            groups: DashMap::new(),
            server_state: Mutex::new(GrpcTaskState::default()),
            listen_addr,
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Cascade shutdown: stop gRPC server (with timeout), then shut down each
    /// group. Idempotent; second+ calls return an empty clean report.
    ///
    /// Continue on errors, aggregate `critical:` messages into [`ShutdownReport`],
    /// force-clean on layer timeout. Caller decides what to do with a non-clean
    /// report (typically: surface to operator, fail health check).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(store_id = self.store_id, timeout_ms = per_layer_timeout.as_millis() as u64)
    )]
    pub async fn shutdown(&self, per_layer_timeout: Duration) -> ShutdownReport {
        // Idempotency gate: only the first caller proceeds.
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(store_id = self.store_id, "PxKvStore::shutdown is a no-op (already shut down)");
            return ShutdownReport::new();
        }

        info!(
            store_id = self.store_id,
            group_count = self.groups.len(),
            timeout_ms = per_layer_timeout.as_millis() as u64,
            "PxKvStore shutdown starting"
        );

        let mut report = ShutdownReport::new();

        // 1. Stop gRPC server first so no new requests reach the groups.
        if let Err(msg) = self.shutdown_server(per_layer_timeout).await {
            report.push_error(msg);
        }

        // 2. Cascade into each group. Continue on errors.
        for entry in &self.groups {
            let group_id = *entry.key();
            let group = entry.value();
            info!(store_id = self.store_id, group_id, "shutting down PxGroup");
            let sub = group.shutdown(per_layer_timeout).await;
            if !sub.is_clean() {
                debug!(store_id = self.store_id, group_id, error_count = sub.errors.len(), "PxGroup shutdown reported errors");
            }
            report.merge(sub);
        }

        if report.is_clean() {
            info!(store_id = self.store_id, "PxKvStore shutdown complete");
        } else {
            info!(
                store_id = self.store_id,
                error_count = report.errors.len(),
                "PxKvStore shutdown complete with errors (see critical: logs above)"
            );
        }
        report
    }

    /// Hierarchical point-in-time snapshot for `/topology`.
    /// Composes group snapshots; cheap.
    ///
    /// # Panics
    /// If the internal `server_state` mutex is poisoned (i.e. another thread
    /// panicked while holding it). Treat as unrecoverable.
    #[must_use]
    pub fn snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            store_id: self.store_id,
            listen_addr: self.server_state.lock().unwrap().listen_addr,
            groups: self.groups.iter().map(|entry| entry.value().snapshot()).collect(),
        }
    }

    /// Aggregate cached health for this store: server liveness + each group.
    ///
    /// - `Unhealthy` if `shutdown()` has run or the gRPC server is not active.
    /// - Otherwise worst-of across groups.
    ///
    /// # Panics
    /// If the internal `server_state` mutex is poisoned. Treat as unrecoverable.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        let mut report = HealthReport::ok();

        if self.shutdown_started.load(Ordering::Acquire) {
            return HealthReport::unhealthy(format!("store {} has been shut down", self.store_id));
        }

        // gRPC server liveness — listen_addr is set by start() and cleared by
        // shutdown_server() taking the JoinHandle.
        let server_running = self.server_state.lock().unwrap().handle.is_some();
        if !server_running {
            return HealthReport::unhealthy(format!("store {}: gRPC server not running", self.store_id));
        }

        for entry in &self.groups {
            let group_id = *entry.key();
            let sub = entry.value().health();
            report.merge_child(&format!("group#{group_id}"), sub);
        }
        report
    }

    pub fn add_group(&self, group: PxGroup) {
        let group_id = group.group_id;
        info!(store_id = self.store_id, group_id, replicas = group.remote_replica_count(), "added group to kv store");
        self.groups.insert(group_id, Arc::new(group));
    }

    pub fn get_group(&self, group_id: u64) -> Option<Arc<PxGroup>> {
        self.groups.get(&group_id).map(|r| r.clone())
    }

    pub fn remove_group(&self, group_id: u64) -> bool {
        self.groups.remove(&group_id).is_some()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Return `(group_id, local_replica_id, leader_id, remote_count)` for all groups.
    pub fn group_summaries(&self) -> Vec<(u64, u64, u64, usize)> {
        self.groups
            .iter()
            .map(|entry| {
                let group = entry.value();
                (group.group_id, group.local_replica().id, group.leader_id, group.remote_replica_info().len())
            })
            .collect()
    }

    // ── KV operations ─────────────────────────────────────────

    async fn propose_and_respond(
        &self,
        group_id: u64,
        payload: Vec<u8>,
        client_id: Option<u64>,
        seq: Option<u64>,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let Some(group) = self.get_group(group_id) else {
            return missing_group_response(request_id, request_create_ms);
        };

        match group.propose(payload, client_id, seq).await {
            ProposeResult::Chosen { slot } => crate::rpc::KvResponse {
                version: 1,
                ok: true,
                revision: slot,
                error: String::new(),
                not_found: false,
                not_leader_hint: String::new(),
                request_id,
                request_create_ms,
                value: Vec::new(),
            },
            ProposeResult::NotLeader { leader_hint } => crate::rpc::KvResponse {
                version: 1,
                ok: false,
                revision: 0,
                error: "not leader".to_string(),
                not_found: false,
                not_leader_hint: leader_hint,
                request_id,
                request_create_ms,
                value: Vec::new(),
            },
            ProposeResult::Err(msg) => crate::rpc::KvResponse {
                version: 1,
                ok: false,
                revision: 0,
                error: msg,
                not_found: false,
                not_leader_hint: String::new(),
                request_id,
                request_create_ms,
                value: Vec::new(),
            },
        }
    }

    // ── KV payload encoding ───────────────────────────────────

    fn encode_kv_payload(ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(ops.len() as u8);
        for (key, value_opt) in ops {
            buf.push(u8::from(value_opt.is_none()));
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            let value_len = value_opt.as_ref().map_or(0, Vec::len) as u32;
            buf.extend_from_slice(&value_len.to_le_bytes());
            if let Some(value) = value_opt {
                buf.extend_from_slice(value);
            }
        }
        buf
    }

    fn encode_kv_batch_items(items: &[crate::rpc::KvBatchItem]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(items.len() as u8);
        for item in items {
            buf.push(u8::from(item.is_delete));
            buf.extend_from_slice(&(item.key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&item.key);
            let value_len = if item.is_delete { 0 } else { item.value.len() as u32 };
            buf.extend_from_slice(&value_len.to_le_bytes());
            if !item.is_delete {
                buf.extend_from_slice(&item.value);
            }
        }
        buf
    }
}

fn missing_group_response(request_id: u64, request_create_ms: u64) -> crate::rpc::KvResponse {
    crate::rpc::KvResponse {
        version: 1,
        ok: false,
        revision: 0,
        error: "no kv group configured for request group_id".to_string(),
        not_found: false,
        not_leader_hint: String::new(),
        request_id,
        request_create_ms,
        value: Vec::new(),
    }
}
