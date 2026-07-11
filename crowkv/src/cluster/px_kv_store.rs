use crate::cluster::group::{ProposeResult, PxGroup};
use crate::cluster::kv_server::GrpcTaskState;
use crate::cluster::kv_store::KvStore;
use crate::common::optional_u64;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tracing::info;

pub struct PxKvStore {
    pub(crate) groups: DashMap<u64, Arc<PxGroup>>,
    pub(crate) server_state: Mutex<GrpcTaskState>,
    pub(crate) listen_addr: SocketAddr,
}

impl KvStore for PxKvStore {
    async fn kv_get(
        &self,
        group_id: u64,
        key: Vec<u8>,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let Some(group) = self.get_group(group_id) else {
            return missing_group_response(request_id, request_create_ms);
        };

        let value = group
            .local_replica()
            .learner
            .store()
            .get(&key)
            .map(|v| v.value().clone());

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

    async fn kv_put(
        &self,
        group_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&[(key, Some(value))]);
        self.propose_and_respond(
            group_id,
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }
    async fn kv_delete(
        &self,
        group_id: u64,
        key: Vec<u8>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&[(key, None)]);
        self.propose_and_respond(
            group_id,
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
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
        self.propose_and_respond(
            group_id,
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }
}

impl PxKvStore {
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            groups: DashMap::new(),
            server_state: Mutex::new(GrpcTaskState::default()),
            listen_addr,
        }
    }

    pub fn add_group(&self, group: PxGroup) {
        let group_id = group.group_id;
        info!(
            group_id,
            replicas = group.remote_replica_count(),
            "added group to kv store"
        );
        self.groups.insert(group_id, Arc::new(group));
    }

    pub fn get_group(&self, group_id: u64) -> Option<Arc<PxGroup>> {
        self.groups.get(&group_id).map(|r| r.clone())
    }

    pub fn remove_group(&self, group_id: u64) -> bool {
        self.groups.remove(&group_id).is_some()
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
            let value_len = if item.is_delete {
                0
            } else {
                item.value.len() as u32
            };
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
