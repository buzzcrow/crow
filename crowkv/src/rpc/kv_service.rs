//! Tonic `KvService` implementation that delegates to `PxNode`.
//!
//! All KV RPCs are forwarded to the node's stub methods so that the
//! wire-format handling stays in the `rpc` module while the real logic
//! lives next to `PxNode`.

use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::node::PxNode;
use crate::rpc::kv_service_server::KvService;
use crate::rpc::{KvBatchWriteRequest, KvDeleteRequest, KvResponse, KvSetRequest};

#[derive(Clone)]
pub struct KvNodeService {
    node: PxNode,
}

impl KvNodeService {
    pub fn new(node: PxNode) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl KvService for KvNodeService {
    async fn put(&self, request: Request<KvSetRequest>) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        debug!(
            request_id = req.request_id,
            client_id = req.client_id,
            seq = req.seq,
            key_len = req.key.len(),
            value_len = req.value.len(),
            "received kv put rpc"
        );
        let mut resp = self
            .node
            .kv_put(
                req.key,
                req.value,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                request_id = req.request_id,
                error = resp.error,
                not_leader_hint = resp.not_leader_hint,
                "kv put failed; next step: retry at hinted leader or inspect paxos logs"
            );
        }
        resp.request_id = req.request_id;
        resp.request_create_ms = req.request_create_ms;
        Ok(Response::new(resp))
    }

    async fn delete(
        &self,
        request: Request<KvDeleteRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        debug!(
            request_id = req.request_id,
            client_id = req.client_id,
            seq = req.seq,
            key_len = req.key.len(),
            "received kv delete rpc"
        );
        let mut resp = self
            .node
            .kv_delete(
                req.key,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                request_id = req.request_id,
                error = resp.error,
                not_leader_hint = resp.not_leader_hint,
                "kv delete failed; next step: retry at hinted leader or inspect paxos logs"
            );
        }
        resp.request_id = req.request_id;
        resp.request_create_ms = req.request_create_ms;
        Ok(Response::new(resp))
    }

    async fn batch_write(
        &self,
        request: Request<KvBatchWriteRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        debug!(
            request_id = req.request_id,
            client_id = req.client_id,
            seq = req.seq,
            item_count = req.items.len(),
            "received kv batch_write rpc"
        );
        let mut resp = self
            .node
            .kv_batch_write(
                req.items,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                request_id = req.request_id,
                error = resp.error,
                not_leader_hint = resp.not_leader_hint,
                "kv batch_write failed; next step: retry at hinted leader or inspect paxos logs"
            );
        }
        resp.request_id = req.request_id;
        resp.request_create_ms = req.request_create_ms;
        Ok(Response::new(resp))
    }
}
