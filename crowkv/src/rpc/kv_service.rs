//! Tonic `KvService` implementation that delegates to `KvStore`.
//!
//! Most KV RPCs are forwarded to the node's stub methods so that the
//! wire-format handling stays in the `rpc` module while the real logic
//! lives next to `KvStore`. The `Get` and `Scan` handlers additionally
//! perform **transparent leader-forwarding**: when this node is not
//! the group's leader and the leader's endpoint is known, the request
//! is re-issued against the leader before falling back to a local
//! learner read. A loop-guard metadata header (`x-crowkv-forwarded`)
//! prevents an infinite forwarding chain if upstream cluster info is
//! transiently inconsistent.

use crate::cluster::kv_store::KvStore;
use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::kv_service_client::KvServiceClient;
use crate::rpc::kv_service_server::KvService;
use crate::rpc::{
    KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvResponse, KvScanRequest, KvScanResponse,
    KvSetRequest,
};
use std::sync::{Arc, OnceLock};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

/// Loop-guard header. The `Get`/`Scan` forwarder sets this to `"1"`
/// before re-issuing a request against the leader. The receiving
/// handler sees the header and skips its own forward step, serving
/// the request from the local store regardless of leader status. This
/// guarantees forwarding terminates after at most one hop.
const FORWARD_HEADER: &str = "x-crowkv-forwarded";

use dashmap::DashMap;

/// Process-wide cache of tonic `Channel`s keyed by leader endpoint
/// (`host:port`). `Channel` is a thin Arc handle that multiplexes
/// HTTP/2 streams, so cloning is cheap; the cache only saves the
/// initial TCP+TLS+HTTP/2 handshake on subsequent forwards.
fn forward_channel_cache() -> &'static DashMap<String, Channel> {
    static CACHE: OnceLock<DashMap<String, Channel>> = OnceLock::new();
    CACHE.get_or_init(DashMap::new)
}

async fn forward_channel(endpoint: &str) -> Result<Channel, Status> {
    let cache = forward_channel_cache();
    if let Some(entry) = cache.get(endpoint) {
        return Ok(entry.clone());
    }
    let ch = Endpoint::from_shared(format!("http://{endpoint}"))
        .map_err(|e| Status::invalid_argument(format!("bad leader endpoint {endpoint}: {e}")))?
        .connect()
        .await
        .map_err(|e| Status::unavailable(format!("connect leader {endpoint}: {e}")))?;
    Ok(cache.entry(endpoint.to_string()).or_insert(ch).clone())
}

fn forward_header_set<T>(req: &mut Request<T>) {
    // `"1"` is a static ASCII value, so the parse cannot fail.
    let v: MetadataValue<_> = "1".parse().expect("static metadata value");
    req.metadata_mut().insert(FORWARD_HEADER, v);
}

#[derive(Clone)]
pub struct KvStoreService {
    store: Arc<PxKvStore>,
}

impl KvStoreService {
    pub fn new(store: Arc<PxKvStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl KvService for KvStoreService {
    async fn put(&self, request: Request<KvSetRequest>) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            client_id = req.client_id,
            seq = req.seq,
            key_len = req.key.len(),
            value_len = req.value.len(),
            "received kv put rpc"
        );
        let mut resp = self
            .store
            .kv_put(
                req.group_id,
                &req.key,
                &req.value,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                store_id = self.store.store_id,
                group_id = req.group_id,
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

    async fn get(&self, request: Request<KvGetRequest>) -> Result<Response<KvResponse>, Status> {
        let already_forwarded = request.metadata().get(FORWARD_HEADER).is_some();
        let req = request.into_inner();
        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            key_len = req.key.len(),
            forwarded_in = already_forwarded,
            "received kv get rpc"
        );

        // Transparent leader-forward: only linearizable reads are forwarded
        // to the leader; the stale read modes (`READ_YOUR_WRITES`,
        // `BOUNDED_STALE`, `BEST_EFFORT`) are deliberately served from the
        // local replica without a hop. The loop-guard header makes the
        // linearizable hop at-most-once.
        let linearizable = req.read_mode == crate::rpc::ReadMode::Linearizable as i32;
        if linearizable && !already_forwarded {
            if let Some(endpoint) = self.store.forward_target_for(req.group_id) {
                match forward_kv_get(&endpoint, req.clone()).await {
                    Ok(mut resp) => {
                        debug!(
                            store_id = self.store.store_id,
                            group_id = req.group_id,
                            request_id = req.request_id,
                            leader = %endpoint,
                            "kv get forwarded to leader"
                        );
                        resp.request_id = req.request_id;
                        resp.request_create_ms = req.request_create_ms;
                        return Ok(Response::new(resp));
                    }
                    Err(status) => {
                        warn!(
                            store_id = self.store.store_id,
                            group_id = req.group_id,
                            request_id = req.request_id,
                            leader = %endpoint,
                            error = %status,
                            "kv get forward failed; next step: returning the store decision (linearizable reads redirect with not_leader_hint rather than serve stale)"
                        );
                        let mut resp = self
                            .store
                            .kv_get(
                                req.group_id,
                                &req.key,
                                req.read_mode,
                                req.client_slot,
                                req.request_id,
                                req.request_create_ms,
                            )
                            .await;
                        resp.not_leader_hint = endpoint;
                        resp.request_id = req.request_id;
                        resp.request_create_ms = req.request_create_ms;
                        return Ok(Response::new(resp));
                    }
                }
            }
        }

        let mut resp = self
            .store
            .kv_get(
                req.group_id,
                &req.key,
                req.read_mode,
                req.client_slot,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        resp.request_id = req.request_id;
        resp.request_create_ms = req.request_create_ms;
        Ok(Response::new(resp))
    }

    async fn delete(&self, request: Request<KvDeleteRequest>) -> Result<Response<KvResponse>, Status> {
        let req = request.into_inner();
        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            client_id = req.client_id,
            seq = req.seq,
            key_len = req.key.len(),
            "received kv delete rpc"
        );
        let mut resp = self
            .store
            .kv_delete(
                req.group_id,
                &req.key,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                store_id = self.store.store_id,
                group_id = req.group_id,
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

    async fn scan(&self, request: Request<KvScanRequest>) -> Result<Response<KvScanResponse>, Status> {
        let already_forwarded = request.metadata().get(FORWARD_HEADER).is_some();
        let req = request.into_inner();
        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            prefix_len = req.prefix.len(),
            limit = req.limit,
            forwarded_in = already_forwarded,
            "received kv scan rpc"
        );

        // Transparent leader-forward, mirroring `get`: only linearizable
        // scans hop to the leader; stale modes serve from the local replica.
        let linearizable = req.read_mode == crate::rpc::ReadMode::Linearizable as i32;
        if linearizable && !already_forwarded {
            if let Some(endpoint) = self.store.forward_target_for(req.group_id) {
                match forward_kv_scan(&endpoint, req.clone()).await {
                    Ok(mut resp) => {
                        debug!(
                            store_id = self.store.store_id,
                            group_id = req.group_id,
                            request_id = req.request_id,
                            leader = %endpoint,
                            "kv scan forwarded to leader"
                        );
                        resp.request_id = req.request_id;
                        resp.request_create_ms = req.request_create_ms;
                        return Ok(Response::new(resp));
                    }
                    Err(status) => {
                        warn!(
                            store_id = self.store.store_id,
                            group_id = req.group_id,
                            request_id = req.request_id,
                            leader = %endpoint,
                            error = %status,
                            "kv scan forward failed; next step: serving stale local scan"
                        );
                        // Fall through to local scan. KvScanResponse has
                        // no not_leader_hint field, so the caller cannot
                        // observe the routing decision here — this is a
                        // best-effort degraded read.
                    }
                }
            }
        }

        let mut resp = self
            .store
            .kv_scan(
                req.group_id,
                &req.prefix,
                req.limit,
                req.read_mode,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                store_id = self.store.store_id,
                group_id = req.group_id,
                request_id = req.request_id,
                error = resp.error,
                "kv scan failed; next step: confirm group exists on this server"
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
            store_id = self.store.store_id,
            group_id = req.group_id,
            request_id = req.request_id,
            client_id = req.client_id,
            seq = req.seq,
            item_count = req.items.len(),
            "received kv batch_write rpc"
        );
        let mut resp = self
            .store
            .kv_batch_write(
                req.group_id,
                req.items,
                req.client_id,
                req.seq,
                req.request_id,
                req.request_create_ms,
            )
            .await;
        if !resp.ok {
            warn!(
                store_id = self.store.store_id,
                group_id = req.group_id,
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

/// Forward a `KvGetRequest` to the group's current leader. Caller is
/// responsible for skipping this when `x-crowkv-forwarded` is already
/// set on the inbound request (see `KvService::get`).
async fn forward_kv_get(endpoint: &str, body: KvGetRequest) -> Result<KvResponse, Status> {
    let channel = forward_channel(endpoint).await?;
    let mut client = KvServiceClient::new(channel);
    let mut req = Request::new(body);
    forward_header_set(&mut req);
    let resp = client.get(req).await?;
    Ok(resp.into_inner())
}

/// Forward a `KvScanRequest` to the group's current leader. Same
/// contract as [`forward_kv_get`].
async fn forward_kv_scan(endpoint: &str, body: KvScanRequest) -> Result<KvScanResponse, Status> {
    let channel = forward_channel(endpoint).await?;
    let mut client = KvServiceClient::new(channel);
    let mut req = Request::new(body);
    forward_header_set(&mut req);
    let resp = client.scan(req).await?;
    Ok(resp.into_inner())
}
