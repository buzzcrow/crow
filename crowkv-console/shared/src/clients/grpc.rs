//! gRPC client for `crowkv-server`'s `KvService` (C6).
//!
//! Wraps the tonic-generated `KvServiceClient` so callers (CLI / web)
//! get a small, console-flavoured surface: `put`, `get`, `delete`,
//! and `scan` (prefix-scan via the server's `Scan` RPC). All reads
//! are local-replica reads in V1; the server may report stale data
//! when the replica lags the leader.
//!
//! Endpoints come from `StoreSummary::listen_addr` (returned by the
//! HTTP management API). Channels are cached process-wide keyed by URL
//! so back-to-back CLI invocations or web requests against the same
//! server reuse the underlying HTTP/2 connection (`tonic::Channel` is
//! cheap to clone — it's `Arc`-wrapped — so each `KvClient` ends up
//! owning its own logical handle while sharing the wire connection).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvDeleteRequest, KvGetRequest, KvResponse, KvScanRequest, KvSetRequest};
use tonic::transport::Channel;

use crate::error::{Error, Result};

/// Process-wide cache of established gRPC channels keyed by the
/// canonical URL form (`http://host:port`). Misses fall through to a
/// fresh `Channel::from_shared(...).connect()`; hits clone the cached
/// channel which costs an `Arc` bump.
fn channel_cache() -> &'static Mutex<HashMap<String, Channel>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Channel>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Outcome of a successful `put` / `delete`.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub revision: u64,
    pub request_id: u64,
}

/// Outcome of a successful `get`.
#[derive(Debug, Clone)]
pub enum GetOutcome {
    Found { value: Vec<u8>, revision: u64 },
    NotFound,
}

/// Outcome of a successful `scan`.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub items: Vec<(Vec<u8>, Vec<u8>)>,
    /// `true` when `limit` was reached and more keys exist past the
    /// returned slice.
    pub truncated: bool,
}

/// Connection to a single `crowkv-server`'s gRPC `KvService`.
#[derive(Debug, Clone)]
pub struct KvClient {
    inner: KvServiceClient<Channel>,
    endpoint: String,
}

impl KvClient {
    /// Connect by `host:port` (no scheme — gRPC is plain HTTP/2 in C6).
    ///
    /// # Panics
    /// Panics if the channel cache mutex is poisoned.
    ///
    /// # Errors
    /// `Error::UpstreamRpc` for transport failures.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.clone()
        } else {
            format!("http://{endpoint}")
        };

        // Cache hit fast path — clone the Arc-backed `Channel` and skip
        // the TCP+HTTP/2 handshake.
        if let Some(channel) = channel_cache()
            .lock()
            .expect("channel cache mutex")
            .get(&url)
            .cloned()
        {
            return Ok(Self {
                inner: KvServiceClient::new(channel),
                endpoint,
            });
        }

        let channel = Channel::from_shared(url.clone())
            .map_err(|e| Error::UpstreamRpc {
                node_id: endpoint.clone(),
                status: format!("invalid endpoint: {e}"),
            })?
            .connect()
            .await
            .map_err(|e| Error::UpstreamRpc {
                node_id: endpoint.clone(),
                status: format!("grpc connect: {e}"),
            })?;

        // Best-effort insert. If a racing connect won the slot we still
        // hand back our freshly-built channel; the loser's channel will
        // be dropped on the next hit. No correctness issue, only a
        // brief duplicate connection.
        channel_cache()
            .lock()
            .expect("channel cache mutex")
            .insert(url, channel.clone());

        Ok(Self {
            inner: KvServiceClient::new(channel),
            endpoint,
        })
    }

    /// Drop any cached gRPC channel for this endpoint. Useful when the
    /// caller knows the upstream restarted and we want subsequent
    /// `connect` calls to redial.
    ///
    /// # Panics
    /// Panics if the channel cache mutex is poisoned.
    pub fn invalidate_cache(endpoint: &str) {
        let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.to_string()
        } else {
            format!("http://{endpoint}")
        };
        channel_cache().lock().expect("channel cache mutex").remove(&url);
    }

    /// `Put` a single key/value. `client_id` and `seq` are passed through
    /// to the server for idempotency tracking; pass `0` if you don't
    /// care.
    ///
    /// # Errors
    /// Transport errors, or `Error::UpstreamRpc` if the server returns
    /// `ok=false`.
    pub async fn put(
        &mut self,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        client_id: u64,
        seq: u64,
    ) -> Result<WriteOutcome> {
        let request_id = next_request_id();
        let request_create_ms = now_ms();
        let req = KvSetRequest {
            version: 1,
            key: key.to_vec(),
            value: value.to_vec(),
            seq,
            ttl_ms: 0,
            client_id,
            request_id,
            request_create_ms,
            group_id,
        };
        let resp = self
            .inner
            .put(req)
            .await
            .map_err(|e| self.rpc_err(format!("put: {e}")))?
            .into_inner();
        check_ok(&resp).map(|()| WriteOutcome {
            revision: resp.revision,
            request_id: resp.request_id,
        })
    }

    /// `Get` a single key. Returns `GetOutcome::NotFound` for a missing
    /// key (not an error). Note: get is a local-only read in V1 — it
    /// does not go through Paxos and may return stale data on
    /// followers.
    ///
    /// # Errors
    /// Transport errors only.
    pub async fn get(&mut self, group_id: u64, key: &[u8]) -> Result<GetOutcome> {
        let req = KvGetRequest {
            version: 1,
            key: key.to_vec(),
            request_id: next_request_id(),
            request_create_ms: now_ms(),
            group_id,
            // Console reads default to linearizable; no read-your-writes slot.
            read_mode: 0,
            client_slot: 0,
        };
        let resp = self
            .inner
            .get(req)
            .await
            .map_err(|e| self.rpc_err(format!("get: {e}")))?
            .into_inner();
        if resp.not_found {
            return Ok(GetOutcome::NotFound);
        }
        if !resp.ok {
            return Err(self.rpc_err(format!("get: {}", resp.error)));
        }
        Ok(GetOutcome::Found {
            value: resp.value,
            revision: resp.revision,
        })
    }

    /// `Delete` a single key.
    ///
    /// # Errors
    /// Transport errors, or `Error::UpstreamRpc` if the server returns
    /// `ok=false` for reasons other than `not_found`.
    pub async fn delete(
        &mut self,
        group_id: u64,
        key: &[u8],
        client_id: u64,
        seq: u64,
    ) -> Result<WriteOutcome> {
        let request_id = next_request_id();
        let request_create_ms = now_ms();
        let req = KvDeleteRequest {
            version: 1,
            key: key.to_vec(),
            seq,
            client_id,
            request_id,
            request_create_ms,
            group_id,
        };
        let resp = self
            .inner
            .delete(req)
            .await
            .map_err(|e| self.rpc_err(format!("delete: {e}")))?
            .into_inner();
        // not_found on delete is a benign no-op: report it as a successful
        // write with revision 0.
        if resp.not_found {
            return Ok(WriteOutcome {
                revision: 0,
                request_id: resp.request_id,
            });
        }
        check_ok(&resp).map(|()| WriteOutcome {
            revision: resp.revision,
            request_id: resp.request_id,
        })
    }

    /// Prefix-scan the upstream group's learner store. `limit == 0`
    /// means "no limit". Returns the matching `(key, value)` pairs
    /// sorted lexicographically and a `truncated` flag that's `true`
    /// when more keys exist past `limit`.
    ///
    /// # Errors
    /// Transport errors, or `Error::UpstreamRpc` if the server returns
    /// `ok=false` (typically: group not found on this replica).
    pub async fn scan(&mut self, group_id: u64, prefix: &[u8], limit: u32) -> Result<ScanOutcome> {
        let request_id = next_request_id();
        let request_create_ms = now_ms();
        let req = KvScanRequest {
            version: 1,
            prefix: prefix.to_vec(),
            limit,
            request_id,
            request_create_ms,
            group_id,
            // Console scans default to linearizable.
            read_mode: 0,
        };
        let resp = self.inner.scan(req).await.map_err(|e| Error::UpstreamRpc {
            node_id: self.endpoint.clone(),
            status: format!("scan: {e}"),
        })?;
        let resp = resp.into_inner();
        if !resp.ok {
            return Err(self.rpc_err(format!("scan: {}", resp.error)));
        }
        let items: Vec<(Vec<u8>, Vec<u8>)> = resp.items.into_iter().map(|i| (i.key, i.value)).collect();
        Ok(ScanOutcome {
            items,
            truncated: resp.truncated,
        })
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn rpc_err(&self, status: impl Into<String>) -> Error {
        Error::UpstreamRpc {
            node_id: self.endpoint.clone(),
            status: status.into(),
        }
    }
}

fn check_ok(resp: &KvResponse) -> Result<()> {
    if resp.ok {
        return Ok(());
    }
    if !resp.not_leader_hint.is_empty() {
        return Err(Error::NotLeader {
            hint: resp.not_leader_hint.clone(),
        });
    }
    Err(Error::UpstreamRpc {
        node_id: "<grpc>".into(),
        status: if resp.error.is_empty() {
            "server returned ok=false".into()
        } else {
            resp.error.clone()
        },
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Monotonic-ish request id derived from the system clock. Good enough
/// for log correlation; not a globally unique identifier.
fn next_request_id() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::KvClient;

    #[tokio::test]
    async fn connect_to_unbound_port_fails_with_upstreamrpc() {
        // Port 1 is privileged; binding/connecting will fail fast.
        let r = KvClient::connect("127.0.0.1:1").await;
        assert!(r.is_err());
    }
}
