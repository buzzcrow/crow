//! gRPC client for `crowkv-server`'s `KvService` (C6).
//!
//! Wraps the tonic-generated `KvServiceClient` so callers (CLI / web)
//! get a small, console-flavoured surface: `put`, `get`, `delete` —
//! plus `list` / `scan` placeholders that surface a clear "not yet
//! supported on the server" error until the server implements prefix
//! reads.
//!
//! Endpoints come from `StoreSummary::listen_addr` (returned by the
//! HTTP management API). The console connects per call; for repeated
//! ops on a single store, hold onto the [`KvClient`] returned from
//! [`KvClient::connect`].

use std::time::{SystemTime, UNIX_EPOCH};

use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvDeleteRequest, KvGetRequest, KvResponse, KvSetRequest};
use tonic::transport::Channel;

use crate::error::{Error, Result};

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

/// Connection to a single `crowkv-server`'s gRPC `KvService`.
#[derive(Debug, Clone)]
pub struct KvClient {
    inner: KvServiceClient<Channel>,
    endpoint: String,
}

impl KvClient {
    /// Connect by `host:port` (no scheme — gRPC is plain HTTP/2 in C6).
    ///
    /// # Errors
    /// `Error::ServerRpc` for transport failures.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.clone()
        } else {
            format!("http://{endpoint}")
        };
        let channel = Channel::from_shared(url.clone())
            .map_err(|e| Error::ServerRpc {
                server_id: endpoint.clone(),
                status: format!("invalid endpoint: {e}"),
            })?
            .connect()
            .await
            .map_err(|e| Error::ServerRpc {
                server_id: endpoint.clone(),
                status: format!("grpc connect: {e}"),
            })?;
        Ok(Self {
            inner: KvServiceClient::new(channel),
            endpoint,
        })
    }

    /// `Put` a single key/value. `client_id` and `seq` are passed through
    /// to the server for idempotency tracking; pass `0` if you don't
    /// care.
    ///
    /// # Errors
    /// Transport errors, or `Error::ServerRpc` if the server returns
    /// `ok=false`.
    pub async fn put(&mut self, group_id: u64, key: &[u8], value: &[u8], client_id: u64, seq: u64) -> Result<WriteOutcome> {
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
        let resp = self.inner.put(req).await.map_err(|e| self.rpc_err(format!("put: {e}")))?.into_inner();
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
        };
        let resp = self.inner.get(req).await.map_err(|e| self.rpc_err(format!("get: {e}")))?.into_inner();
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
    /// Transport errors, or `Error::ServerRpc` if the server returns
    /// `ok=false` for reasons other than `not_found`.
    pub async fn delete(&mut self, group_id: u64, key: &[u8], client_id: u64, seq: u64) -> Result<WriteOutcome> {
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
        let resp = self.inner.delete(req).await.map_err(|e| self.rpc_err(format!("delete: {e}")))?.into_inner();
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

    /// Best-effort placeholder for `list` / `scan`. The server has no
    /// prefix scan RPC yet (see `crowkv/src/rpc/proto/kv.proto`); this
    /// returns a clear `Error::ServerRpc` so the CLI/web layer can
    /// print a helpful message.
    ///
    /// # Errors
    /// Always returns a "not implemented" `Error::ServerRpc` until the
    /// server adds `Scan(KvScanRequest)`.
    #[allow(clippy::unused_async)]
    pub async fn scan(&mut self, _group_id: u64, _prefix: &[u8], _limit: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(self.rpc_err("scan: server does not yet implement prefix scan (see C6 / kv.proto)".to_string()))
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn rpc_err(&self, status: impl Into<String>) -> Error {
        Error::ServerRpc {
            server_id: self.endpoint.clone(),
            status: status.into(),
        }
    }
}

fn check_ok(resp: &KvResponse) -> Result<()> {
    if resp.ok {
        return Ok(());
    }
    if !resp.not_leader_hint.is_empty() {
        return Err(Error::ServerRpc {
            server_id: "<grpc>".into(),
            status: format!("not leader (hint: {})", resp.not_leader_hint),
        });
    }
    Err(Error::ServerRpc {
        server_id: "<grpc>".into(),
        status: if resp.error.is_empty() { "server returned ok=false".into() } else { resp.error.clone() },
    })
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Monotonic-ish request id derived from the system clock. Good enough
/// for log correlation; not a globally unique identifier.
fn next_request_id() -> u64 {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::KvClient;

    #[tokio::test]
    async fn connect_to_unbound_port_fails_with_serverrpc() {
        // Port 1 is privileged; binding/connecting will fail fast.
        let r = KvClient::connect("127.0.0.1:1").await;
        assert!(r.is_err());
    }
}
