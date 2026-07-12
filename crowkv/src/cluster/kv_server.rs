//! gRPC server lifecycle for a `PxKvStore`.
//!
//! Defines the [`KvServer`] trait (start / join / stop / `listen_addr`)
//! and implements it on `Arc<PxKvStore>`, which multiplexes the
//! `PxService` (Paxos peer RPCs) and `KvService` (client KV RPCs)
//! tonic servers onto a single `tokio::net::TcpListener`. Server state
//! (join handle, shutdown sender, bound address) lives on
//! [`GrpcTaskState`] inside the store so [`PxKvStore::shutdown_server`]
//! can drive a timed graceful stop from the cascade shutdown path.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::kv_service_server::KvServiceServer;
use crate::rpc::px_service_server::PxServiceServer;
use crate::rpc::{KvStoreService, PxReplicaService};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{debug, error, info};

#[allow(async_fn_in_trait)]
pub trait KvServer {
    async fn start(&self) -> Result<(), String>;

    async fn join(&self);

    fn stop(&self);

    fn listen_addr(&self) -> Option<SocketAddr>;
}

#[derive(Default)]
pub(crate) struct GrpcTaskState {
    pub(crate) handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub(crate) listen_addr: Option<SocketAddr>,
}

impl KvServer for Arc<PxKvStore> {
    async fn start(&self) -> Result<(), String> {
        {
            let state = self.server_state.lock();
            if state.handle.is_some() {
                debug!(
                    store_id = self.store_id,
                    "kv server start skipped because server is already running"
                );
                return Ok(());
            }
        }

        let listener = match TcpListener::bind(self.listen_addr).await {
            Ok(tcp) => tcp,
            Err(error) => {
                let msg = format!(
                    "failed to bind kv server on {}: {error}; next step: choose an available listen_addr or stop the conflicting process",
                    self.listen_addr
                );
                error!(store_id = self.store_id, listen_addr = %self.listen_addr, error = %error, "{msg}");
                return Err(msg);
            }
        };
        let bound_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                let msg = format!("failed to read bound kv server address: {error}; next step: restart kv server and inspect socket state");
                error!(store_id = self.store_id, error = %error, "{msg}");
                return Err(msg);
            }
        };

        let px_service = PxReplicaService::new(self.clone());
        let kv_service = KvStoreService::new(self.clone());
        let px_service_server = PxServiceServer::new(px_service);
        let kv_service_server = KvServiceServer::new(kv_service);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            let serve = Server::builder()
                .add_service(px_service_server)
                .add_service(kv_service_server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

            tokio::select! {
                _ = serve => {},
                _ = rx => {},
            }
        });

        {
            let mut state = self.server_state.lock();
            state.listen_addr = Some(bound_addr);
            state.handle = Some(handle);
            state.shutdown_tx = Some(tx);
        }

        // Update local replica endpoints on all groups with the actual
        // bound address, so future persist_config calls write the correct
        // endpoint. Groups added after start() get the endpoint in
        // add_group_inner.
        for entry in &self.groups {
            entry.local_replica().set_endpoint(bound_addr.to_string());
        }

        info!(store_id = self.store_id, listen_addr = %bound_addr, "kv server started");
        Ok(())
    }

    async fn join(&self) {
        let handle = {
            let mut state = self.server_state.lock();
            state.handle.take()
        };
        if let Some(task) = handle {
            debug!(store_id = self.store_id, "joining kv server task");
            let _ = task.await;
            info!(store_id = self.store_id, "kv server task joined");
        }
    }

    fn stop(&self) {
        let sender = {
            let mut state = self.server_state.lock();
            state.shutdown_tx.take()
        };
        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(store_id = self.store_id, "kv server shutdown requested");
        }
    }

    fn listen_addr(&self) -> Option<SocketAddr> {
        self.server_state.lock().listen_addr
    }
}

impl PxKvStore {
    /// Stop the gRPC server task and await its join under a timeout.
    ///
    /// Used by [`PxKvStore::shutdown`] as the first cascade step. Aborts the
    /// task on hang; idempotent.
    pub(crate) async fn shutdown_server(&self, timeout: Duration) -> Result<(), String> {
        let (handle, sender) = {
            let mut state = self.server_state.lock();
            (state.handle.take(), state.shutdown_tx.take())
        };
        if handle.is_none() && sender.is_none() {
            debug!(
                store_id = self.store_id,
                "kv server shutdown is a no-op (already stopped)"
            );
            return Ok(());
        }

        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(store_id = self.store_id, "kv server shutdown requested");
        }

        let Some(task) = handle else { return Ok(()) };
        // abort_handle() lets us force-cancel on timeout; dropping a JoinHandle
        // only detaches the task.
        let abort = task.abort_handle();
        match tokio::time::timeout(timeout, task).await {
            Ok(Ok(())) => {
                info!(store_id = self.store_id, "kv server task joined");
                Ok(())
            }
            Ok(Err(join_err)) if join_err.is_cancelled() => {
                debug!(store_id = self.store_id, "kv server task was already cancelled");
                Ok(())
            }
            Ok(Err(join_err)) => {
                let msg = format!("critical: kv server task ended abnormally: {join_err}; next step: inspect server logs for panic");
                error!(store_id = self.store_id, error = %join_err, "{msg}");
                Err(msg)
            }
            Err(_elapsed) => {
                abort.abort();
                let msg = format!("critical: kv server shutdown hung > {timeout:?}; aborted task; next step: investigate stuck gRPC handlers");
                error!(
                    store_id = self.store_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "{msg}"
                );
                Err(msg)
            }
        }
    }
}
