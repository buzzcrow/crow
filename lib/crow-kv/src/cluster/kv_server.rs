// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Server lifecycle for a `PxKvStore`.
//!
//! Defines the [`KvServer`] trait (start / join / stop / `listen_addr`)
//! and implements it on `Arc<PxKvStore>`. The trait's `start` binds a
//! TCP listener to record the listen address (used for endpoint
//! derivation by the crow-rpc servers and peer discovery), then the
//! crow-rpc servers (`start_rpc_server` for consensus,
//! `start_client_rpc_server` for client-facing) handle the actual
//! request serving. Server state (join handle, shutdown sender, bound
//! address) lives on [`GrpcTaskState`] inside the store so
//! [`PxKvStore::shutdown_server`] can drive a timed graceful stop from
//! the cascade shutdown path.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::{KvClientRpcForwarder, KvRpcService, PxRpcService, PxRpcTransport};
use crow_rpc_ffi::RpcServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
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

/// crow-rpc server state. Holds the `RpcServer` handle + the shared
/// `PxRpcTransport` for outbound RPCs to peers.
#[derive(Default)]
pub(crate) struct RpcServerState {
    pub(crate) server: Option<Arc<RpcServer>>,
    pub(crate) transport: Option<Arc<PxRpcTransport>>,
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

        // Bind a TCP listener to determine the actual port (supports
        // port 0 for OS-assigned), then immediately drop it — the
        // crow-rpc server will re-bind the same port.
        let bound_addr = match TcpListener::bind(self.listen_addr).await {
            Ok(tcp) => tcp
                .local_addr()
                .map_err(|e| format!("failed to read bound kv server address: {e}"))?,
            Err(error) => {
                let msg = format!(
                    "failed to bind kv server on {}: {error}; next step: choose an available listen_addr or stop the conflicting process",
                    self.listen_addr
                );
                error!(store_id = self.store_id, listen_addr = %self.listen_addr, error = %error, "{msg}");
                return Err(msg);
            }
        };

        // Start a single crow-rpc server on the gRPC port. Both
        // consensus (PxRpcService) and client (KvRpcService) handlers
        // are registered on the same server — their handler names
        // don't conflict.
        let server = Arc::new(RpcServer::with_engines(None, 1, 2));
        server
            .listen(&bound_addr.ip().to_string(), i32::from(bound_addr.port()))
            .map_err(|e| format!("crow-rpc listen on {bound_addr}: {e:?}"))?;
        server.start();

        let rt = tokio::runtime::Handle::current();
        let px_service = Arc::new(PxRpcService::new(self.clone(), rt.clone()));
        px_service.register_handlers(&server);

        let forwarder = Arc::new(KvClientRpcForwarder::new());
        let kv_service = Arc::new(KvRpcService::new(self.clone(), rt, forwarder));
        kv_service.register_handlers(&server);

        let transport = Arc::new(PxRpcTransport::new());

        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_clone = Arc::clone(&server);
        let handle = tokio::spawn(async move {
            let _ = rx.await;
            server_clone.stop();
        });

        {
            let mut state = self.server_state.lock();
            state.listen_addr = Some(bound_addr);
            state.handle = Some(handle);
            state.shutdown_tx = Some(tx);
        }
        {
            let mut state = self.rpc_server_state.lock();
            state.server = Some(server);
            state.transport = Some(transport.clone());
        }

        let group_count = self.groups.len();
        for entry in &self.groups {
            entry.local_replica().set_endpoint(bound_addr.to_string());
            // Wire the shared transport into remote replicas that were
            // created by apply_config during restore (before start()).
            let mut wired = 0usize;
            for remote in &entry.remote_replicas {
                if let Some(real) = remote.as_real() {
                    real.set_rpc_transport(transport.clone());
                    wired += 1;
                }
            }
            debug!(
                store_id = self.store_id,
                group_id = entry.group_id(),
                remote_count = entry.remote_replicas.len(),
                wired,
                "start: wired rpc transport into remote replicas"
            );
        }

        info!(store_id = self.store_id, listen_addr = %bound_addr, group_count, "kv server started");
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
            debug!(store_id = self.store_id, "kv server task joined");
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
    /// Stop the server task and await its join under a timeout.
    ///
    /// Used by [`PxKvStore::shutdown`] as the first cascade step. Aborts the
    /// task on hang; idempotent.
    pub(crate) async fn shutdown_server(&self, timeout: Duration) -> Result<(), String> {
        self.stop_rpc_server();
        self.stop_client_rpc_server();

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
        let abort = task.abort_handle();
        match tokio::time::timeout(timeout, task).await {
            Ok(Ok(())) => {
                debug!(store_id = self.store_id, "kv server task joined");
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
                let msg = format!("critical: kv server shutdown hung > {timeout:?}; aborted task; next step: investigate stuck handlers");
                error!(
                    store_id = self.store_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "{msg}"
                );
                Err(msg)
            }
        }
    }

    /// Start the crow-rpc consensus server.
    ///
    /// This is now a no-op — `start()` starts a single crow-rpc server
    /// that hosts both consensus and client handlers on the gRPC port.
    /// Kept for backward compatibility with callers that call it
    /// explicitly (e.g. `rpc_migration_test`).
    #[allow(clippy::missing_errors_doc)]
    pub fn start_rpc_server(self: &Arc<Self>, _rt: Handle) -> Result<(), String> {
        if self.rpc_server_state.lock().server.is_some() {
            return Ok(());
        }
        Err("start_rpc_server is a no-op; start() already starts the server".to_string())
    }

    /// Get the shared `PxRpcTransport` for outbound RPCs. Returns
    /// `None` if `start_rpc_server` has not been called.
    pub fn rpc_transport(&self) -> Option<Arc<PxRpcTransport>> {
        self.rpc_server_state.lock().transport.clone()
    }

    /// Sample transport-level stats (syscall counts, frame aggregation,
    /// submit→writev queue wait) from the crow-rpc server. Returns
    /// `None` if the server has not been started.
    pub fn rpc_transport_stats(&self) -> Option<crow_rpc_ffi::CrowRpcTransportStats> {
        self.rpc_server_state
            .lock()
            .server
            .as_ref()
            .map(|s| s.transport_stats())
    }

    /// Wire the shared `PxRpcTransport` into all existing remote
    /// replicas across all groups. Must be called after
    /// [`Self::start_rpc_server`]. Remote replicas added later (via
    /// the management API) get the transport via
    /// [`Self::rpc_transport`] at construction time.
    pub fn wire_rpc_transport(&self) {
        let transport = self.rpc_transport();
        let Some(transport) = transport else { return };
        for entry in &self.groups {
            for remote in &entry.remote_replicas {
                if let Some(real) = remote.as_real() {
                    real.set_rpc_transport(transport.clone());
                }
            }
        }
    }

    /// Stop the crow-rpc server. Called from [`Self::shutdown_server`]
    /// or directly for testing.
    pub(crate) fn stop_rpc_server(&self) {
        let server = {
            let mut state = self.rpc_server_state.lock();
            state.server.take()
        };
        if let Some(server) = server {
            server.stop();
            info!(store_id = self.store_id, "crow-rpc server stopped");
        }
    }

    /// Start the client-facing crow-rpc server.
    ///
    /// This is now a no-op — `start()` starts a single crow-rpc server
    /// that hosts both consensus and client handlers on the gRPC port.
    /// Kept for backward compatibility.
    ///
    /// # Errors
    /// Returns an error string if the client-facing crow-rpc port is
    /// out of range or the server fails to listen on the derived port.
    /// Start the client-facing crow-rpc server.
    ///
    /// This is now a no-op — `start()` starts a single crow-rpc server
    /// that hosts both consensus and client handlers on the gRPC port.
    /// Kept for backward compatibility.
    pub fn start_client_rpc_server(self: &Arc<Self>, _rt: Handle) -> Result<(), String> {
        Ok(())
    }

    /// Stop the client-facing crow-rpc server. Called from
    /// [`Self::shutdown_server`] or directly for testing.
    pub(crate) fn stop_client_rpc_server(&self) {
        let server = {
            let mut state = self.client_rpc_server_state.lock();
            state.server.take()
        };
        if let Some(server) = server {
            server.stop();
            info!(store_id = self.store_id, "client crow-rpc server stopped");
        }
    }
}
