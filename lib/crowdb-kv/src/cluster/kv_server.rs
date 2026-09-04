// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server lifecycle for a `PxKvStore`.
//!
//! Defines the [`KvServer`] trait (start / join / stop / `listen_addr`)
//! and implements it on `Arc<PxKvStore>`. The trait's `start` binds a
//! single crowdb-rpc server on the listen port that hosts both
//! consensus (`PxRpcService`) and client-facing (`KvRpcService`)
//! handlers. Server state (join handle, shutdown sender, bound
//! address) lives on [`RpcTaskState`] inside the store so
//! [`PxKvStore::shutdown_server`] can drive a timed graceful stop from
//! the cascade shutdown path.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::{KvClientRpcForwarder, KvRpcService, PxRpcService, PxRpcTransport};
use crowdb_rpc_ffi::RpcServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, info_span, Instrument};

#[allow(async_fn_in_trait)]
pub trait KvServer {
    async fn start(&self) -> Result<(), String>;

    async fn join(&self);

    fn stop(&self);

    fn listen_addr(&self) -> Option<SocketAddr>;
}

#[derive(Default)]
pub(crate) struct RpcTaskState {
    pub(crate) handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub(crate) listen_addr: Option<SocketAddr>,
}

/// crowdb-rpc server state. Holds the `RpcServer` handle + the shared
/// `PxRpcTransport` for outbound RPCs to peers.
#[derive(Default)]
pub(crate) struct RpcServerState {
    pub(crate) server: Option<Arc<RpcServer>>,
    pub(crate) transport: Option<Arc<PxRpcTransport>>,
}
#[allow(async_fn_in_trait)]
impl KvServer for Arc<PxKvStore> {
    #[allow(clippy::unused_async_trait_impl)]
    async fn start(&self) -> Result<(), String> {
        {
            let state = self.server_state.lock();
            if state.handle.is_some() {
                debug!(
                    s = self.store_id,
                    "kv server start skipped because server is already running"
                );
                return Ok(());
            }
        }

        let bound_addr = self.listen_addr;

        // Start a single crowdb-rpc server on the listen port. Both
        // consensus (PxRpcService) and client (KvRpcService) handlers
        // are registered on the same server — their handler names
        // don't conflict. Worker count set from `--rpc-workers` CLI
        // (via `PxKvStore::rpc_workers`).
        let workers = self.rpc_workers;
        let server = Arc::new(RpcServer::with_engines(None, 1, workers));
        server.set_tcp_nodelay(!self.enable_nagle);
        server.set_quickack(self.quickack);
        server.set_event_write(self.event_write);
        server.set_send_queue_capacity(self.send_queue_capacity);
        server
            .listen(&bound_addr.ip().to_string(), i32::from(bound_addr.port()))
            .map_err(|e| {
                let msg = format!(
                    "failed to bind kv server on {bound_addr}: {e:?}; next step: choose an available listen_addr or stop the conflicting process"
                );
                error!(s = self.store_id, listen_addr = %bound_addr, error = ?e, "{msg}");
                msg
            })?;
        server.start();
        server.register_conn_count_gauge("rpc.server.connections");

        let rt = tokio::runtime::Handle::current();
        let px_service = Arc::new(PxRpcService::new(self.clone(), rt.clone()));
        px_service.register_handlers(&server);

        let forwarder = Arc::new(KvClientRpcForwarder::with_workers(self.rpc_workers));
        let kv_service = Arc::new(KvRpcService::new(self.clone(), rt, forwarder));
        kv_service.register_handlers(&server);

        let transport = Arc::new(PxRpcTransport::with_pool_size(
            self.peer_pool_size,
            self.enable_nagle,
            self.quickack,
            self.event_write,
            self.send_queue_capacity,
            self.rpc_workers,
        ));

        let (tx, rx) = tokio::sync::oneshot::channel();
        let server_clone = Arc::clone(&server);
        let handle = tokio::spawn(
            async move {
                let _ = rx.await;
                server_clone.stop();
            }
            .instrument(info_span!("rpc_server", s = self.store_id)),
        );

        {
            let mut state = self.server_state.lock();
            // If the listen addr used port 0 (OS-assigned), read the
            // actual bound port from the RPC server.
            let actual_addr = if bound_addr.port() == 0 {
                let actual_port = u16::try_from(server.port()).unwrap_or(0);
                SocketAddr::new(bound_addr.ip(), actual_port)
            } else {
                bound_addr
            };
            state.listen_addr = Some(actual_addr);
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
                s = self.store_id,
                g = entry.group_id(),
                remote_count = entry.remote_replicas.len(),
                wired,
                "start: wired rpc transport into remote replicas"
            );
        }

        info!(s = self.store_id, listen_addr = %bound_addr, group_count, "kv server started");
        Ok(())
    }

    async fn join(&self) {
        let handle = {
            let mut state = self.server_state.lock();
            state.handle.take()
        };
        if let Some(task) = handle {
            debug!(s = self.store_id, "joining kv server task");
            let _ = task.await;
            debug!(s = self.store_id, "kv server task joined");
        }
    }

    fn stop(&self) {
        let sender = {
            let mut state = self.server_state.lock();
            state.shutdown_tx.take()
        };
        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(s = self.store_id, "kv server shutdown requested");
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
                s = self.store_id,
                "kv server shutdown is a no-op (already stopped)"
            );
            return Ok(());
        }

        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(s = self.store_id, "kv server shutdown requested");
        }
        let Some(task) = handle else { return Ok(()) };
        let abort = task.abort_handle();
        match tokio::time::timeout(timeout, task).await {
            Ok(Ok(())) => {
                debug!(s = self.store_id, "kv server task joined");
                Ok(())
            }
            Ok(Err(join_err)) if join_err.is_cancelled() => {
                debug!(s = self.store_id, "kv server task was already cancelled");
                Ok(())
            }
            Ok(Err(join_err)) => {
                let msg = format!("critical: kv server task ended abnormally: {join_err}; next step: inspect server logs for panic");
                error!(s = self.store_id, error = %join_err, "{msg}");
                Err(msg)
            }
            Err(_elapsed) => {
                abort.abort();
                let msg = format!("critical: kv server shutdown hung > {timeout:?}; aborted task; next step: investigate stuck handlers");
                error!(
                    s = self.store_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "{msg}"
                );
                Err(msg)
            }
        }
    }

    /// Get the shared `PxRpcTransport` for outbound RPCs. Returns
    /// `None` if `start()` has not been called.
    pub fn rpc_transport(&self) -> Option<Arc<PxRpcTransport>> {
        self.rpc_server_state.lock().transport.clone()
    }

    /// Sample transport-level stats (syscall counts, frame aggregation,
    /// submit→writev queue wait) from the crowdb-rpc server. Returns
    /// `None` if the server has not been started.
    pub fn rpc_transport_stats(&self) -> Option<crowdb_rpc_ffi::CrowdbRpcTransportStats> {
        self.rpc_server_state
            .lock()
            .server
            .as_ref()
            .map(|s| s.transport_stats())
    }

    /// Wire the shared `PxRpcTransport` into all existing remote
    /// replicas across all groups. Must be called after
    /// [`KvServer::start`]. Remote replicas added later (via
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

    /// Stop the crowdb-rpc server. Called from [`Self::shutdown_server`]
    /// or directly for testing.
    pub(crate) fn stop_rpc_server(&self) {
        let server = {
            let mut state = self.rpc_server_state.lock();
            state.server.take()
        };
        if let Some(server) = server {
            server.stop();
            info!(s = self.store_id, "crowdb-rpc server stopped");
        }
    }

    /// Stop the client-facing crowdb-rpc server. Called from
    /// [`Self::shutdown_server`] or directly for testing.
    pub(crate) fn stop_client_rpc_server(&self) {
        let server = {
            let mut state = self.client_rpc_server_state.lock();
            state.server.take()
        };
        if let Some(server) = server {
            server.stop();
            info!(s = self.store_id, "client crowdb-rpc server stopped");
        }
    }
}
