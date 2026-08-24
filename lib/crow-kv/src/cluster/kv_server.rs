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
use crow_protocol::{KV_CLIENT_RPC_BASE, KV_RPC_BASE, KV_SERVER_GRPC_BASE};
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

        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            drop(listener);
            let _ = rx.await;
        });

        {
            let mut state = self.server_state.lock();
            state.listen_addr = Some(bound_addr);
            state.handle = Some(handle);
            state.shutdown_tx = Some(tx);
        }

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

    /// Start the crow-rpc server. Binds to the crow-rpc port (derived
    /// from the bound port via a fixed offset), registers all consensus
    /// handlers, and creates a shared `PxRpcTransport` for outbound
    /// RPCs. The transport is stored in `rpc_server_state` and can be
    /// retrieved via [`Self::rpc_transport`] to wire into
    /// `PxRemoteReplica`.
    ///
    /// Must be called after [`KvServer::start`] (which binds the port
    /// and sets `listen_addr`).
    ///
    /// # Errors
    /// Returns an error string if the crow-rpc port is out of range or
    /// the server fails to listen on the derived port.
    pub fn start_rpc_server(self: &Arc<Self>, rt: Handle) -> Result<(), String> {
        {
            let state = self.rpc_server_state.lock();
            if state.server.is_some() {
                debug!(
                    store_id = self.store_id,
                    "rpc server start skipped because server is already running"
                );
                return Ok(());
            }
        }

        let bound_addr = self
            .server_state
            .lock()
            .listen_addr
            .ok_or_else(|| "start_rpc_server called before start()".to_string())?;
        let grpc_port = bound_addr.port();
        let rpc_port = i32::from(grpc_port) + (i32::from(KV_RPC_BASE) - i32::from(KV_SERVER_GRPC_BASE));
        if !(1..=65535).contains(&rpc_port) {
            return Err(format!(
                "crow-rpc port {rpc_port} out of range (port {grpc_port})"
            ));
        }
        let rpc_addr = format!("{}:{}", bound_addr.ip(), rpc_port);

        let server = Arc::new(RpcServer::new(None));
        server
            .listen(&bound_addr.ip().to_string(), rpc_port)
            .map_err(|e| format!("crow-rpc listen on {rpc_addr}: {e:?}"))?;
        server.start();

        let service = Arc::new(PxRpcService::new(self.clone(), rt));
        service.register_handlers(&server);

        let transport = Arc::new(PxRpcTransport::new());

        {
            let mut state = self.rpc_server_state.lock();
            state.server = Some(server);
            state.transport = Some(transport);
        }

        info!(
            store_id = self.store_id,
            rpc_addr = %rpc_addr,
            "crow-rpc server started"
        );
        Ok(())
    }

    /// Get the shared `PxRpcTransport` for outbound RPCs. Returns
    /// `None` if `start_rpc_server` has not been called.
    pub fn rpc_transport(&self) -> Option<Arc<PxRpcTransport>> {
        self.rpc_server_state.lock().transport.clone()
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

    /// Start the client-facing crow-rpc server. Binds to the
    /// client-facing crow-rpc port (derived from the port via
    /// `KV_CLIENT_RPC_BASE - KV_SERVER_GRPC_BASE`), registers all
    /// client-facing handlers, and stores the server handle in
    /// `client_rpc_server_state`.
    ///
    /// Must be called after [`Self::start_rpc_server`].
    ///
    /// # Errors
    /// Returns an error string if the client-facing crow-rpc port is
    /// out of range or the server fails to listen on the derived port.
    pub fn start_client_rpc_server(self: &Arc<Self>, rt: Handle) -> Result<(), String> {
        {
            let state = self.client_rpc_server_state.lock();
            if state.server.is_some() {
                debug!(
                    store_id = self.store_id,
                    "client rpc server start skipped because server is already running"
                );
                return Ok(());
            }
        }

        let bound_addr = self
            .server_state
            .lock()
            .listen_addr
            .ok_or_else(|| "start_client_rpc_server called before start()".to_string())?;
        let grpc_port = bound_addr.port();
        let rpc_port =
            i32::from(grpc_port) + (i32::from(KV_CLIENT_RPC_BASE) - i32::from(KV_SERVER_GRPC_BASE));
        if !(1..=65535).contains(&rpc_port) {
            return Err(format!(
                "client crow-rpc port {rpc_port} out of range (port {grpc_port})"
            ));
        }
        let rpc_addr = format!("{}:{}", bound_addr.ip(), rpc_port);

        let server = Arc::new(RpcServer::new(None));
        server
            .listen(&bound_addr.ip().to_string(), rpc_port)
            .map_err(|e| format!("client crow-rpc listen on {rpc_addr}: {e:?}"))?;
        server.start();

        let forwarder = Arc::new(KvClientRpcForwarder::new());
        let service = Arc::new(KvRpcService::new(self.clone(), rt, forwarder));
        service.register_handlers(&server);

        {
            let mut state = self.client_rpc_server_state.lock();
            state.server = Some(server);
        }

        info!(
            store_id = self.store_id,
            rpc_addr = %rpc_addr,
            "client crow-rpc server started"
        );
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
