// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

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
use crate::rpc::snapshot_service_server::SnapshotServiceServer;
use crate::rpc::{KvStoreService, PxReplicaService, PxRpcService, PxRpcTransport, PxSnapshotService};
use crow_protocol::{KV_RPC_BASE, KV_SERVER_GRPC_BASE};
use crow_rpc_ffi::RpcServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio_stream::Stream;
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

/// crow-rpc server state (R32 migration). Holds the `RpcServer`
/// handle + the shared `PxRpcTransport` for outbound RPCs to peers.
#[derive(Default)]
pub(crate) struct RpcServerState {
    /// The crow-rpc server (listens on the crow-rpc port).
    pub(crate) server: Option<Arc<RpcServer>>,
    /// Shared client-side transport for outbound RPCs. Wired into
    /// `PxRemoteReplica` via `with_rpc_transport`.
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

        let px_service = PxReplicaService::new(self.clone());
        let kv_service = KvStoreService::new(self.clone());
        let snapshot_service = PxSnapshotService::new(self.clone());
        let px_service_server = PxServiceServer::new(px_service);
        let kv_service_server = KvServiceServer::new(kv_service);
        let snapshot_service_server = SnapshotServiceServer::new(snapshot_service);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            let incoming = NoDelayIncoming::new(listener);
            let serve = Server::builder()
                .add_service(px_service_server)
                .add_service(kv_service_server)
                .add_service(snapshot_service_server)
                .serve_with_incoming(incoming);

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
    /// Stop the gRPC server task and await its join under a timeout.
    ///
    /// Used by [`PxKvStore::shutdown`] as the first cascade step. Aborts the
    /// task on hang; idempotent.
    pub(crate) async fn shutdown_server(&self, timeout: Duration) -> Result<(), String> {
        // R32: also stop the crow-rpc server.
        self.stop_rpc_server();

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

    /// Start the crow-rpc server (R32 migration). Binds to the
    /// crow-rpc port (derived from the gRPC port via a fixed offset),
    /// registers all consensus handlers, and creates a shared
    /// `PxRpcTransport` for outbound RPCs. The transport is stored in
    /// `rpc_server_state` and can be retrieved via
    /// [`Self::rpc_transport`] to wire into `PxRemoteReplica`.
    ///
    /// Must be called after [`KvServer::start`] (which binds the gRPC
    /// port and sets `listen_addr`).
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

        // Derive the crow-rpc port from the actual bound gRPC port.
        let bound_addr = self
            .server_state
            .lock()
            .listen_addr
            .ok_or_else(|| "start_rpc_server called before gRPC start()".to_string())?;
        let grpc_port = bound_addr.port();
        let rpc_port = i32::from(grpc_port) + (i32::from(KV_RPC_BASE) - i32::from(KV_SERVER_GRPC_BASE));
        if !(1..=65535).contains(&rpc_port) {
            return Err(format!(
                "crow-rpc port {rpc_port} out of range (gRPC port {grpc_port})"
            ));
        }
        let rpc_addr = format!("{}:{}", bound_addr.ip(), rpc_port);

        let server = Arc::new(RpcServer::new(None));
        server
            .listen(&bound_addr.ip().to_string(), rpc_port)
            .map_err(|e| format!("crow-rpc listen on {rpc_addr}: {e:?}"))?;
        server.start();

        // Register consensus handlers.
        let service = Arc::new(PxRpcService::new(self.clone(), rt));
        service.register_handlers(&server);

        // Create the shared client transport.
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

    /// Get the shared `PxRpcTransport` for outbound RPCs (R32
    /// migration). Returns `None` if `start_rpc_server` has not been
    /// called.
    #[allow(dead_code)] // Wired in Phase 8 (integration tests)
    pub fn rpc_transport(&self) -> Option<Arc<PxRpcTransport>> {
        self.rpc_server_state.lock().transport.clone()
    }

    /// Stop the crow-rpc server (R32 migration). Called from
    /// [`Self::shutdown_server`] or directly for testing.
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
}

pin_project_lite::pin_project! {
    struct NoDelayIncoming {
        #[pin]
        inner: tokio_stream::wrappers::TcpListenerStream,
    }
}

impl NoDelayIncoming {
    fn new(listener: TcpListener) -> Self {
        Self {
            inner: tokio_stream::wrappers::TcpListenerStream::new(listener),
        }
    }
}

impl Stream for NoDelayIncoming {
    type Item = std::io::Result<tokio::net::TcpStream>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx).map_ok(|stream| {
            let _ = stream.set_nodelay(true);
            stream
        })
    }
}
