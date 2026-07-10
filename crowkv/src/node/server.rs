use crate::node::PxNode;
use crate::rpc::kv_service_server::KvServiceServer;
use crate::rpc::px_service_server::PxServiceServer;
use crate::rpc::{KvNodeService, PxNodeService};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{debug, error, info};

/// Server-lifecycle trait for a CrowKV consensus node.
///
/// Implementors can start a gRPC service, wait for it to stop, and
/// trigger graceful shutdown.
#[allow(async_fn_in_trait)]
pub trait NodeServer {
    /// Start the gRPC service. Returns `true` if the server was
    /// successfully bound and spawned.
    async fn start(&self) -> bool;

    /// Block until the service task has completed.
    async fn join(&self);

    /// Signal graceful shutdown.
    fn stop(&self);
}

/// Server-side state kept behind a short-lived `std::sync::Mutex` so that
/// `start`/`stop`/`join` can all take `&self`.
pub struct GrpcTaskState {
    pub(crate) handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub(crate) listen_addr: Option<SocketAddr>,
}

impl NodeServer for PxNode {
    async fn start(&self) -> bool {
        {
            let state = self.server_state.lock().unwrap();
            if state.handle.is_some() {
                debug!(
                    node_id = self.id,
                    "node server start skipped because server is already running"
                );
                return false;
            }
        }

        let listener = match TcpListener::bind(self.config.listen_addr).await {
            Ok(tcp) => tcp,
            Err(error) => {
                error!(
                    node_id = self.id,
                    listen_addr = %self.config.listen_addr,
                    error = %error,
                    "failed to bind node server; next step: choose an available listen_addr or stop the conflicting process"
                );
                return false;
            }
        };
        let bound_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                error!(
                    node_id = self.id,
                    error = %error,
                    "failed to read bound node server address; next step: restart node server and inspect socket state"
                );
                return false;
            }
        };

        let px_service = PxNodeService::new(self.clone());
        let kv_service = KvNodeService::new(self.clone());
        let px_server = PxServiceServer::new(px_service);
        let kv_server = KvServiceServer::new(kv_service);

        let (tx, rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            let serve = Server::builder()
                .add_service(px_server)
                .add_service(kv_server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

            tokio::select! {
                _ = serve => {},
                _ = rx => {},
            }
        });

        {
            let mut state = self.server_state.lock().unwrap();
            state.listen_addr = Some(bound_addr);
            state.handle = Some(handle);
            state.shutdown_tx = Some(tx);
        }

        info!(node_id = self.id, listen_addr = %bound_addr, "node server started");
        true
    }

    async fn join(&self) {
        let handle = {
            let mut state = self.server_state.lock().unwrap();
            state.handle.take()
        };
        if let Some(task) = handle {
            debug!(node_id = self.id, "joining node server task");
            let _ = task.await;
            info!(node_id = self.id, "node server task joined");
        }
    }

    fn stop(&self) {
        let sender = {
            let mut state = self.server_state.lock().unwrap();
            state.shutdown_tx.take()
        };
        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(node_id = self.id, "node server shutdown requested");
        }
    }
}
