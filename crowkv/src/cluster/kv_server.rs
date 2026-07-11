use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::kv_service_server::KvServiceServer;
use crate::rpc::px_service_server::PxServiceServer;
use crate::rpc::{KvStoreService, PxReplicaService};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{debug, error, info};

#[allow(async_fn_in_trait)]
pub trait KvServer {
    async fn start(&self) -> bool;

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
    async fn start(&self) -> bool {
        {
            let state = self.server_state.lock().unwrap();
            if state.handle.is_some() {
                debug!("kv server start skipped because server is already running");
                return false;
            }
        }

        let listener = match TcpListener::bind(self.listen_addr).await {
            Ok(tcp) => tcp,
            Err(error) => {
                error!(
                    listen_addr = %self.listen_addr,
                    error = %error,
                    "failed to bind kv server; next step: choose an available listen_addr or stop the conflicting process"
                );
                return false;
            }
        };
        let bound_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                error!(
                    error = %error,
                    "failed to read bound kv server address; next step: restart kv server and inspect socket state"
                );
                return false;
            }
        };

        if self.groups.is_empty() {
            error!("no groups configured; cannot start server");
            return false;
        }

        let px_service = PxReplicaService::new(self.clone());
        let kv_service = KvStoreService::new(self.clone());
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

        info!(listen_addr = %bound_addr, "kv server started");
        true
    }

    async fn join(&self) {
        let handle = {
            let mut state = self.server_state.lock().unwrap();
            state.handle.take()
        };
        if let Some(task) = handle {
            debug!("joining kv server task");
            let _ = task.await;
            info!("kv server task joined");
        }
    }

    fn stop(&self) {
        let sender = {
            let mut state = self.server_state.lock().unwrap();
            state.shutdown_tx.take()
        };
        if let Some(tx) = sender {
            let _ = tx.send(());
            info!("kv server shutdown requested");
        }
    }

    fn listen_addr(&self) -> Option<SocketAddr> {
        self.server_state.lock().unwrap().listen_addr
    }
}
