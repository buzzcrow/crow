//! `Node` — runtime container for a single CrowKV group member.
//!
//! A `Node` wraps the consensus state (Acceptor), a hard-coded role
//! (leader or follower), and the minimal gRPC service required for
//! P1 M2 classic-Paxos over loopback TCP.
//!
//! Introduced in P1 M2. See `doc/plan/plan-consensus.md` §1 M2.3.

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tonic::transport::Server;

use crate::paxos::acceptor::PxAcceptor;
use crate::rpc::peer_service_server::PeerServiceServer;
use crate::rpc::service::AcceptorService;

/// Hard-coded role for a node in P1 M2 (no election yet).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeRole {
    /// Drives Prepare / Accept rounds, tracks quorum.
    Leader,
    /// Only serves Acceptor handlers over RPC.
    Follower,
}

/// Runtime container for one group member.
///
/// In P1 M2 the node is constructed directly by tests (via `TestNodeHarness`)
/// rather than by the `crowkv-server` binary.
pub struct Node {
    pub id: u64,
    pub role: NodeRole,
    pub acceptor: PxAcceptor,
}

impl Node {
    /// Create a new node with the given id and hard-coded role.
    pub fn new(id: u64, role: NodeRole) -> Self {
        Self {
            id,
            role,
            acceptor: PxAcceptor::new(),
        }
    }

    /// Convenience: is this node the leader?
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// Start the gRPC server on `127.0.0.1:0`, returning the bound address
    /// and a shutdown trigger.
    ///
    /// Every node (leader or follower) runs the full `PeerService` because
    /// even a leader must answer `Prepare` / `Accept` from competing proposers.
    pub async fn serve(
        self,
    ) -> Result<(SocketAddr, tokio::sync::oneshot::Sender<()>), std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let bound_addr = listener.local_addr()?;

        let service = AcceptorService::new(self.acceptor);
        let server = PeerServiceServer::new(service);

        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let serve = Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

            tokio::select! {
                _ = serve => {},
                _ = rx => {},
            }
        });

        Ok((bound_addr, tx))
    }
}
