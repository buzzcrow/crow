use crate::rpc::px_service_client::PxServiceClient;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

#[derive(Clone, Default)]
pub struct PxPeerConnectionPool {
    grpc_clients: Arc<tokio::sync::Mutex<HashMap<String, PxServiceClient<Channel>>>>,
}
impl PxPeerConnectionPool {
    pub(crate) async fn grpc_client(
        &self,
        endpoint: &str,
    ) -> Result<PxServiceClient<Channel>, tonic::Status> {
        if let Some(client) = self.grpc_clients.lock().await.get(endpoint).cloned() {
            return Ok(client);
        }

        let client = PxServiceClient::connect(format!("http://{}", endpoint))
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
        self.grpc_clients
            .lock()
            .await
            .insert(endpoint.to_string(), client.clone());
        Ok(client)
    }
}
