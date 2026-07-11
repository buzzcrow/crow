//! Cluster topology aggregator.
//!
//! Polls `/health` + `/topology` on each input server in parallel and
//! returns a `ClusterSnapshot`. Per-server failures do not abort the
//! aggregate; they surface as a populated `error` field on that server's
//! entry.

use futures::future::join_all;

use crate::clients::http::ServerClient;
use crate::error::Result;
use crate::snapshot::{ClusterSnapshot, ServerSnapshot};

/// Aggregate the cluster snapshot from a list of management URLs.
///
/// # Errors
/// Currently never returns an outer error; per-server errors are recorded
/// in `ServerSnapshot::error`. The `Result` is kept so future variants
/// (e.g. configuration loading) can surface fatal errors.
pub async fn aggregate(server_urls: &[String]) -> Result<ClusterSnapshot> {
    let futs = server_urls.iter().map(|url| poll_server(url.clone()));
    let servers = join_all(futs).await;
    Ok(ClusterSnapshot { servers })
}

async fn poll_server(mgmt_url: String) -> ServerSnapshot {
    let client = match ServerClient::new(mgmt_url.clone()) {
        Ok(c) => c,
        Err(e) => {
            return ServerSnapshot {
                mgmt_url,
                health: None,
                stores: Vec::new(),
                error: Some(format!("{e}")),
            };
        }
    };

    let (health_res, topo_res) = futures::join!(client.health(), client.topology());

    let mut snapshot = ServerSnapshot {
        mgmt_url,
        health: None,
        stores: Vec::new(),
        error: None,
    };
    let mut errs: Vec<String> = Vec::new();

    match health_res {
        Ok(h) => snapshot.health = Some(h),
        Err(e) => errs.push(format!("health: {e}")),
    }
    match topo_res {
        Ok(stores) => snapshot.stores = stores,
        Err(e) => errs.push(format!("topology: {e}")),
    }

    if !errs.is_empty() {
        snapshot.error = Some(errs.join("; "));
    }
    snapshot
}
