//! `crowkv-server` — `CrowKV` daemon entry point.
//!
//! Wraps the `crowkv` library into a runnable server process with:
//! - CLI-driven startup of `PxKvStore` instances with `PxGroup`s.
//! - HTTP management API for runtime topology control.
//! - Graceful shutdown on SIGINT/SIGTERM.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing::info;

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::{PxElectionConfig, ServerConfig};

use crowkv_server::cli::{parse_id_list, parse_port_list, Cli};
use crowkv_server::mgmt_api;
use crowkv_server::store_registry::KvStoreRegistry;

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let _guards = if args.log {
        crowkv::common::logging::init_file_and_console_logging("log", "crowkv-server")
            .expect("failed to initialize crowkv-server logging")
    } else {
        crowkv::common::logging::init_file_logging("log", "crowkv-server")
            .expect("failed to initialize crowkv-server logging")
    };

    info!("crowkv-server starting...");

    info!(
        stores = ?args.stores.as_deref(),
        groups = ?args.groups.as_deref(),
        replica = args.replica,
        ports = ?args.ports.as_deref(),
        management_addr = %args.management_addr,
        management_port = args.management_port,
        election_profile = %args.election_profile,
        "parsed CLI arguments"
    );

    let bootstrap = parse_and_validate_cli_args(&args);

    let registry = Arc::new(KvStoreRegistry::new());

    // Start HTTP management server first
    let mgmt_addr: SocketAddr = format!("{}:{}", args.management_addr, args.management_port)
        .parse()
        .unwrap_or_else(|e| {
            eprintln!("error: invalid management address: {e}");
            std::process::exit(1);
        });

    let router = mgmt_api::router(registry.clone());
    let listener = tokio::net::TcpListener::bind(mgmt_addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to bind management HTTP on {mgmt_addr}: {e}");
            std::process::exit(1);
        });

    let bound_mgmt_addr: SocketAddr = listener.local_addr().unwrap_or(mgmt_addr);
    // Use 127.0.0.1 in the URL if binding to 0.0.0.0 for better local testing UX
    let display_addr = if bound_mgmt_addr.ip().is_unspecified() {
        format!("127.0.0.1:{}", bound_mgmt_addr.port())
    } else {
        bound_mgmt_addr.to_string()
    };

    info!(
        management_addr = %display_addr,
        topology_url = format!("http://{}/top", display_addr),
        "HTTP management API starting"
    );
    // Print to stdout for test harness to read
    println!("Management API listening on: management_addr={display_addr}");
    std::io::stdout().flush().expect("failed to flush stdout");

    // Create and start stores after management API is ready.
    if let Some(b) = bootstrap.as_ref() {
        create_and_start_stores(
            &b.store_ids,
            &b.group_ids,
            b.replica_id,
            b.ports.clone(),
            &args.election_profile,
            registry.clone(),
        )
        .await;
    }

    // Serve until shutdown signal
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|e| {
            tracing::error!("management HTTP server error: {e}");
        });

    // Graceful cascade shutdown (PxKvStore → PxGroup → replicas)
    graceful_shutdown(registry).await;
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("received shutdown signal (CTRL+C), initiating graceful shutdown");
}

/// Parsed bootstrap parameters. `None` means "no `--stores` was passed,
/// so the server boots empty".
struct Bootstrap {
    store_ids: Vec<u64>,
    group_ids: Vec<u64>,
    replica_id: u64,
    ports: Vec<u16>,
}

/// Parse and validate CLI arguments. Returns `None` when `--stores`
/// was not provided (empty-boot mode). Groups are optional; if not
/// provided, stores are created without groups.
fn parse_and_validate_cli_args(args: &Cli) -> Option<Bootstrap> {
    let stores_str = args.stores.as_ref()?;
    let store_ids = parse_id_list(stores_str).unwrap_or_else(|e| {
        eprintln!("error: invalid --stores: {e}");
        std::process::exit(1);
    });
    let group_ids = if let Some(ref groups_str) = args.groups {
        parse_id_list(groups_str).unwrap_or_else(|e| {
            eprintln!("error: invalid --groups: {e}");
            std::process::exit(1);
        })
    } else {
        Vec::new()
    };
    let replica_id = args.replica;
    if !(1..=128).contains(&replica_id) {
        eprintln!("error: --replica must be in range [1, 128], got {replica_id}");
        std::process::exit(1);
    }

    let ports: Vec<u16> = if let Some(ref port_str) = args.ports {
        let p = parse_port_list(port_str).unwrap_or_else(|e| {
            eprintln!("error: invalid --ports: {e}");
            std::process::exit(1);
        });
        if p.len() < store_ids.len() {
            eprintln!(
                "error: --ports has {} ports but --stores needs {}",
                p.len(),
                store_ids.len()
            );
            std::process::exit(1);
        }
        p
    } else {
        vec![0u16; store_ids.len()]
    };

    Some(Bootstrap {
        store_ids,
        group_ids,
        replica_id,
        ports,
    })
}

/// Create and start stores with their groups, registering each with the registry.
/// If `group_ids` is empty, stores are created without groups.
async fn create_and_start_stores(
    store_ids: &[u64],
    group_ids: &[u64],
    replica_id: u64,
    ports: Vec<u16>,
    election_profile: &str,
    registry: Arc<KvStoreRegistry>,
) {
    info!(
        store_count = store_ids.len(),
        group_count = group_ids.len(),
        "creating stores and groups"
    );

    for (i, &store_id) in store_ids.iter().enumerate() {
        let port = ports[i];
        let addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        info!(store_id, bind_addr = %addr, "creating PxKvStore");
        let store = Arc::new(PxKvStore::new(store_id, addr));

        // Create groups with the single local replica for this store, if group_ids provided.
        // The election driver auto-starts in PxKvStore::add_group; the local replica
        // begins as Follower and is promoted via Paxos PreVote/RequestVote.
        for &group_id in group_ids {
            info!(
                store_id,
                group_id, replica_id, "creating PxGroup with local replica"
            );
            let local_replica = PxLocalReplica::new(replica_id, PxLocalReplicaRole::Follower);
            let mut group = PxGroup::new(group_id, local_replica);
            if election_profile == "test" {
                group.set_election_config(PxElectionConfig::for_tests());
            }
            store.add_group(group);
        }

        if let Err(e) = store.start().await {
            tracing::error!(store_id, port, error = %e, "failed to start store, skipping");
            continue;
        }

        info!(
            store_id,
            listen_addr = ?store.listen_addr(),
            group_count = group_ids.len(),
            "PxKvStore started successfully"
        );
        registry.add_store(store_id, store);
    }

    info!(
        store_count = registry.stores.len(),
        "all stores started, management API ready"
    );
}

/// Gracefully cascade-shutdown every store via [`PxKvStore::shutdown`].
///
/// Continues on errors; aggregates `critical:` messages to the operator.
async fn graceful_shutdown(registry: Arc<KvStoreRegistry>) {
    info!(
        store_count = registry.stores.len(),
        "initiating graceful shutdown of gRPC stores"
    );
    let mut total_errors = 0usize;
    for entry in &registry.stores {
        let store_id = *entry.key();
        let report = entry
            .value()
            .shutdown(std::time::Duration::from_millis(
                ServerConfig::DEFAULT.shutdown_timeout_ms,
            ))
            .await;
        if !report.is_clean() {
            total_errors += report.errors.len();
            for err in &report.errors {
                tracing::error!(store_id, "{err}");
            }
        }
    }
    if total_errors == 0 {
        info!("crowkv-server shut down cleanly");
    } else {
        tracing::warn!(
            error_count = total_errors,
            "crowkv-server shut down with errors (see critical: logs above)"
        );
    }
}
