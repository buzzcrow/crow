// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crowkv-server` — `CrowKV` daemon entry point.
//!
//! Wraps the `crowkv` library into a runnable server process with:
//! - CLI-driven startup of `PxKvStore` instances with `PxGroup`s.
//! - HTTP management API for runtime topology control.
//! - Graceful shutdown on SIGINT/SIGTERM.

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use tracing::{info, warn};

use crowkv::cluster::kv_server::KvServer;
use crowkv::cluster::local_replica::PxLocalReplicaRole;
use crowkv::cluster::px_kv_store::PxKvStore;
use crowkv::common::config::{PxElectionConfig, ServerConfig};
use crowkv::metrics::MetricsRunner;

use crowkv_server::cli::{parse_id_list, parse_port_list, Cli};
use crowkv_server::mgmt_api::{self, persisted_port_for_store};
use crowkv_server::startup::create_group_with_wal;
use crowkv_server::store_registry::KvStoreRegistry;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();

    let _guards = if args.log {
        crowkv::common::logging::init_file_and_console_logging(
            "log",
            "crowkv-server",
            args.log_max_file_mb,
            args.log_max_files,
        )
        .expect("failed to initialize crowkv-server logging")
    } else {
        crowkv::common::logging::init_file_logging(
            "log",
            "crowkv-server",
            args.log_max_file_mb,
            args.log_max_files,
        )
        .expect("failed to initialize crowkv-server logging")
    };

    // Initialize the C++ spdlog async logger as a process-global resource.
    // This must happen before any Crowtree::open() so all engine instances
    // share one logger. No-op when the build has no spdlog.
    crowtree_ffi::ct_init_logging(
        "log",
        "info",
        args.log_max_file_mb,
        args.log_max_files,
        "crowkv-server-tree",
    );

    info!("crowkv-server starting...");

    // Metrics runner: periodic flush to a dedicated metrics log file.
    let mut metrics_runner =
        create_metrics_runner(args.metrics_interval, args.log_max_file_mb, args.log_max_files);

    info!(
        stores = ?args.stores.as_deref(),
        groups = ?args.groups.as_deref(),
        replica = args.replica,
        ports = ?args.ports.as_deref(),
        management_addr = %args.management_addr,
        management_port = args.management_port,
        election_profile = %args.election_profile,
        kv_backend = %args.kv_backend,
        wal_backend = %args.wal_backend,
        "parsed CLI arguments"
    );

    let bootstrap = parse_and_validate_cli_args(&args);

    let election_cfg = match args.election_profile.as_str() {
        "test" => PxElectionConfig::for_tests(),
        "e2e" => PxElectionConfig::for_e2e(),
        "bench" => PxElectionConfig::for_bench(),
        _ => PxElectionConfig::DEFAULT,
    };

    let wal_root = args.wal_root.clone().unwrap_or_else(|| PathBuf::from("wal"));
    let config_root = args
        .config_root
        .clone()
        .unwrap_or_else(|| wal_root.parent().unwrap_or_else(|| Path::new("")).join("conf"));
    let data_root = args
        .data_root
        .clone()
        .unwrap_or_else(|| wal_root.parent().unwrap_or_else(|| Path::new("")).join("ctdata"));
    let crowtree_backend = crowkv_server::store_registry::parse_crowtree_backend(&args.kv_backend);
    let wal_backend = Arc::new(crowkv_server::store_registry::parse_wal_backend(
        &args.wal_backend,
    ));

    let registry = Arc::new(
        KvStoreRegistry::with_runtime(election_cfg, wal_root, config_root, wal_backend)
            .with_data_root(data_root)
            .with_crowtree_backend(crowtree_backend)
            .with_metrics_registry(metrics_runner.as_ref().map_or_else(
                || Arc::new(std::sync::Mutex::new(crowkv::metrics::MetricsRegistry::new())),
                |r| r.registry().clone(),
            ))
            .with_wal_skip_fsync(args.no_fsync),
    );

    // Populate the port pool from `--ports` even when `--stores` is not
    // provided, so stores created later via the management API can use
    // these ports instead of falling back to stale persisted config ports.
    if let Some(ref port_str) = args.ports {
        match parse_port_list(port_str) {
            Ok(ports) => {
                registry.set_port_pool(ports);
            }
            Err(e) => {
                eprintln!("error: invalid --ports: {e}");
                std::process::exit(1);
            }
        }
    }

    // Start HTTP management server first
    let mgmt_addr: SocketAddr = format!("{}:{}", args.management_addr, args.management_port)
        .parse()
        .unwrap_or_else(|e| {
            eprintln!("error: invalid management address: {e}");
            std::process::exit(1);
        });

    let router = mgmt_api::router(crowkv_server::operation_registry::AppState::new(registry.clone()));
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
            registry.clone(),
        )
        .await;
    }

    // Wire engine stats collector into the metrics runner, then start it.
    // The collector polls C++ engine counters via ct_get_stats each tick,
    // computes deltas, and inc_by()s on registered Rust counters so they
    // appear in the metrics log alongside the Rust-side metrics.
    if let Some(ref mut runner) = metrics_runner {
        let reg = runner.registry().clone();
        crowkv_server::engine_collector::setup_engine_collector(&reg, &registry, runner);
        runner.start();
        info!(interval_secs = args.metrics_interval, "metrics runner started");
    }

    // Serve until shutdown signal
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|e| {
            tracing::error!("management HTTP server error: {e}");
        });

    // Graceful cascade shutdown (PxKvStore → PxGroup → replicas)
    if let Some(ref mut runner) = metrics_runner {
        runner.stop().await;
        info!("metrics runner stopped");
    }
    graceful_shutdown(registry).await;
}

/// Create a metrics runner if interval > 0, else None. Does NOT start
/// the runner — call `start()` after wiring collectors.
fn create_metrics_runner(interval_secs: u64, max_file_mb: usize, max_files: usize) -> Option<MetricsRunner> {
    if interval_secs == 0 {
        return None;
    }
    let metrics_file =
        crowkv::common::logging::open_metrics_log("log", "crowkv-server", max_file_mb, max_files)
            .expect("failed to open metrics log file");
    let runner = MetricsRunner::new(metrics_file, interval_secs);
    Some(runner)
}

async fn shutdown_signal() {
    let mut term_stream = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received SIGINT (CTRL+C), initiating graceful shutdown");
        }
        _ = term_stream.recv() => {
            warn!("received SIGTERM, initiating graceful shutdown");
        }
    }
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
    registry: Arc<KvStoreRegistry>,
) {
    info!(
        store_count = store_ids.len(),
        group_count = group_ids.len(),
        "creating stores and groups"
    );

    for (i, &store_id) in store_ids.iter().enumerate() {
        // Port priority: explicit CLI port > persisted config file > OS-assigned (0).
        let port = if ports[i] > 0 {
            ports[i]
        } else {
            persisted_port_for_store(&registry.config_root, store_id)
                .await
                .unwrap_or(0)
        };
        let addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        info!(store_id, bind_addr = %addr, "creating PxKvStore");
        let mut store = PxKvStore::new(store_id, addr);
        if let Some(ref mr) = registry.metrics_registry {
            store.set_metrics_registry(Arc::clone(mr));
        }
        let store = Arc::new(store);

        // Create groups with the single local replica for this store, if group_ids provided.
        // The election driver auto-starts in PxKvStore::add_group; the local replica
        // begins as Follower and is promoted via Paxos PreVote/RequestVote.
        for &group_id in group_ids {
            info!(
                store_id,
                group_id, replica_id, "creating PxGroup with local replica"
            );
            let group = match create_group_with_wal(
                store_id,
                group_id,
                replica_id,
                PxLocalReplicaRole::Follower,
                registry.election_cfg,
                &registry.wal_root,
                &registry.config_root,
                registry.wal_backend.clone(),
                &registry.data_root,
                registry.crowtree_backend,
                registry.wal_skip_fsync,
                "log",
            )
            .await
            {
                Ok(group) => group,
                Err(e) => {
                    tracing::error!(store_id, group_id, error = %e, "failed to create WAL-backed group");
                    continue;
                }
            };
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

    // Flush C++ logs before store shutdown so any in-flight engine messages
    // are on disk before the engines start tearing down.
    crowtree_ffi::ct_flush_logging();

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

    // Final flush + stop the C++ spdlog async logger. All Crowtree instances
    // are now dropped (or about to be), so this is safe.
    crowtree_ffi::ct_flush_logging();
    crowtree_ffi::ct_shutdown_logging();
}
