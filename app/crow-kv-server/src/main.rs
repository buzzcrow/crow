// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-kv-server` — `CrowKV` daemon entry point.
//!
//! Wraps the `crow_kv` library into a runnable server process with:
//! - CLI-driven startup of `PxKvStore` instances with `PxGroup`s.
//! - HTTP management API for runtime topology control.
//! - Graceful shutdown on SIGINT/SIGTERM.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tracing::{debug, info, warn};

use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::local_replica::PxLocalReplicaRole;
use crow_kv::cluster::px_kv_store::PxKvStore;
use crow_kv::common::config::{CrowKVConfig, PxElectionConfig, ServerConfig};
use crow_kv::metrics::MetricsRunner;

use crow_kv_server::cli::{parse_id_list, parse_port_list, Cli};
use crow_kv_server::mgmt::{self, persisted_port_for_store};
use crow_kv_server::startup::create_group_with_wal;
use crow_kv_server::store_registry::KvStoreRegistry;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let args = Cli::parse();

    let _guards = if args.log {
        crow_kv::common::logging::init_file_and_console_logging(
            "log",
            "crow-kv-server",
            args.log_max_file_mb,
            args.log_max_files,
        )
        .expect("failed to initialize crow-kv-server logging")
    } else {
        crow_kv::common::logging::init_file_logging(
            "log",
            "crow-kv-server",
            args.log_max_file_mb,
            args.log_max_files,
        )
        .expect("failed to initialize crow-kv-server logging")
    };

    // Initialize the C++ spdlog async logger as a process-global resource.
    // This must happen before any Crowtree::open() so all engine instances
    // share one logger. No-op when the build has no spdlog.
    crow_tree_ffi::ct_init_logging(
        "log",
        "info",
        args.log_max_file_mb,
        args.log_max_files,
        "crow-kv-server-tree",
    );

    info!("crow-kv-server starting...");

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
        max_inflight = args.max_inflight,
        "parsed CLI arguments"
    );

    let bootstrap = parse_and_validate_cli_args(&args);

    // Load config: from --config file (optional, first-boot tunables
    // only). When omitted, use defaults. Paths are always derived from
    // --root via apply_root (fixed on-disk layout).
    let mut config = match args.config.as_ref() {
        Some(path) => CrowKVConfig::load_from_file(path)
            .unwrap_or_else(|e| panic!("failed to load config from {}: {e}", path.display())),
        None => CrowKVConfig::default(),
    };
    config.apply_root(&args.root);

    // CLI tunable overrides.
    config.election = match args.election_profile.as_str() {
        "test" => PxElectionConfig::for_tests(),
        "e2e" => PxElectionConfig::for_e2e(),
        _ => PxElectionConfig::DEFAULT,
    };
    config.wal_backend = args.wal_backend.clone();
    config.crowtree_backend = args.kv_backend.clone();
    config.wal_skip_fsync = args.no_fsync;
    config.paxos.max_inflight_proposals = args.max_inflight;
    if let Some(max_keys) = args.coalesce_max_keys {
        config.paxos.coalesce_max_keys = max_keys;
    }
    if let Some(threshold) = args.coalesce_drain_threshold {
        config.paxos.coalesce_drain_threshold = threshold;
    }

    let registry = Arc::new(
        KvStoreRegistry::with_config(config.clone()).with_metrics_registry(
            metrics_runner.as_ref().map_or_else(
                || Arc::new(std::sync::Mutex::new(crow_kv::metrics::MetricsRegistry::new())),
                |r| r.registry().clone(),
            ),
        ),
    );

    // Spawn a config file watcher for diff logging. Only when --config is
    // passed; with no toml there is nothing to watch.
    let watcher_old_config = Arc::new(std::sync::Mutex::new(config));
    let _config_watcher = match args.config.as_ref() {
        Some(path) => {
            match crow_common::config::watch::<CrowKVConfig, _>(std::path::Path::new(path), move |new| {
                let mut old = watcher_old_config.lock().unwrap();
                crow_common::config::log_diff(&*old, new);
                *old = new.clone();
            }) {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::warn!(error = %e, "config file watcher failed to start; live reload disabled");
                    None
                }
            }
        }
        None => None,
    };

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

    let router = mgmt::router(crow_kv_server::operation_registry::AppState::new(
        registry.clone(),
    ));
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

    // Boot stores/groups. Two modes:
    // - Restore mode: group 0 is on disk (`<wal_root>/store0/group0`).
    //   Scan local waldata, load every store/group from disk (replay WAL
    //   + open crow-tree + apply node-config.json membership), then
    //   reconcile with group 0 as verification/fallback. The toml and
    //   --stores/--groups are ignored for topology.
    // - First-boot mode: no group 0 on disk. Use --stores/--groups CLI
    //   args (if given) to create stores; otherwise boot empty so the
    //   operator can call POST /system/init.
    let local_groups = crow_kv_server::restore::scan_local_groups(&registry.config.wal_root)
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, wal_root = %registry.config.wal_root.display(), "scan_local_groups failed; treating as empty");
            Vec::new()
        });
    if crow_kv_server::restore::group0_exists(&registry.config.wal_root) {
        info!(
            local_count = local_groups.len(),
            "restore mode: group 0 present on disk, loading local stores/groups"
        );
        if bootstrap.is_some() {
            warn!("restore mode: --stores/--groups ignored (local disk is the source of truth)");
        }
        crow_kv_server::restore::load_local_groups(&local_groups, args.replica, &registry).await;
        crow_kv_server::reconcile::reconcile_with_group0(&registry).await;
    } else {
        info!("first-boot mode: no group 0 on disk");
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
        // Reconcile is a no-op without group 0; skip the call.
    }

    // Start the keep-alive loop (registers under /srv/kv-server/<id>).
    let keepalive = if args.keepalive_interval > 0 {
        let instance_id = args.instance_id.unwrap_or_else(|| {
            let id = crow_kv_client::new_client_id();
            info!(instance_id = id, "keep-alive: generated instance id");
            id
        });
        let mgmt_endpoint = format!("http://{display_addr}");
        // The group-0 gRPC endpoint is the first store's listen addr,
        // or the management addr as a fallback (single-node dev).
        let group0_ep = registry
            .get_store(0)
            .and_then(|s| s.listen_addr().map(|a| a.to_string()))
            .unwrap_or_else(|| format!("http://{display_addr}"));
        Some(crow_kv_server::keepalive::KeepAliveLoop::spawn(
            registry.clone(),
            instance_id,
            mgmt_endpoint,
            &group0_ep,
            registry
                .config
                .node_root
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            args.keepalive_interval,
        ))
    } else {
        None
    };

    // Start the chunkdb range binding monitor (leader-gated on group-0).
    // Reuses the keep-alive group-0 endpoint derivation. Only the
    // group-0 leader writes the binding table; followers compute only.
    let binding_monitor = if args.binding_monitor_interval > 0 {
        let group0_ep = registry
            .get_store(0)
            .and_then(|s| s.listen_addr().map(|a| a.to_string()))
            .unwrap_or_else(|| format!("http://{display_addr}"));
        Some(
            crow_kv_server::binding_monitor_wiring::spawn_chunkdb_binding_monitor(
                &registry,
                group0_ep,
                args.binding_monitor_interval,
            ),
        )
    } else {
        None
    };

    // Wire engine stats collector into the metrics runner, then start it.
    // The collector polls C++ engine counters via ct_get_stats each tick,
    // computes deltas, and inc_by()s on registered Rust counters so they
    // appear in the metrics log alongside the Rust-side metrics.
    if let Some(ref mut runner) = metrics_runner {
        let reg = runner.registry().clone();
        crow_kv_server::engine_collector::setup_engine_collector(&reg, &registry, runner);
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
    if let Some(ka) = keepalive {
        ka.stop().await;
        info!("keep-alive loop stopped");
    }
    if let Some(bm) = binding_monitor {
        bm.stop();
        info!("chunkdb binding monitor stopped");
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
        crow_kv::common::logging::open_metrics_log("log", "crow-kv-server", max_file_mb, max_files)
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
#[allow(clippy::too_many_arguments)]
async fn create_and_start_stores(
    store_ids: &[u64],
    group_ids: &[u64],
    replica_id: u64,
    ports: Vec<u16>,
    registry: Arc<KvStoreRegistry>,
) {
    debug!(
        store_count = store_ids.len(),
        group_count = group_ids.len(),
        "creating stores and groups"
    );

    for (i, &store_id) in store_ids.iter().enumerate() {
        // Port priority: explicit CLI port > persisted config file > OS-assigned (0).
        let port = if ports[i] > 0 {
            ports[i]
        } else {
            persisted_port_for_store(&registry.config.config_root, store_id)
                .await
                .unwrap_or(0)
        };
        let addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        debug!(store_id, bind_addr = %addr, "creating PxKvStore");
        let mut store = PxKvStore::new(store_id, addr);
        if let Some(ref mr) = registry.metrics_registry {
            store.set_metrics_registry(Arc::clone(mr));
        }
        store.set_scan_byte_budget(registry.config.server.scan_byte_budget);
        let store = Arc::new(store);

        // Create groups with the single local replica for this store, if group_ids provided.
        // The election driver auto-starts in PxKvStore::add_group; the local replica
        // begins as Follower and is promoted via Paxos PreVote/RequestVote.
        for &group_id in group_ids {
            debug!(
                store_id,
                group_id, replica_id, "creating PxGroup with local replica"
            );
            let group = match create_group_with_wal(
                store_id,
                group_id,
                replica_id,
                PxLocalReplicaRole::Follower,
                &registry.config,
                registry.wal_backend.clone(),
                registry.crowtree_backend,
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

        // R32: start the crow-rpc server alongside the gRPC server.
        let rt = tokio::runtime::Handle::current();
        if let Err(e) = store.start_rpc_server(rt.clone()) {
            tracing::error!(store_id, port, error = %e, "failed to start crow-rpc server, skipping");
            continue;
        }

        // R117: start the client-facing crow-rpc server.
        if let Err(e) = store.start_client_rpc_server(rt.clone()) {
            tracing::error!(store_id, port, error = %e, "failed to start client crow-rpc server, skipping");
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

    debug!(
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
    crow_tree_ffi::ct_flush_logging();

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
        info!("crow-kv-server shut down cleanly");
    } else {
        tracing::warn!(
            error_count = total_errors,
            "crow-kv-server shut down with errors (see critical: logs above)"
        );
    }

    // Final flush + stop the C++ spdlog async logger. All Crowtree instances
    // are now dropped (or about to be), so this is safe.
    crow_tree_ffi::ct_flush_logging();
    crow_tree_ffi::ct_shutdown_logging();
}
