// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `cluster` domain — cluster-level ops: init, reset, clean, status,
//! topology, plus hardware subcommands (rack/node/disk-group/disk).

use std::process::ExitCode;

use clap::Subcommand;

use crate::commands::{
    commit_config, op_context, print_json, run_disk_group_verb, run_disk_verb, run_node_verb, run_rack_verb,
    DiskGroupVerb, DiskVerb, NodeVerb, RackVerb,
};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ClusterVerb {
    /// Initialize the cluster by bootstrapping group 0 on the listed nodes.
    Init {
        #[arg(short = 'n', long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
    /// Deploy a local N-node KV cluster on 127.0.0.1 (forks
    /// `crowdb-kv-server` on each node, bootstraps group 0).
    /// With `-t rpc`, deploys a standalone crowdb-rpc-fb-server (echo)
    /// for RPC regression bench.
    #[command(name = "local-deploy")]
    LocalDeploy {
        #[arg(short = 'n', long, default_value_t = 3)]
        nodes: usize,
        /// Service type: `kv` (default), `diskdb`, or `rpc`.
        #[arg(short = 't', long, default_value = "kv")]
        service_type: String,
        /// [rpc] Listen port. 0 = auto-allocate.
        #[arg(long, default_value_t = 0)]
        rpc_port: u16,
        /// [rpc] Independent epoll instances (round-robin).
        #[arg(long, default_value_t = 1)]
        io_engines: u32,
        /// [rpc] Total I/O worker threads.
        #[arg(long, default_value_t = 1)]
        io_workers: u32,
        /// [rpc] Enable Nagle's algorithm (disable `TCP_NODELAY`).
        #[arg(long, default_value_t = false)]
        enable_nagle: bool,
        /// [kv] `--rpc-workers` for the spawned server. 0 = server default (2).
        #[arg(long, default_value_t = 0)]
        rpc_workers: u32,
        /// [kv] `--peer-pool-size` for the spawned server. 0 = server default (2).
        #[arg(long, default_value_t = 0)]
        peer_pool_size: usize,
        /// [kv] `--max-inflight` for the spawned server. 0 = server default.
        #[arg(long, default_value_t = 0)]
        max_inflight: usize,
        /// [kv] `--coalesce-max-keys` for the spawned server. 0 = server default.
        #[arg(long, default_value_t = 0)]
        coalesce_max_keys: usize,
        /// [kv] `--coalesce-drain-threshold` for the spawned server. 0 = server default (`max_inflight/4`).
        #[arg(long, default_value_t = 0)]
        coalesce_drain_threshold: usize,
        /// [kv] Enable `--event-write` on the spawned server.
        #[arg(long, default_value_t = false)]
        event_write: bool,
        /// [kv] `--send-queue-capacity` for the spawned server. 0 = server default (4096).
        #[arg(long, default_value_t = 0)]
        send_queue_capacity: u32,
        /// [kv] `--metrics-interval` for the spawned server (seconds). 0 = server default (5).
        #[arg(long, default_value_t = 0)]
        metrics_interval: u64,
    },
    /// Tear down the entire cluster (all groups, stores, servers, sysdata).
    Destroy,
    /// Remove orphaned sysdata entries without stopping running servers.
    Reset,
    /// Wipe user data on every node + wait for re-election. Preserves
    /// group-0 sysdata + topology — servers stay running.
    Clean,
    /// Show cluster status (list all stores from group-0 sysdata).
    Status,
    /// Show the topology view from a node's `/topology` endpoint.
    Topology {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// Hardware: rack management.
    Rack {
        #[command(subcommand)]
        verb: RackVerb,
    },
    /// Hardware: node management.
    Node {
        #[command(subcommand)]
        verb: NodeVerb,
    },
    /// Hardware: disk-group management.
    #[command(name = "disk-group")]
    DiskGroup {
        #[command(subcommand)]
        verb: DiskGroupVerb,
    },
    /// Hardware: disk management.
    Disk {
        #[command(subcommand)]
        verb: DiskVerb,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_cluster_verb(cli: &Cli, verb: ClusterVerb) -> ExitCode {
    match verb {
        ClusterVerb::Init { nodes } => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let node_ids: Vec<u64> = match nodes.iter().map(|s| s.parse::<u64>()).collect::<Result<_, _>>() {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            match crowdb_console_shared::ops::cluster::init(&ctx, &node_ids).await {
                Ok(summary) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if cli.json {
                        return print_json(cli, &summary);
                    }
                    println!(
                        "cluster initialized: store {}, group {}, {} nodes",
                        summary.store_id,
                        summary.group_id,
                        summary.nodes.len()
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster init: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::LocalDeploy {
            nodes,
            service_type,
            rpc_port,
            io_engines,
            io_workers,
            enable_nagle,
            rpc_workers,
            peer_pool_size,
            max_inflight,
            coalesce_max_keys,
            coalesce_drain_threshold,
            event_write,
            send_queue_capacity,
            metrics_interval,
        } => match service_type.as_str() {
            "kv" => {
                let ctx = match op_context(cli) {
                    Ok(c) => c,
                    Err(c) => return c,
                };
                let tunables = crowdb_console_shared::ops::cluster::KvDeployTunables {
                    rpc_workers: nonzero(rpc_workers),
                    peer_pool_size: nonzero(peer_pool_size),
                    max_inflight: nonzero(max_inflight),
                    coalesce_max_keys: nonzero(coalesce_max_keys),
                    coalesce_drain_threshold: nonzero(coalesce_drain_threshold),
                    event_write: if event_write { Some(true) } else { None },
                    send_queue_capacity: nonzero(send_queue_capacity),
                    metrics_interval: nonzero(metrics_interval),
                };
                let workspace = deploy_workspace(cli);
                match crowdb_console_shared::ops::cluster::local_deploy(
                    &ctx,
                    nodes,
                    workspace.as_deref(),
                    Some(&tunables),
                )
                .await
                {
                    Ok(summary) => {
                        if let Err(c) = commit_config(cli, &ctx) {
                            return c;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!(
                            "local-deploy complete: {} nodes (rack {}, nodes [{}]), group 0 bootstrapped",
                            summary.node_count,
                            summary.rack_id,
                            summary
                                .node_ids
                                .iter()
                                .map(std::string::ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("error: local-deploy: {e}");
                        ExitCode::from(2)
                    }
                }
            }
            "rpc" => {
                let ctx = match op_context(cli) {
                    Ok(c) => c,
                    Err(c) => return c,
                };
                let rpc_cfg = crowdb_console_shared::ops::cluster::RpcDeployConfig {
                    port: rpc_port,
                    io_engines,
                    io_workers,
                    enable_nagle,
                    ..Default::default()
                };
                let workspace = deploy_workspace(cli);
                match crowdb_console_shared::ops::cluster::local_deploy_rpc(
                    &ctx,
                    &rpc_cfg,
                    workspace.as_deref(),
                )
                .await
                {
                    Ok(summary) => {
                        if let Err(c) = commit_config(cli, &ctx) {
                            return c;
                        }
                        if cli.json {
                            return print_json(cli, &summary);
                        }
                        println!(
                            "local-deploy rpc: port={}, pid={}, io_engines={}, io_workers={}, nagle={}",
                            summary.port, summary.pid, summary.io_engines, summary.io_workers, summary.nagle
                        );
                        ExitCode::SUCCESS
                    }
                    Err(e) => {
                        eprintln!("error: local-deploy rpc: {e}");
                        ExitCode::from(2)
                    }
                }
            }
            other => {
                eprintln!("error: local-deploy: unsupported service type `{other}` (expected kv or rpc)");
                ExitCode::from(1)
            }
        },
        ClusterVerb::Destroy => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::destroy(&ctx).await {
                Ok(()) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if !cli.json {
                        println!("cluster destroy complete");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster destroy: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Reset => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::reset(&ctx).await {
                Ok(()) => {
                    if !cli.json {
                        println!("cluster reset complete");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster reset: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Clean => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::clean(&ctx).await {
                Ok(result) => {
                    if cli.json {
                        return print_json(cli, &result);
                    }
                    println!(
                        "cluster clean: wiped {} nodes, leader = {}",
                        result.wiped_nodes, result.new_leader
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster clean: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Status => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::cluster::status(&ctx).await {
                Ok(stores) => {
                    if cli.json {
                        return print_json(cli, &stores);
                    }
                    if stores.is_empty() {
                        println!("(no stores)");
                    } else {
                        println!("{:<12}  {:<12}  NODES", "STORE", "REPLICAS");
                        for s in &stores {
                            println!(
                                "{:<12}  {:<12}  {}",
                                s.store_id,
                                s.node_ids.len(),
                                s.node_ids
                                    .iter()
                                    .map(std::string::ToString::to_string)
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster status: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Topology { node } => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let node_id: u64 = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            match crowdb_console_shared::ops::cluster::topology(&ctx, node_id).await {
                Ok(stores) => {
                    if cli.json {
                        return print_json(cli, &stores);
                    }
                    for s in &stores {
                        println!(
                            "store {} listen={}",
                            s.store_id,
                            s.listen_addr.as_deref().unwrap_or("-")
                        );
                        for g in &s.groups {
                            println!("  group {} leader={}", g.group_id, g.local_replica_id);
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: cluster topology: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ClusterVerb::Rack { verb } => run_rack_verb(cli, verb).await,
        ClusterVerb::Node { verb } => run_node_verb(cli, verb).await,
        ClusterVerb::DiskGroup { verb } => run_disk_group_verb(cli, verb).await,
        ClusterVerb::Disk { verb } => run_disk_verb(cli, verb).await,
    }
}

/// Convert 0 to `None` (use server default), pass through nonzero values.
fn nonzero<T: Copy + PartialEq + Default>(v: T) -> Option<T> {
    (v != T::default()).then_some(v)
}

/// Resolve the `local-deploy` workspace from the CLI's per-invocation
/// log dir. The workspace lands at `<log_dir>/deploy/` so server data
/// dirs stay separate from the CLI's own log files. Returns `None`
/// (falling back to the lib default) when `log_dir` is unset.
fn deploy_workspace(cli: &Cli) -> Option<std::path::PathBuf> {
    if cli.log_dir.as_os_str().is_empty() {
        None
    } else {
        Some(cli.log_dir.join("deploy"))
    }
}
