// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `cluster` domain — cluster-level ops: init, reset, clean, status,
//! topology, plus hardware subcommands (rack/node/disk-group/disk).

use std::process::ExitCode;

use clap::Subcommand;

use crate::commands::{
    op_context, print_json, run_disk_group_verb, run_disk_verb, run_node_verb, run_rack_verb, DiskGroupVerb,
    DiskVerb, NodeVerb, RackVerb,
};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ClusterVerb {
    /// Initialize the cluster by bootstrapping group 0 on the listed nodes.
    Init {
        #[arg(short = 'n', long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
    /// Tear down the entire cluster (all groups, stores, servers, sysdata).
    Reset,
    /// Remove orphaned sysdata entries without stopping running servers.
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
                Ok(()) => {
                    if !cli.json {
                        println!("cluster clean complete");
                    }
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
