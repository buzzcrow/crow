// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware command handlers: rack, node, disk-group, disk.
//! Delegates to `ops::hardware`.

use std::process::ExitCode;

use clap::Subcommand;
use crowdb_console_shared::config::NodeEntry;
use crowdb_protocol::{NodeId, RackId};

use crate::commands::{commit_config, op_context, print_json};
use crate::Cli;

// ── rack ─────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum RackVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'n', long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

pub async fn run_rack_verb(cli: &Cli, verb: RackVerb) -> ExitCode {
    match verb {
        RackVerb::Add { id, name } => {
            let rack_id: RackId = match id.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::hardware::add_rack(&ctx, rack_id, &name).await {
                Ok(entry) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if cli.json {
                        return print_json(cli, &entry);
                    }
                    println!("added rack {}", entry.id);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add rack {id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        RackVerb::Remove { id } => {
            let rack_id: RackId = match id.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::hardware::remove_rack(&ctx, rack_id).await {
                Ok(()) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if !cli.json {
                        println!("removed rack {id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove rack {id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        RackVerb::List => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let racks = crowdb_console_shared::ops::hardware::list_racks(&ctx);
            if cli.json {
                return print_json(cli, &racks);
            }
            if racks.is_empty() {
                println!("(no racks)");
                return ExitCode::SUCCESS;
            }
            println!("{:<16}  NAME", "ID");
            for r in &racks {
                println!("{:<16}  {}", r.id, r.name);
            }
            ExitCode::SUCCESS
        }
    }
}

// ── node ─────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum NodeVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'r', long)]
        rack: String,
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short = 'P', long, default_value_t = 22)]
        ssh_port: u16,
        #[arg(short = 'u', long, default_value = "")]
        ssh_user: String,
        #[arg(short = 'k', long)]
        ssh_key: Option<String>,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
    #[command(alias = "ls")]
    ListRack {
        #[arg(short = 'r', long)]
        rack: String,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_node_verb(cli: &Cli, verb: NodeVerb) -> ExitCode {
    match verb {
        NodeVerb::Add {
            id,
            rack,
            host,
            ssh_port,
            ssh_user,
            ssh_key,
        } => {
            let node_id: NodeId = match id.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let rack_id: RackId = match rack.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {rack:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let entry = NodeEntry {
                id: node_id,
                rack_id,
                host,
                ssh_port,
                ssh_user,
                ssh_key,
                ssh_password: None,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::hardware::add_node(&ctx, entry.clone()).await {
                Ok(e) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if cli.json {
                        return print_json(cli, &e);
                    }
                    println!("added node {} (rack {})", e.id, e.rack_id);
                    ExitCode::SUCCESS
                }
                Err(err) => {
                    eprintln!("error: add node {id}: {err}");
                    ExitCode::from(2)
                }
            }
        }
        NodeVerb::Remove { id } => {
            let node_id: NodeId = match id.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id {id:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::hardware::remove_node(&ctx, node_id).await {
                Ok(()) => {
                    if let Err(c) = commit_config(cli, &ctx) {
                        return c;
                    }
                    if !cli.json {
                        println!("removed node {id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove node {id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        NodeVerb::List => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let nodes = crowdb_console_shared::ops::hardware::list_nodes(&ctx, None);
            print_node_table(cli, &nodes)
        }
        NodeVerb::ListRack { rack } => {
            let rack_id: RackId = match rack.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid rack id {rack:?}: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let nodes = crowdb_console_shared::ops::hardware::list_nodes(&ctx, Some(rack_id));
            print_node_table(cli, &nodes)
        }
    }
}

fn print_node_table(cli: &Cli, nodes: &[NodeEntry]) -> ExitCode {
    if cli.json {
        return print_json(cli, &nodes.to_vec());
    }
    if nodes.is_empty() {
        println!("(no nodes)");
        return ExitCode::SUCCESS;
    }
    println!("{:<8}  {:<8}  {:<16}  {:<8}  SSH", "ID", "RACK", "HOST", "PORT");
    for n in nodes {
        println!(
            "{:<8}  {:<8}  {:<16}  {:<8}  {}",
            n.id, n.rack_id, n.host, n.ssh_port, n.ssh_user
        );
    }
    ExitCode::SUCCESS
}

// ── disk-group ───────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum DiskGroupVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'r', long)]
        rack: String,
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'N', long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

pub async fn run_disk_group_verb(cli: &Cli, verb: DiskGroupVerb) -> ExitCode {
    let _ = (cli, verb);
    eprintln!("disk-group commands not yet wired to ops (Phase 3)");
    ExitCode::from(1)
}

// ── disk ─────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum DiskVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'r', long)]
        rack: String,
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 't', long)]
        disk_type: String,
        #[arg(short = 'c', long)]
        capacity: String,
        #[arg(short = 'z', long)]
        zone_size: String,
        #[arg(short = 'u', long)]
        unit_size: String,
        #[arg(short = 'd', long, default_value = "")]
        device_path: String,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

pub async fn run_disk_verb(cli: &Cli, verb: DiskVerb) -> ExitCode {
    let _ = (cli, verb);
    eprintln!("disk commands not yet wired to ops (Phase 3)");
    ExitCode::from(1)
}
