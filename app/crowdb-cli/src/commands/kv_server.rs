// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `kv server` command handlers — deploy/restart/stop/delete/list.

use std::process::ExitCode;

use clap::Subcommand;
use crowdb_protocol::NodeId;

use crate::commands::{op_context, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum KvServerVerb {
    Deploy {
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'r', long)]
        rest_port: u16,
        #[arg(short = 'R', long)]
        rpc_port: u16,
        #[arg(short = 'b', long)]
        binary: Option<String>,
    },
    #[command(alias = "start")]
    Restart {
        #[arg(short = 'n', long)]
        node: String,
    },
    Stop {
        #[arg(short = 'n', long)]
        node: String,
    },
    Delete {
        #[arg(short = 'n', long)]
        node: String,
    },
    List,
}

#[allow(clippy::too_many_lines)]
pub async fn run_kv_server_verb(cli: &Cli, verb: KvServerVerb) -> ExitCode {
    match verb {
        KvServerVerb::Deploy {
            node,
            rest_port,
            rpc_port,
            binary,
        } => {
            let node_id: NodeId = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_server::deploy(&ctx, node_id, rest_port, rpc_port, binary)
                .await
            {
                Ok(d) => {
                    if cli.json {
                        return print_json(
                            cli,
                            &serde_json::json!({
                                "node_id": node_id,
                                "mgmt_url": d.mgmt_url,
                                "rpc_url": d.rpc_url,
                                "pid": d.pid,
                            }),
                        );
                    }
                    println!(
                        "deployed server on node {} -> {} (pid {}, rpc {})",
                        node_id, d.mgmt_url, d.pid, d.rpc_url
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: deploy on node {node_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvServerVerb::Restart { node } => {
            let node_id: NodeId = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_server::restart(&ctx, node_id).await {
                Ok(d) => {
                    if cli.json {
                        return print_json(
                            cli,
                            &serde_json::json!({
                                "node_id": node_id,
                                "mgmt_url": d.mgmt_url,
                                "rpc_url": d.rpc_url,
                                "pid": d.pid,
                            }),
                        );
                    }
                    println!(
                        "restarted server on node {} -> {} (pid {}, rpc {})",
                        node_id, d.mgmt_url, d.pid, d.rpc_url
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: restart on node {node_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvServerVerb::Stop { node } => {
            let node_id: NodeId = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_server::stop(&ctx, node_id).await {
                Ok(sent) => {
                    if cli.json {
                        return print_json(cli, &serde_json::json!({"sent": sent}));
                    }
                    if sent {
                        println!("sent SIGTERM to server on node {node_id}");
                    } else {
                        println!("server on node {node_id} was already gone");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: stop on node {node_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvServerVerb::Delete { node } => {
            let node_id: NodeId = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_server::delete(&ctx, node_id).await {
                Ok(()) => {
                    if !cli.json {
                        println!("deleted server on node {node_id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: delete on node {node_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        KvServerVerb::List => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            let servers = crowdb_console_shared::ops::kv_server::list(&ctx);
            if cli.json {
                return print_json(cli, &servers);
            }
            if servers.is_empty() {
                println!("(no servers deployed)");
                return ExitCode::SUCCESS;
            }
            println!("{:<12}  {:<26}  {:<26}  {:<8}", "NODE", "MGMT", "RPC", "PID");
            for s in &servers {
                println!(
                    "{:<12}  {:<26}  {:<26}  {:<8}",
                    s.node_id.map_or_else(|| "-".into(), |n| n.to_string()),
                    s.url,
                    s.rpc_url.as_deref().unwrap_or("-"),
                    s.pid.map_or_else(|| "-".into(), |p| p.to_string()),
                );
            }
            ExitCode::SUCCESS
        }
    }
}
