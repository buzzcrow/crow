// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crowdb_console_shared::clients::console::DeployNodeServerBody;
use crowdb_console_shared::cluster::NodeHealth;
use crowdb_protocol::NodeId;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ServerVerb {
    /// Deploy a `crowdb-kv-server` on the given node. The console owns
    /// the SSH/local-fork transport; the CLI just forwards ports.
    Deploy {
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'r', long)]
        rest_port: u16,
        #[arg(short = 'R', long)]
        rpc_port: u16,
        /// Override `CROWDB_KV_SERVER_BIN` for this deploy.
        #[arg(short = 'b', long)]
        binary: Option<String>,
    },
    /// Restart the `crowdb-kv-server` on a node: stop the tracked
    /// process (if any) and re-deploy on the same recorded ports.
    /// Aliased as `start` for backward compatibility.
    #[command(alias = "start")]
    Restart {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// Stop the `crowdb-kv-server` running on the given node.
    Stop {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// List every deployed `crowdb-kv-server` (node, endpoints, health).
    List,
}

pub async fn run_server_verb(cli: &Cli, verb: ServerVerb) -> ExitCode {
    match verb {
        ServerVerb::Deploy {
            node,
            rest_port,
            rpc_port,
            binary,
        } => match node.parse::<NodeId>() {
            Ok(nid) => server_deploy(cli, nid, rest_port, rpc_port, binary).await,
            Err(e) => {
                eprintln!("error: invalid node id {node:?}: {e}");
                ExitCode::from(1)
            }
        },
        ServerVerb::Restart { node } => match node.parse::<NodeId>() {
            Ok(nid) => server_restart(cli, nid).await,
            Err(e) => {
                eprintln!("error: invalid node id {node:?}: {e}");
                ExitCode::from(1)
            }
        },
        ServerVerb::Stop { node } => match node.parse::<NodeId>() {
            Ok(nid) => server_stop(cli, nid).await,
            Err(e) => {
                eprintln!("error: invalid node id {node:?}: {e}");
                ExitCode::from(1)
            }
        },
        ServerVerb::List => server_list(cli).await,
    }
}

async fn server_list(cli: &Cli) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.list_servers().await {
        Ok(servers) => {
            if cli.json {
                return print_json(&servers);
            }
            if servers.is_empty() {
                println!("(no servers deployed)");
                return ExitCode::SUCCESS;
            }
            println!(
                "{:<12}  {:<26}  {:<26}  {:<8}  HEALTH",
                "NODE", "MGMT", "RPC", "PID"
            );
            for s in &servers {
                let health = match s.health {
                    NodeHealth::Up => "up",
                    NodeHealth::Down => "down",
                    NodeHealth::Unknown => "unknown",
                };
                println!(
                    "{:<12}  {:<26}  {:<26}  {:<8}  {health}",
                    s.node_id.map_or_else(|| "-".to_string(), |n| n.to_string()),
                    s.mgmt_url,
                    s.rpc_url.as_deref().unwrap_or("-"),
                    s.pid.map_or_else(|| "-".to_string(), |p| p.to_string()),
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: list servers: {e}");
            ExitCode::from(2)
        }
    }
}

async fn server_deploy(
    cli: &Cli,
    node_id: NodeId,
    rest_port: u16,
    rpc_port: u16,
    binary: Option<String>,
) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let body = DeployNodeServerBody {
        rest_port,
        rpc_port,
        binary,
        ..Default::default()
    };
    match client.deploy_node_server(node_id, &body).await {
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            println!(
                "deployed server on node {} -> {} (pid {}, rpc {})",
                r.node_id, r.mgmt_url, r.pid, r.rpc_url
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: deploy on node {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn server_restart(cli: &Cli, node_id: NodeId) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.restart_node_server(node_id).await {
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            println!(
                "restarted crowdb-kv-server on node {} -> {} (pid {}, rpc {})",
                r.node_id, r.mgmt_url, r.pid, r.rpc_url
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: restart on node {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn server_stop(cli: &Cli, node_id: NodeId) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.stop_node_server(node_id).await {
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            if r.sent {
                println!("sent SIGTERM to crowdb-kv-server on node {node_id}");
            } else {
                println!("crowdb-kv-server on node {node_id} was already gone");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: stop on node {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}
