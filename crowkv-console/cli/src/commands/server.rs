// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crowkv_console_shared::clients::console::DeployNodeServerBody;
use crowkv_console_shared::cluster::NodeHealth;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ServerVerb {
    /// Deploy a `crowkv-server` on the given node. The console owns
    /// the SSH/local-fork transport; the CLI just forwards ports.
    Deploy {
        #[arg(long)]
        node: String,
        #[arg(long)]
        mgmt_port: u16,
        #[arg(long)]
        grpc_port: u16,
        /// Override `CROWKV_SERVER_BIN` for this deploy.
        #[arg(long)]
        binary: Option<String>,
    },
    /// Restart the `crowkv-server` on a node: stop the tracked
    /// process (if any) and re-deploy on the same recorded ports.
    /// Aliased as `start` for backward compatibility.
    #[command(alias = "start")]
    Restart {
        #[arg(long)]
        node: String,
    },
    /// Stop the `crowkv-server` running on the given node.
    Stop {
        #[arg(long)]
        node: String,
    },
    /// List every deployed `crowkv-server` (node, endpoints, health).
    List,
}

pub async fn run_server_verb(cli: &Cli, verb: ServerVerb) -> ExitCode {
    match verb {
        ServerVerb::Deploy {
            node,
            mgmt_port,
            grpc_port,
            binary,
        } => server_deploy(cli, &node, mgmt_port, grpc_port, binary).await,
        ServerVerb::Restart { node } => server_restart(cli, &node).await,
        ServerVerb::Stop { node } => server_stop(cli, &node).await,
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
                "NODE", "MGMT", "GRPC", "PID"
            );
            for s in &servers {
                let health = match s.health {
                    NodeHealth::Up => "up",
                    NodeHealth::Down => "down",
                    NodeHealth::Unknown => "unknown",
                };
                println!(
                    "{:<12}  {:<26}  {:<26}  {:<8}  {health}",
                    s.node_id.as_deref().unwrap_or("-"),
                    s.mgmt_url,
                    s.grpc_url.as_deref().unwrap_or("-"),
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
    node_id: &str,
    mgmt_port: u16,
    grpc_port: u16,
    binary: Option<String>,
) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let body = DeployNodeServerBody {
        mgmt_port,
        grpc_port,
        binary,
    };
    match client.deploy_node_server(node_id, &body).await {
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            println!(
                "deployed server on node {} -> {} (pid {}, grpc {})",
                r.node_id, r.mgmt_url, r.pid, r.grpc_url
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: deploy on node {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn server_restart(cli: &Cli, node_id: &str) -> ExitCode {
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
                "restarted crowkv-server on node {} -> {} (pid {}, grpc {})",
                r.node_id, r.mgmt_url, r.pid, r.grpc_url
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: restart on node {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn server_stop(cli: &Cli, node_id: &str) -> ExitCode {
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
                println!("sent SIGTERM to crowkv-server on node {node_id}");
            } else {
                println!("crowkv-server on node {node_id} was already gone");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: stop on node {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}
