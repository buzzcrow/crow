use clap::Subcommand;
use crowkv_console_shared::clients::console::DeployNodeServerBody;
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
    /// Aliased as `start` for parity with `design-console.md` §6.3.4.
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
    }
}

async fn server_deploy(cli: &Cli, node_id: &str, mgmt_port: u16, grpc_port: u16, binary: Option<String>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let body = DeployNodeServerBody { mgmt_port, grpc_port, binary };
    match client.deploy_node_server(node_id, &body).await {
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            println!("deployed server on node {} -> {} (pid {}, grpc {})", r.node_id, r.mgmt_url, r.pid, r.grpc_url);
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
            println!("restarted crowkv-server on node {} -> {} (pid {}, grpc {})", r.node_id, r.mgmt_url, r.pid, r.grpc_url);
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
