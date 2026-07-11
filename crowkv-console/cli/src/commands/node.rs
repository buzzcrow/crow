use clap::Subcommand;
use crowkv_console_shared::config::NodeEntry;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum NodeVerb {
    Add {
        #[arg(long)]
        id: String,
        #[arg(long)]
        rack: String,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 22)]
        ssh_port: u16,
        /// SSH user. Empty disables SSH and uses local-fork lifecycle.
        #[arg(long, default_value = "")]
        ssh_user: String,
        #[arg(long)]
        ssh_key: Option<String>,
        #[arg(long)]
        ssh_password: Option<String>,
    },
    Remove {
        #[arg(long)]
        id: String,
    },
    List,
    /// Validate node reachability via the console: an SSH handshake for
    /// SSH-enabled nodes, or a no-op success for local-fork nodes.
    Ping {
        node: String,
    },
}

pub async fn run_node_verb(cli: &Cli, verb: NodeVerb) -> ExitCode {
    match verb {
        NodeVerb::Add {
            id,
            rack,
            host,
            ssh_port,
            ssh_user,
            ssh_key,
            ssh_password,
        } => {
            node_add(
                cli,
                NodeAddArgs {
                    id,
                    rack_id: rack,
                    host,
                    ssh_port,
                    ssh_user,
                    ssh_key,
                    ssh_password,
                },
            )
            .await
        }
        NodeVerb::Remove { id } => node_remove(cli, &id).await,
        NodeVerb::List => node_list(cli).await,
        NodeVerb::Ping { node } => node_ping(cli, &node).await,
    }
}

struct NodeAddArgs {
    id: String,
    rack_id: String,
    host: String,
    ssh_port: u16,
    ssh_user: String,
    ssh_key: Option<String>,
    ssh_password: Option<String>,
}

async fn node_add(cli: &Cli, args: NodeAddArgs) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let entry = NodeEntry {
        id: args.id.clone(),
        rack_id: args.rack_id.clone(),
        host: args.host,
        ssh_port: args.ssh_port,
        ssh_user: args.ssh_user,
        ssh_key: args.ssh_key,
        ssh_password: args.ssh_password,
    };
    match client.add_node(&args.rack_id, &entry).await {
        Ok(n) => {
            if cli.json {
                return print_json(&n);
            }
            println!("added node {} (rack={})", n.id, n.rack_id);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: add node {}: {e}", args.id);
            ExitCode::from(2)
        }
    }
}

async fn node_remove(cli: &Cli, id: &str) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.remove_node(id).await {
        Ok(()) => {
            println!("removed node {id}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: remove node {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn node_list(cli: &Cli) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let nodes = match client.list_nodes(None).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: list nodes: {e}");
            return ExitCode::from(2);
        }
    };
    if cli.json {
        return print_json(&nodes);
    }
    if nodes.is_empty() {
        println!("(no nodes)");
        return ExitCode::SUCCESS;
    }
    println!("{:<16}  {:<12}  {:<20}  SSH_USER", "ID", "RACK", "HOST");
    for n in &nodes {
        println!("{:<16}  {:<12}  {:<20}  {}", n.id, n.rack_id, n.host, n.ssh_user);
    }
    ExitCode::SUCCESS
}

async fn node_ping(cli: &Cli, node_id: &str) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.ping_node(node_id).await {
        Ok(r) if r.ok => {
            if cli.json {
                return print_json(&r);
            }
            println!("ok: node {node_id} reachable");
            ExitCode::SUCCESS
        }
        Ok(r) => {
            if cli.json {
                return print_json(&r);
            }
            eprintln!(
                "error: ping failed: {}",
                r.error.unwrap_or_else(|| "unknown".into())
            );
            ExitCode::from(2)
        }
        Err(e) => {
            eprintln!("error: ping {node_id}: {e}");
            ExitCode::from(2)
        }
    }
}
