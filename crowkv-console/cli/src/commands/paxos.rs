use clap::Subcommand;
use crowkv_console_shared::clients::console::CreateGroupBody;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum PaxosVerb {
    /// Add a Paxos group to an existing store. The console
    /// orchestrates the per-node `PxGroup` creation across `--nodes`
    /// and wires bidirectional remote-replica entries.
    Add {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        /// Base replica id; subsequent nodes get `replica_id + i`.
        #[arg(long)]
        replica_id: u64,
        /// Comma-separated node ids that should host a replica.
        #[arg(long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
    /// Remove a Paxos group from every node hosting it.
    Remove {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
    },
    /// List groups in a store (logical view).
    List {
        #[arg(long)]
        store_id: u64,
    },
    /// Inspect one group (logical view: replicas + leader).
    Inspect {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
    },
}

pub async fn run_paxos_verb(cli: &Cli, verb: PaxosVerb) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match verb {
        PaxosVerb::Add {
            store_id,
            group_id,
            replica_id,
            nodes,
        } => {
            let body = CreateGroupBody {
                group_id,
                replica_id,
                nodes: nodes.into_iter().filter(|n| !n.is_empty()).collect(),
            };
            match client.add_group(store_id, &body).await {
                Ok(v) => {
                    if cli.json {
                        return print_json(&v);
                    }
                    println!("added group {group_id} to store {store_id} on nodes {}", v["nodes"]);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add group {group_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        PaxosVerb::Remove { store_id, group_id } => match client.remove_group(store_id, group_id).await {
            Ok(()) => {
                println!("removed group {group_id} from store {store_id}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: remove group {group_id}: {e}");
                ExitCode::from(2)
            }
        },
        PaxosVerb::List { store_id } => match client.list_groups(store_id).await {
            Ok(groups) => {
                if cli.json {
                    return print_json(&groups);
                }
                if groups.is_empty() {
                    println!("(no groups)");
                    return ExitCode::SUCCESS;
                }
                println!("{:>8}  {:>8}  {:>10}", "GROUP", "LEADER", "REPLICAS");
                for g in &groups {
                    println!("{:>8}  {:>8}  {:>10}", g.group_id, g.leader.map_or_else(|| "?".into(), |l| l.to_string()), g.replica_count);
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: list groups: {e}");
                ExitCode::from(2)
            }
        },
        PaxosVerb::Inspect { store_id, group_id } => match client.get_group(store_id, group_id).await {
            Ok(view) => {
                if cli.json {
                    return print_json(&view);
                }
                println!("group {} leader={}", view.group_id, view.leader.map_or_else(|| "?".into(), |l| l.to_string()));
                println!("{:>10}  {:<12}  {:<10}  STATE", "REPLICA", "NODE", "ROLE");
                for r in &view.replicas {
                    println!("{:>10}  {:<12}  {:?}  {:?}", r.replica_id, r.node_id, r.role, r.state);
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: inspect group {group_id}: {e}");
                ExitCode::from(2)
            }
        },
    }
}
