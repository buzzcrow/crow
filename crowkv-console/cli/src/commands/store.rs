use clap::Subcommand;
use crowkv_console_shared::clients::console::CreateStoreBody;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum StoreVerb {
    /// Create a new logical store across the listed nodes. The console
    /// orchestrates per-node creation and rolls back on partial failure.
    Add {
        #[arg(long)]
        store_id: u64,
        /// Comma-separated node ids that should host the store. If
        /// empty, the console picks the first node with a running
        /// `crowkv-server`.
        #[arg(long, value_delimiter = ',', default_value = "")]
        nodes: Vec<String>,
    },
    /// Remove a store from every node hosting it.
    Remove {
        #[arg(long)]
        store_id: u64,
    },
    /// List logical stores aggregated from the monitor cache.
    List,
    /// Print detailed info for one logical store.
    Inspect {
        #[arg(long)]
        store_id: u64,
    },
}

pub async fn run_store_verb(cli: &Cli, verb: StoreVerb) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match verb {
        StoreVerb::Add { store_id, nodes } => {
            let body = CreateStoreBody {
                store_id,
                nodes: nodes.into_iter().filter(|n| !n.is_empty()).collect(),
            };
            match client.add_store(&body).await {
                Ok(v) => {
                    if cli.json {
                        return print_json(&v);
                    }
                    println!("added store {store_id} on nodes {}", v["nodes"]);
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add store {store_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        StoreVerb::Remove { store_id } => match client.remove_store(store_id).await {
            Ok(()) => {
                println!("removed store {store_id}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: remove store {store_id}: {e}");
                ExitCode::from(2)
            }
        },
        StoreVerb::List => match client.list_stores().await {
            Ok(stores) => {
                if cli.json {
                    return print_json(&stores);
                }
                if stores.is_empty() {
                    println!("(no stores)");
                    return ExitCode::SUCCESS;
                }
                println!("{:>8}  {:>8}  NODES", "STORE", "GROUPS");
                for s in &stores {
                    println!("{:>8}  {:>8}  {}", s.store_id, s.groups.len(), s.nodes.join(","));
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: list stores: {e}");
                ExitCode::from(2)
            }
        },
        StoreVerb::Inspect { store_id } => match client.get_store(store_id).await {
            Ok(detail) => {
                if cli.json {
                    return print_json(&detail);
                }
                println!("store {} nodes=[{}]", detail.store_id, detail.nodes.join(","));
                for g in &detail.groups {
                    println!(
                        "  group {} replicas={} leader={}",
                        g.group_id,
                        g.replica_count,
                        g.leader.map_or_else(|| "?".into(), |l| l.to_string())
                    );
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: inspect store {store_id}: {e}");
                ExitCode::from(2)
            }
        },
    }
}
