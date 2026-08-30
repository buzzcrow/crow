// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `kv store/group/replica` command handlers — logical plane.

use std::process::ExitCode;

use clap::Subcommand;

use crate::commands::{op_context, print_json};
use crate::Cli;

// ── store ────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum StoreVerb {
    Add {
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'n', long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
    Remove {
        #[arg(short = 'I', long)]
        id: String,
    },
    List,
}

#[allow(clippy::too_many_lines)]
pub async fn run_store_verb(cli: &Cli, verb: StoreVerb) -> ExitCode {
    match verb {
        StoreVerb::Add { id, nodes } => {
            let store_id: u64 = match id.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let node_ids: Vec<u64> = match nodes.iter().map(|s| s.parse::<u64>()).collect::<Result<_, _>>() {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::add_store(&ctx, store_id, &node_ids).await {
                Ok(hosting) => {
                    if cli.json {
                        return print_json(cli, &hosting);
                    }
                    println!(
                        "added store {store_id} on nodes: {}",
                        hosting
                            .iter()
                            .map(std::string::ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add store {store_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        StoreVerb::Remove { id } => {
            let store_id: u64 = match id.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::remove_store(&ctx, store_id).await {
                Ok(()) => {
                    if !cli.json {
                        println!("removed store {store_id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove store {store_id}: {e}");
                    ExitCode::from(2)
                }
            }
        }
        StoreVerb::List => {
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::list_stores(&ctx).await {
                Ok(stores) => {
                    if cli.json {
                        return print_json(cli, &stores);
                    }
                    if stores.is_empty() {
                        println!("(no stores)");
                    } else {
                        println!("{:<12}  NODES", "STORE");
                        for s in &stores {
                            println!(
                                "{:<12}  {}",
                                s.store_id,
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
                    eprintln!("error: list stores: {e}");
                    ExitCode::from(2)
                }
            }
        }
    }
}

// ── group ────────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum GroupVerb {
    Add {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'r', long, default_value = "1")]
        replica_id: String,
        #[arg(short = 'n', long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
    Remove {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
    },
    List {
        #[arg(short = 's', long)]
        store: String,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_group_verb(cli: &Cli, verb: GroupVerb) -> ExitCode {
    match verb {
        GroupVerb::Add {
            store,
            group,
            replica_id,
            nodes,
        } => {
            let store_id: u64 = match store.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let group_id: u64 = match group.parse() {
                Ok(g) => g,
                Err(e) => {
                    eprintln!("error: invalid group id: {e}");
                    return ExitCode::from(1);
                }
            };
            let replica_id: u64 = match replica_id.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid replica id: {e}");
                    return ExitCode::from(1);
                }
            };
            let node_ids: Vec<u64> = match nodes.iter().map(|s| s.parse::<u64>()).collect::<Result<_, _>>() {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::add_group(
                &ctx, store_id, group_id, replica_id, &node_ids,
            )
            .await
            {
                Ok(()) => {
                    if !cli.json {
                        println!("added group {group_id} in store {store_id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add group: {e}");
                    ExitCode::from(2)
                }
            }
        }
        GroupVerb::Remove { store, group } => {
            let store_id: u64 = match store.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let group_id: u64 = match group.parse() {
                Ok(g) => g,
                Err(e) => {
                    eprintln!("error: invalid group id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::remove_group(&ctx, store_id, group_id).await {
                Ok(()) => {
                    if !cli.json {
                        println!("removed group {group_id} in store {store_id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove group: {e}");
                    ExitCode::from(2)
                }
            }
        }
        GroupVerb::List { store } => {
            let store_id: u64 = match store.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::list_groups(&ctx, store_id).await {
                Ok(groups) => {
                    if cli.json {
                        return print_json(cli, &groups);
                    }
                    if groups.is_empty() {
                        println!("(no groups in store {store_id})");
                    } else {
                        println!("{:<12}", "GROUP");
                        for g in &groups {
                            println!("{}", g.group_id);
                        }
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: list groups: {e}");
                    ExitCode::from(2)
                }
            }
        }
    }
}

// ── replica ──────────────────────────────────────────────────────

#[derive(Subcommand, Debug)]
pub enum ReplicaVerb {
    Add {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'r', long)]
        replica_id: Option<String>,
    },
    Remove {
        #[arg(short = 's', long)]
        store: String,
        #[arg(short = 'g', long)]
        group: String,
        #[arg(short = 'r', long)]
        replica_id: String,
    },
}

#[allow(clippy::too_many_lines)]
pub async fn run_replica_verb(cli: &Cli, verb: ReplicaVerb) -> ExitCode {
    match verb {
        ReplicaVerb::Add {
            store,
            group,
            node,
            replica_id,
        } => {
            let store_id: u64 = match store.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let group_id: u64 = match group.parse() {
                Ok(g) => g,
                Err(e) => {
                    eprintln!("error: invalid group id: {e}");
                    return ExitCode::from(1);
                }
            };
            let node_id: u64 = match node.parse() {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("error: invalid node id: {e}");
                    return ExitCode::from(1);
                }
            };
            let rid: Option<u64> = match replica_id {
                Some(r) => match r.parse() {
                    Ok(rid) => Some(rid),
                    Err(e) => {
                        eprintln!("error: invalid replica id: {e}");
                        return ExitCode::from(1);
                    }
                },
                None => None,
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::add_replica(&ctx, store_id, group_id, node_id, rid)
                .await
            {
                Ok(new_rid) => {
                    if cli.json {
                        return print_json(cli, &serde_json::json!({"replica_id": new_rid}));
                    }
                    println!("added replica {new_rid} to group {group_id} in store {store_id}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add replica: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ReplicaVerb::Remove {
            store,
            group,
            replica_id,
        } => {
            let store_id: u64 = match store.parse() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: invalid store id: {e}");
                    return ExitCode::from(1);
                }
            };
            let group_id: u64 = match group.parse() {
                Ok(g) => g,
                Err(e) => {
                    eprintln!("error: invalid group id: {e}");
                    return ExitCode::from(1);
                }
            };
            let rid: u64 = match replica_id.parse() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: invalid replica id: {e}");
                    return ExitCode::from(1);
                }
            };
            let ctx = match op_context(cli) {
                Ok(c) => c,
                Err(c) => return c,
            };
            match crowdb_console_shared::ops::kv_logical::remove_replica(&ctx, store_id, group_id, rid).await
            {
                Ok(()) => {
                    if !cli.json {
                        println!("removed replica {rid} from group {group_id} in store {store_id}");
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: remove replica: {e}");
                    ExitCode::from(2)
                }
            }
        }
    }
}
