// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crowkv_console_shared::clients::console::AddReplicaBody;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ReplicaVerb {
    /// Add a replica to a group on a target node. The console
    /// orchestrates the bidirectional remote wiring (and rolls back
    /// on partial failure).
    Add {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        /// Target node id. The console looks up the node's gRPC URL
        /// from its config; the operator never sees `host:port`.
        #[arg(long)]
        node: String,
        /// Optional explicit replica id; defaults to `max + 1`.
        #[arg(long)]
        replica_id: Option<u64>,
    },
    /// Remove a replica from its hosting node and deregister it
    /// from every peer.
    Remove {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        replica_id: u64,
    },
}

pub async fn run_replica_verb(cli: &Cli, verb: ReplicaVerb) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match verb {
        ReplicaVerb::Add {
            store_id,
            group_id,
            node,
            replica_id,
        } => {
            let body = AddReplicaBody {
                node_id: node,
                replica_id,
            };
            match client.add_replica(store_id, group_id, &body).await {
                Ok(v) => {
                    if cli.json {
                        return print_json(&v);
                    }
                    println!(
                        "added replica {} on node {} (store {}, group {})",
                        v["replica_id"], v["node_id"], store_id, group_id
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add replica: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ReplicaVerb::Remove {
            store_id,
            group_id,
            replica_id,
        } => match client.remove_replica(store_id, group_id, replica_id).await {
            Ok(()) => {
                println!("removed replica {replica_id} from group {group_id} (store {store_id})");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: remove replica: {e}");
                ExitCode::from(2)
            }
        },
    }
}
