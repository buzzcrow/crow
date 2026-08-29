// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crowdb_console_shared::clients::console::AddDiskGroupBody;
use crowdb_protocol::NodeId;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum DiskGroupVerb {
    Add {
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'I', long)]
        id: String,
        #[arg(short = 'N', long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(short = 'n', long)]
        node: String,
        #[arg(short = 'I', long)]
        id: String,
    },
    List {
        #[arg(short = 'n', long)]
        node: String,
    },
}

pub async fn run_disk_group_verb(cli: &Cli, verb: DiskGroupVerb) -> ExitCode {
    match verb {
        DiskGroupVerb::Add { node, id, name } => disk_group_add(cli, &node, &id, &name).await,
        DiskGroupVerb::Remove { node, id } => disk_group_remove(cli, &node, &id).await,
        DiskGroupVerb::List { node } => disk_group_list(cli, &node).await,
    }
}

async fn disk_group_add(cli: &Cli, node: &str, id: &str, name: &str) -> ExitCode {
    let node_id: NodeId = match node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let dg_id: u64 = match id.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid disk-group id {id:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .add_disk_group(
            node_id,
            &AddDiskGroupBody {
                id: dg_id,
                name: name.to_string(),
            },
        )
        .await
    {
        Ok(dg) => {
            if cli.json {
                return print_json(&dg);
            }
            println!("added disk-group {} on node {}", dg.id, dg.node_id);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: add disk-group {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn disk_group_remove(cli: &Cli, node: &str, id: &str) -> ExitCode {
    let node_id: NodeId = match node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let dg_id: u64 = match id.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid disk-group id {id:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.remove_disk_group(node_id, dg_id).await {
        Ok(()) => {
            println!("removed disk-group {id} on node {node}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: remove disk-group {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn disk_group_list(cli: &Cli, node: &str) -> ExitCode {
    let node_id: NodeId = match node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let dgs = match client.list_disk_groups(node_id).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: list disk-groups: {e}");
            return ExitCode::from(2);
        }
    };
    if cli.json {
        return print_json(&dgs);
    }
    if dgs.is_empty() {
        println!("(no disk-groups)");
        return ExitCode::SUCCESS;
    }
    println!("{:<16}  {:<8}  {:<8}  NAME", "ID", "RACK", "NODE");
    for dg in &dgs {
        println!("{:<16}  {:<8}  {:<8}  {}", dg.id, dg.rack_id, dg.node_id, dg.name);
    }
    ExitCode::SUCCESS
}
