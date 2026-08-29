// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crowdb_console_shared::clients::console::{AddDiskBody, MoveDiskBody};
use crowdb_protocol::NodeId;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum DiskVerb {
    Add {
        #[arg(long)]
        node: String,
        #[arg(long)]
        group: String,
        #[arg(long)]
        id: String,
        #[arg(long, default_value = "Hdd")]
        disk_type: String,
        #[arg(long)]
        capacity_bytes: u64,
        #[arg(long)]
        zone_size_bytes: u64,
        #[arg(long, default_value = "4096")]
        unit_size_bytes: u32,
        #[arg(long, default_value = "")]
        device_path: String,
    },
    Remove {
        #[arg(long)]
        node: String,
        #[arg(long)]
        group: String,
        #[arg(long)]
        id: String,
    },
    List {
        #[arg(long)]
        node: String,
        #[arg(long)]
        group: String,
    },
    Move {
        #[arg(long)]
        id: String,
        #[arg(long)]
        new_rack: String,
        #[arg(long)]
        new_node: String,
        #[arg(long)]
        new_group: String,
    },
}

pub async fn run_disk_verb(cli: &Cli, verb: DiskVerb) -> ExitCode {
    match verb {
        DiskVerb::Add {
            node,
            group,
            id,
            disk_type,
            capacity_bytes,
            zone_size_bytes,
            unit_size_bytes,
            device_path,
        } => {
            disk_add(
                cli,
                &node,
                &group,
                &id,
                &disk_type,
                capacity_bytes,
                zone_size_bytes,
                unit_size_bytes,
                &device_path,
            )
            .await
        }
        DiskVerb::Remove { node, group, id } => disk_remove(cli, &node, &group, &id).await,
        DiskVerb::List { node, group } => disk_list(cli, &node, &group).await,
        DiskVerb::Move {
            id,
            new_rack,
            new_node,
            new_group,
        } => disk_move(cli, &id, &new_rack, &new_node, &new_group).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn disk_add(
    cli: &Cli,
    node: &str,
    group: &str,
    id: &str,
    disk_type: &str,
    capacity_bytes: u64,
    zone_size_bytes: u64,
    unit_size_bytes: u32,
    device_path: &str,
) -> ExitCode {
    let node_id: NodeId = match node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let dg_id: u64 = match group.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid disk-group id {group:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .add_disk(
            node_id,
            dg_id,
            &AddDiskBody {
                disk_id: id.to_string(),
                disk_type: disk_type.to_string(),
                capacity_bytes,
                zone_size_bytes,
                unit_size_bytes,
                device_path: device_path.to_string(),
            },
        )
        .await
    {
        Ok(d) => {
            if cli.json {
                return print_json(&d);
            }
            println!(
                "added disk {} to disk-group {} on node {}",
                d.disk_id, d.disk_group_id, d.node_id
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: add disk {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn disk_remove(cli: &Cli, node: &str, group: &str, id: &str) -> ExitCode {
    let node_id: NodeId = match node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let dg_id: u64 = match group.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid disk-group id {group:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.remove_disk(node_id, dg_id, id).await {
        Ok(()) => {
            println!("removed disk {id}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: remove disk {id}: {e}");
            ExitCode::from(2)
        }
    }
}

async fn disk_list(cli: &Cli, node: &str, group: &str) -> ExitCode {
    let node_id: NodeId = match node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let dg_id: u64 = match group.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid disk-group id {group:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let disks = match client.list_disks(node_id, dg_id).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: list disks: {e}");
            return ExitCode::from(2);
        }
    };
    if cli.json {
        return print_json(&disks);
    }
    if disks.is_empty() {
        println!("(no disks)");
        return ExitCode::SUCCESS;
    }
    println!("{:<40}  {:<8}  {:<16}  CAPACITY", "DISK_ID", "TYPE", "GROUP");
    for d in &disks {
        println!(
            "{:<40}  {:<8}  {:<16}  {}",
            d.disk_id, d.disk_type, d.disk_group_id, d.capacity_bytes
        );
    }
    ExitCode::SUCCESS
}

async fn disk_move(cli: &Cli, id: &str, new_rack: &str, new_node: &str, new_group: &str) -> ExitCode {
    let new_rack_id: u64 = match new_rack.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid rack id {new_rack:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let new_node_id: NodeId = match new_node.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid node id {new_node:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let new_dg_id: u64 = match new_group.parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: invalid disk-group id {new_group:?}: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client
        .move_disk(
            id,
            &MoveDiskBody {
                new_rack_id,
                new_node_id,
                new_disk_group_id: new_dg_id,
            },
        )
        .await
    {
        Ok(d) => {
            if cli.json {
                return print_json(&d);
            }
            println!(
                "moved disk {} to disk-group {} on node {}",
                d.disk_id, d.disk_group_id, d.node_id
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: move disk {id}: {e}");
            ExitCode::from(2)
        }
    }
}
