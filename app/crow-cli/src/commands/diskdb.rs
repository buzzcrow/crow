// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crow_console_shared::diskdb::DeployDiskdbBody;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum DiskdbVerb {
    /// List all diskdb instances from the service registry.
    Instances,
    /// Query capacity usage (cluster-wide or per disk-group).
    Usage {
        #[arg(long)]
        dg: Option<u64>,
        #[arg(long)]
        disk: Option<String>,
        #[arg(long)]
        zone: Option<u32>,
    },
    /// Get scan status.
    ScanStatus {
        #[arg(long)]
        dg: Option<u64>,
    },
    /// Trigger a scan.
    Scan {
        #[arg(long)]
        dg: Option<u64>,
    },
    /// Recalculate disk usage.
    Recalc {
        #[arg(long)]
        dg: Option<u64>,
    },
    /// Compact zones on a disk.
    Compact {
        #[arg(long)]
        disk: String,
        #[arg(long, value_delimiter = ',')]
        zones: Option<Vec<u32>>,
    },
    /// Rebuild a zone bitmap.
    Rebuild {
        #[arg(long)]
        disk: String,
        #[arg(long)]
        zone: Option<u32>,
    },
    /// Set a disk's hardware status.
    SetStatus {
        #[arg(long)]
        disk: String,
        #[arg(long)]
        status: String,
    },
    /// Deploy a diskdb instance on a node.
    Deploy {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        rest_port: u16,
        #[arg(long)]
        rpc_port: u16,
        #[arg(long)]
        binary: Option<String>,
        #[arg(long)]
        listen_addr: Option<String>,
        #[arg(long)]
        http_addr: Option<String>,
        #[arg(long)]
        config: Option<String>,
    },
    /// Restart a diskdb instance on a node.
    Restart {
        #[arg(long)]
        node: u64,
    },
    /// Stop a diskdb instance on a node.
    Stop {
        #[arg(long)]
        node: u64,
    },
    /// Stop (best-effort) and remove a diskdb instance's `ServerEntry`.
    /// Use when `stop` fails with no tracked PID (console restarted).
    Delete {
        #[arg(long)]
        node: u64,
    },
}

pub async fn run_diskdb_verb(cli: &Cli, verb: DiskdbVerb) -> ExitCode {
    match verb {
        DiskdbVerb::Instances => diskdb_instances(cli).await,
        DiskdbVerb::Usage { dg, disk, zone } => diskdb_usage(cli, dg, disk, zone).await,
        DiskdbVerb::ScanStatus { dg } => diskdb_scan_status(cli, dg).await,
        DiskdbVerb::Scan { dg } => diskdb_scan(cli, dg).await,
        DiskdbVerb::Recalc { dg } => diskdb_recalc(cli, dg).await,
        DiskdbVerb::Compact { disk, zones } => diskdb_compact(cli, &disk, zones).await,
        DiskdbVerb::Rebuild { disk, zone } => diskdb_rebuild(cli, &disk, zone).await,
        DiskdbVerb::SetStatus { disk, status } => diskdb_set_status(cli, &disk, &status).await,
        DiskdbVerb::Deploy {
            node,
            rest_port,
            rpc_port,
            binary,
            listen_addr,
            http_addr,
            config,
        } => {
            diskdb_deploy(
                cli,
                node,
                rest_port,
                rpc_port,
                binary,
                listen_addr,
                http_addr,
                config,
            )
            .await
        }
        DiskdbVerb::Restart { node } => diskdb_restart(cli, node).await,
        DiskdbVerb::Stop { node } => diskdb_stop(cli, node).await,
        DiskdbVerb::Delete { node } => diskdb_delete(cli, node).await,
    }
}

async fn diskdb_instances(cli: &Cli) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.list_diskdb_instances().await {
        Ok(instances) => {
            if cli.json {
                return print_json(&instances);
            }
            if instances.is_empty() {
                println!("(no diskdb instances)");
                return ExitCode::SUCCESS;
            }
            println!(
                "{:<12}  {:<30}  {:<16}  DG_IDS",
                "INSTANCE_ID", "GRPC_ENDPOINT", "LAST_HEARTBEAT_MS"
            );
            for i in &instances {
                println!(
                    "{:<12}  {:<30}  {:<16}  {:?}",
                    i.instance_id, i.grpc_endpoint, i.last_heartbeat_ms, i.owned_dg_ids
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: list diskdb instances: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_usage(cli: &Cli, dg: Option<u64>, disk: Option<String>, zone: Option<u32>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.diskdb_usage(dg, disk.as_deref(), zone).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!(
                "{:<12}  {:<8}  {:<16}  {:<16}  {:<16}",
                "DG_ID", "NODE", "CAPACITY_BYTES", "BUSY_BYTES", "FREE_BYTES"
            );
            for g in &resp.disk_groups {
                println!(
                    "{:<12}  {:<8}  {:<16}  {:<16}  {:<16}",
                    g.disk_group_id, g.node_id, g.capacity_bytes, g.busy_bytes, g.free_bytes
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: diskdb usage: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_scan_status(cli: &Cli, dg: Option<u64>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.diskdb_scan_status(dg).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!("has_run: {}", resp.has_run);
            println!("scan_in_progress: {}", resp.scan_in_progress);
            if let Some(s) = &resp.summary {
                println!("  zones_scanned: {}", s.zones_scanned);
                println!("  ghost_busy: {}", s.ghost_busy);
                println!("  ghost_free: {}", s.ghost_free);
                println!("  leak_status: {}", s.leak_status);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: diskdb scan-status: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_scan(cli: &Cli, dg: Option<u64>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.diskdb_trigger_scan(dg).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!("scan triggered; in_progress={}", resp.scan_in_progress);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: diskdb scan: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_recalc(cli: &Cli, dg: Option<u64>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.diskdb_recalc(dg).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            for g in &resp.results {
                println!("dg {}: drift_detected={}", g.disk_group_id, g.drift_detected);
                for z in &g.zones {
                    println!(
                        "  disk {} zone {}: matches={} drift={}",
                        z.disk_id, z.zone_index, z.matches, z.drift_detected
                    );
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: diskdb recalc: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_compact(cli: &Cli, disk: &str, zones: Option<Vec<u32>>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.diskdb_compact(disk, zones).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!(
                "compacted {} zones, {} free records deleted",
                resp.compacted_zone_count, resp.total_free_records_deleted
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: diskdb compact: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_rebuild(cli: &Cli, disk: &str, zone: Option<u32>) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.diskdb_rebuild(disk, zone).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!(
                "rebuilt {} zones: busy={}, free={}",
                resp.rebuilt_zone_count, resp.total_busy_units, resp.total_free_units
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: diskdb rebuild: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_set_status(cli: &Cli, disk: &str, status: &str) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.set_disk_status(disk, status).await {
        Ok(()) => {
            if cli.json {
                println!("{{\"ok\":true}}");
            } else {
                println!("set disk {disk} status → {status}");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: set disk status: {e}");
            ExitCode::from(2)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn diskdb_deploy(
    cli: &Cli,
    node: u64,
    rest_port: u16,
    rpc_port: u16,
    binary: Option<String>,
    listen_addr: Option<String>,
    http_addr: Option<String>,
    config: Option<String>,
) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let body = DeployDiskdbBody {
        rest_port,
        rpc_port,
        binary,
        listen_addr,
        http_addr,
        config,
    };
    match client.deploy_diskdb(node, &body).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!(
                "deployed diskdb on node {}: pid={}, mgmt={}, grpc={}",
                resp.node_id, resp.pid, resp.mgmt_url, resp.grpc_url
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: deploy diskdb: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_restart(cli: &Cli, node: u64) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.restart_diskdb(node).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!(
                "restarted diskdb on node {}: pid={}, mgmt={}",
                resp.node_id, resp.pid, resp.mgmt_url
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: restart diskdb: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_stop(cli: &Cli, node: u64) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.stop_diskdb(node).await {
        Ok(resp) => {
            if cli.json {
                return print_json(&resp);
            }
            println!("stopped diskdb on node {node}: sent={}", resp.sent);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: stop diskdb: {e}");
            ExitCode::from(2)
        }
    }
}

async fn diskdb_delete(cli: &Cli, node: u64) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.delete_diskdb(node).await {
        Ok(()) => {
            if cli.json {
                println!("{{\"ok\":true}}");
            } else {
                println!("deleted diskdb entry on node {node}");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: delete diskdb: {e}");
            ExitCode::from(2)
        }
    }
}
