use clap::Subcommand;
use crowkv_console_shared::snapshot::{ClusterSnapshot, ServerSnapshot};
use std::process::ExitCode;

use crate::utils::{fetch_snapshot, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ClusterVerb {
    /// High-level health summary.
    Status,
    /// Print the full hierarchy.
    Topology,
    /// Detailed view of one node/store/group/replica.
    Inspect { id: String },
}

pub async fn run_cluster_status(cli: &Cli) -> ExitCode {
    let snap = match fetch_snapshot(cli).await {
        Ok(s) => s,
        Err(c) => return c,
    };
    if cli.json {
        return print_json(&snap);
    }
    print_status_human(&snap);
    ExitCode::SUCCESS
}

pub async fn run_cluster_topology(cli: &Cli) -> ExitCode {
    let snap = match fetch_snapshot(cli).await {
        Ok(s) => s,
        Err(c) => return c,
    };
    if cli.json {
        return print_json(&snap);
    }
    print_topology_human(&snap);
    ExitCode::SUCCESS
}

fn print_status_human(snap: &ClusterSnapshot) {
    for s in &snap.servers {
        let label = server_label(s);
        println!("server {} -> {label}", s.mgmt_url);
        if let Some(err) = &s.error {
            println!("  error: {err}");
        }
    }
}

fn print_topology_human(snap: &ClusterSnapshot) {
    for s in &snap.servers {
        println!("server {} ({})", s.mgmt_url, server_label(s));
        if let Some(err) = &s.error {
            println!("  error: {err}");
            continue;
        }
        for store in &s.stores {
            println!("  store {} listen={}", store.store_id, store.listen_addr.as_deref().unwrap_or("-"));
            for g in &store.groups {
                println!("    group {} leader={} local={} role={}", g.group_id, g.leader_id, g.local_replica.id, g.local_replica.role);
                for r in &g.remotes {
                    println!("      remote {} {}", r.id, r.endpoint);
                }
            }
        }
    }
}

fn server_label(s: &ServerSnapshot) -> &str {
    s.health.as_ref().map_or("unreachable", |h| h.status.as_str())
}
