// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use crow_console_shared::clients::console::{ConsoleClient, ServerSummary};
use crow_console_shared::cluster::{GroupView, NodeHealth, StoreView};
use crow_console_shared::config::NodeEntry;
use crow_protocol::NodeId;
use std::process::ExitCode;

use crate::utils::{client::console_client, print_json};
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ClusterVerb {
    /// High-level health summary (servers + store/group counts).
    Status,
    /// Print the full hierarchy (logical stores/groups/replicas plus
    /// physical nodes/servers), built entirely from console reads.
    Topology,
    /// Detailed view of one entity, addressed by id:
    ///   `s<sid>`               → store
    ///   `s<sid>/g<gid>`        → group
    ///   `s<sid>/g<gid>/r<rid>` → replica
    ///   `<node-id>`            → node (any token not matching the above)
    Inspect { id: String },
    /// Initialize the cluster: create the system group (store 0, group
    /// 0) on the given nodes and finalize topology cutover. Must be
    /// called before creating non-zero stores/groups.
    Init {
        /// Comma-separated node ids to bootstrap (e.g. `n1,n2,n3`).
        #[arg(long, value_delimiter = ',')]
        nodes: Vec<String>,
    },
}

pub async fn run_cluster_status(cli: &Cli) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let servers = match client.list_servers().await {
        Ok(v) => v,
        Err(e) => return fail("list servers", &e),
    };
    let stores = match client.list_stores().await {
        Ok(v) => v,
        Err(e) => return fail("list stores", &e),
    };
    if cli.json {
        return print_json(&serde_json::json!({ "servers": servers, "stores": stores }));
    }
    print_status_human(&servers, &stores);
    ExitCode::SUCCESS
}

pub async fn run_cluster_topology(cli: &Cli) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let servers = match client.list_servers().await {
        Ok(v) => v,
        Err(e) => return fail("list servers", &e),
    };
    let nodes = match client.list_nodes(None).await {
        Ok(v) => v,
        Err(e) => return fail("list nodes", &e),
    };
    let stores = match client.list_stores().await {
        Ok(v) => v,
        Err(e) => return fail("list stores", &e),
    };
    // Inflate each group to its replica list (the store view only carries
    // group summaries). One read per group; topology is not hot-path.
    let mut groups: Vec<GroupView> = Vec::new();
    for s in &stores {
        for g in &s.groups {
            match client.get_group(s.store_id, g.group_id).await {
                Ok(v) => groups.push(v),
                Err(e) => eprintln!("warning: get group {}/{}: {e}", s.store_id, g.group_id),
            }
        }
    }
    if cli.json {
        return print_json(&serde_json::json!({
            "servers": servers,
            "nodes": nodes,
            "stores": stores,
            "groups": groups,
        }));
    }
    print_topology_human(&servers, &nodes, &stores, &groups);
    ExitCode::SUCCESS
}

pub async fn run_cluster_init(cli: &Cli, nodes: &[String]) -> ExitCode {
    if nodes.is_empty() {
        eprintln!("error: cluster init requires at least one node (--nodes 1,2,...)");
        return ExitCode::from(1);
    }
    let node_ids: Vec<NodeId> = match nodes
        .iter()
        .map(|n| n.parse::<NodeId>())
        .collect::<Result<_, _>>()
    {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("error: invalid node id: {e}");
            return ExitCode::from(1);
        }
    };
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match client.cluster_init(&node_ids).await {
        Ok(v) => {
            if cli.json {
                print_json(&v)
            } else {
                println!("cluster initialized on nodes: {}", nodes.join(", "));
                ExitCode::SUCCESS
            }
        }
        Err(e) => fail("cluster init", &e),
    }
}

pub async fn run_cluster_inspect(cli: &Cli, id: &str) -> ExitCode {
    let client = match console_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match parse_inspect_id(id) {
        Ok(InspectTarget::Store(sid)) => match client.get_store(sid).await {
            Ok(v) => render(cli, &v, || print_store(&v)),
            Err(e) => fail(&format!("inspect store {sid}"), &e),
        },
        Ok(InspectTarget::Group(sid, gid)) => match client.get_group(sid, gid).await {
            Ok(v) => render(cli, &v, || print_group(&v)),
            Err(e) => fail(&format!("inspect group {sid}/{gid}"), &e),
        },
        Ok(InspectTarget::Replica(sid, gid, rid)) => match client.get_replica(sid, gid, rid).await {
            Ok(v) => render(cli, &v, || {
                println!(
                    "replica {} node={} role={:?} state={:?}",
                    v.replica_id, v.node_id, v.role, v.state
                );
            }),
            Err(e) => fail(&format!("inspect replica {sid}/{gid}/{rid}"), &e),
        },
        Ok(InspectTarget::Node(node)) => inspect_node(cli, &client, node).await,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::from(1)
        }
    }
}

async fn inspect_node(cli: &Cli, client: &ConsoleClient, node: NodeId) -> ExitCode {
    match client.get_node_server(node).await {
        Ok(entry) => render(cli, &entry, || {
            println!("node {node}");
            println!("  mgmt_url: {}", entry.url);
            println!("  grpc_url: {}", entry.grpc_url.as_deref().unwrap_or("-"));
            println!(
                "  pid:      {}",
                entry.pid.map_or_else(|| "-".to_string(), |p| p.to_string())
            );
        }),
        Err(e) => {
            // No server deployed is the common, non-fatal case for a node id.
            eprintln!("error: inspect node {node}: {e}");
            ExitCode::from(2)
        }
    }
}

/// Render a value as JSON (when `--json`) or via the provided human
/// printer, returning success.
fn render<T: serde::Serialize>(cli: &Cli, v: &T, human: impl FnOnce()) -> ExitCode {
    if cli.json {
        return print_json(v);
    }
    human();
    ExitCode::SUCCESS
}

fn fail(what: &str, e: &crow_console_shared::error::Error) -> ExitCode {
    eprintln!("error: {what}: {e}");
    ExitCode::from(2)
}

// ── id grammar ──────────────────────────────────────────────────────

enum InspectTarget {
    Node(NodeId),
    Store(u64),
    Group(u64, u64),
    Replica(u64, u64, u64),
}

/// Parse a `cluster inspect` id. Logical ids use prefixed decimal
/// segments (`s<sid>[/g<gid>[/r<rid>]]`); any token that is not a valid
/// `s<digits>...` path is treated as a node id.
fn parse_inspect_id(id: &str) -> Result<InspectTarget, String> {
    let segs: Vec<&str> = id.split('/').collect();
    let Some(sid) = segs[0].strip_prefix('s').and_then(|d| d.parse::<u64>().ok()) else {
        // Not a logical path: a single bare token is a node id.
        if segs.len() == 1 && !id.is_empty() {
            let nid = id
                .parse::<NodeId>()
                .map_err(|_| format!("invalid node id {id:?} (expected a number)"))?;
            return Ok(InspectTarget::Node(nid));
        }
        return Err(format!(
            "unrecognised id {id:?} (expected s<sid>[/g<gid>[/r<rid>]] or a node id)"
        ));
    };
    match segs.as_slice() {
        [_] => Ok(InspectTarget::Store(sid)),
        [_, g] => parse_seg(g, 'g')
            .map(|gid| InspectTarget::Group(sid, gid))
            .ok_or_else(|| format!("expected g<group_id>, got {g:?}")),
        [_, g, r] => {
            let gid = parse_seg(g, 'g').ok_or_else(|| format!("expected g<group_id>, got {g:?}"))?;
            let rid = parse_seg(r, 'r').ok_or_else(|| format!("expected r<replica_id>, got {r:?}"))?;
            Ok(InspectTarget::Replica(sid, gid, rid))
        }
        _ => Err(format!("too many path segments in id {id:?}")),
    }
}

fn parse_seg(seg: &str, prefix: char) -> Option<u64> {
    seg.strip_prefix(prefix).and_then(|d| d.parse::<u64>().ok())
}

// ── human rendering ─────────────────────────────────────────────────

fn health_str(h: NodeHealth) -> &'static str {
    match h {
        NodeHealth::Up => "up",
        NodeHealth::Down => "down",
        NodeHealth::Unknown => "unknown",
    }
}

fn print_status_human(servers: &[ServerSummary], stores: &[StoreView]) {
    let up = servers
        .iter()
        .filter(|s| matches!(s.health, NodeHealth::Up))
        .count();
    println!("servers: {} ({up} up)", servers.len());
    for s in servers {
        println!(
            "  {:<12} {:<24} health={} pid={}",
            s.node_id.map_or_else(|| "-".to_string(), |n| n.to_string()),
            s.mgmt_url,
            health_str(s.health),
            s.pid.map_or_else(|| "-".to_string(), |p| p.to_string()),
        );
    }
    let groups: usize = stores.iter().map(|s| s.groups.len()).sum();
    println!("stores: {}  groups: {groups}", stores.len());
}

fn print_topology_human(
    servers: &[ServerSummary],
    nodes: &[NodeEntry],
    stores: &[StoreView],
    groups: &[GroupView],
) {
    println!("logical:");
    for s in stores {
        println!("  store {}  nodes={:?}", s.store_id, s.nodes);
        for gsum in &s.groups {
            let view = groups
                .iter()
                .find(|g| g.store_id == s.store_id && g.group_id == gsum.group_id);
            let leader = view
                .and_then(GroupView::leader_id)
                .map_or_else(|| "?".to_string(), |l| l.to_string());
            println!("    group {}  leader={leader}", gsum.group_id);
            if let Some(view) = view {
                for r in &view.replicas {
                    println!(
                        "      replica {}  node={}  role={:?}  state={:?}",
                        r.replica_id, r.node_id, r.role, r.state
                    );
                }
            }
        }
    }

    println!("physical:");
    for n in nodes {
        let server = servers.iter().find(|s| s.node_id == Some(n.id));
        let server_label = server.map_or_else(
            || "none".to_string(),
            |s| format!("{} ({})", s.mgmt_url, health_str(s.health)),
        );
        println!(
            "  node {:<12} rack={:<10} host={:<16} server={server_label}",
            n.id, n.rack_id, n.host
        );
    }
}

fn print_store(v: &StoreView) {
    println!("store {}  nodes={:?}", v.store_id, v.nodes);
    println!("{:>10}  {:>10}  {:>10}", "GROUP", "LEADER", "REPLICAS");
    for g in &v.groups {
        println!(
            "{:>10}  {:>10}  {:>10}",
            g.group_id,
            g.leader.map_or_else(|| "?".to_string(), |l| l.to_string()),
            g.replica_count
        );
    }
}

fn print_group(v: &GroupView) {
    println!(
        "group {} (store {})  leader={}  state={:?}",
        v.group_id,
        v.store_id,
        v.leader_id().map_or_else(|| "?".to_string(), |l| l.to_string()),
        v.state
    );
    println!("{:>10}  {:<12}  {:<10}  STATE", "REPLICA", "NODE", "ROLE");
    for r in &v.replicas {
        println!(
            "{:>10}  {:<12}  {:?}  {:?}",
            r.replica_id, r.node_id, r.role, r.state
        );
    }
}
