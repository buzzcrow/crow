//! `crowkv` CLI entrypoint.
//!
//! C2 status: `cluster status/topology` resolve servers from `--server`
//! (highest priority), `CROWKV_SERVER` env var, or the persisted
//! registry (`~/.crowkv/console.toml`). `server add/remove/list` manage
//! the registry. Other verbs remain placeholders.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use crowkv_console_core::{
    clients::grpc::{GetOutcome, KvClient},
    clients::http::ServerClient,
    config::{NodeEntry, RackEntry, ServerEntry},
    lifecycle::{self, DeployRequest},
    mgmt::{AddGroupRequest, AddStoreRequest, RemoteReplicaInfo},
    topology, ClusterSnapshot, ConsoleConfig, ServerSnapshot,
};

#[derive(Parser, Debug)]
#[command(name = "crowkv", version, about = "CrowKV cluster console (CLI)")]
struct Cli {
    /// Override the default `crowkv-server` management URL. When set,
    /// the registry is bypassed.
    #[arg(long, global = true, env = "CROWKV_SERVER")]
    server: Option<String>,

    /// Path to the console config file. Defaults to
    /// `$CROWKV_CONSOLE_CONFIG` or `~/.crowkv/console.toml`.
    #[arg(long, global = true, env = "CROWKV_CONSOLE_CONFIG")]
    config: Option<PathBuf>,

    /// Emit JSON instead of human-readable output where applicable.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Group,
}

#[derive(Subcommand, Debug)]
enum Group {
    /// Cluster observation commands.
    Cluster {
        #[command(subcommand)]
        verb: ClusterVerb,
    },
    /// Simulated hardware: racks.
    Rack {
        #[command(subcommand)]
        verb: RackVerb,
    },
    /// Simulated hardware: nodes (host + ssh creds).
    Node {
        #[command(subcommand)]
        verb: NodeVerb,
    },
    /// crowkv-server lifecycle on a node.
    Server {
        #[command(subcommand)]
        verb: ServerVerb,
    },
    /// Store management within a server.
    Store {
        #[command(subcommand)]
        verb: StoreVerb,
    },
    /// Paxos group management.
    Paxos {
        #[command(subcommand)]
        verb: PaxosVerb,
    },
    /// Replica add/remove.
    Replica {
        #[command(subcommand)]
        verb: ReplicaVerb,
    },
    /// Data-plane KV operations.
    Kv {
        #[command(subcommand)]
        verb: KvVerb,
    },
    /// Load testing (CLI-only).
    Bench {
        #[command(subcommand)]
        verb: BenchVerb,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterVerb {
    /// High-level health summary.
    Status,
    /// Print the full hierarchy.
    Topology,
    /// Detailed view of one node/store/group/replica.
    Inspect { id: String },
}

#[derive(Subcommand, Debug)]
enum StoreVerb {
    /// Create a new store (and its bootstrap group + local replica).
    Add {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        replica_id: u64,
        /// gRPC port for the store's listener (0 = OS-assigned).
        #[arg(long)]
        port: Option<u16>,
    },
    /// Remove a store.
    Remove {
        #[arg(long)]
        store_id: u64,
    },
    /// List stores on the selected server.
    List,
    /// Print detailed info for one store.
    Inspect {
        #[arg(long)]
        store_id: u64,
    },
}

#[derive(Subcommand, Debug)]
enum PaxosVerb {
    /// Add a Paxos group to an existing store.
    Add {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        replica_id: u64,
    },
    /// Remove a Paxos group.
    Remove {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
    },
    /// List groups in a store.
    List {
        #[arg(long)]
        store_id: u64,
    },
    /// Inspect one group (prints its remote replicas).
    Inspect {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
    },
}

#[derive(Subcommand, Debug)]
enum ReplicaVerb {
    /// Add a remote replica to a group.
    Add {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        replica_id: u64,
        /// `host:port` of the remote `crowkv-server`'s gRPC listener.
        #[arg(long)]
        endpoint: String,
    },
    /// Remove a remote replica.
    Remove {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        replica_id: u64,
    },
}

#[derive(Subcommand, Debug)]
enum RackVerb {
    Add {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = "")]
        name: String,
    },
    Remove {
        #[arg(long)]
        id: String,
    },
    List,
}

#[derive(Subcommand, Debug)]
enum NodeVerb {
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
    /// Validate ssh + http reachability (stub until C4 SSH transport).
    Ping {
        node: String,
    },
}

#[derive(Subcommand, Debug)]
enum ServerVerb {
    /// Register a `crowkv-server` instance with the console.
    Add {
        /// Console-side identifier (must be unique).
        #[arg(long)]
        id: String,
        /// Management base URL, e.g. `http://127.0.0.1:9910`.
        #[arg(long)]
        url: String,
    },
    /// Remove a registered server by id.
    Remove {
        #[arg(long)]
        id: String,
    },
    /// List registered servers.
    List,
    /// Deploy a new `crowkv-server` on a node via local fork (C3
    /// placeholder; SSH transport lands in C4).
    Deploy {
        #[arg(long)]
        id: String,
        #[arg(long)]
        node: String,
        #[arg(long)]
        mgmt_port: u16,
        #[arg(long)]
        grpc_port: u16,
    },
    /// Start a previously-deployed server. C3: not yet supported —
    /// equivalent to `deploy` with the same id.
    Start { server_id: String },
    /// Stop a previously-deployed server (SIGTERM to recorded pid).
    Stop { server_id: String },
}

#[derive(Subcommand, Debug)]
enum KvVerb {
    /// Put a key/value. Bytes are taken as UTF-8 from the CLI; use
    /// `--value-file <path>` for binary payloads.
    Put {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long, conflicts_with = "value_file")]
        value: Option<String>,
        #[arg(long)]
        value_file: Option<PathBuf>,
        /// Optional client id for idempotency tracking. Defaults to 0.
        #[arg(long, default_value_t = 0)]
        client_id: u64,
        /// Optional client sequence for idempotency. Defaults to 0.
        #[arg(long, default_value_t = 0)]
        seq: u64,
    },
    /// Get a single key. Prints the value as UTF-8 (lossy) by default;
    /// use `--hex` to dump a hex-encoded payload.
    Get {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        hex: bool,
    },
    /// Delete a key. No-op (`not found`) is reported but not an error.
    Delete {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long)]
        key: String,
        #[arg(long, default_value_t = 0)]
        client_id: u64,
        #[arg(long, default_value_t = 0)]
        seq: u64,
    },
    /// Prefix list. C6: server-side scan is not implemented yet — this
    /// verb returns an explanatory error.
    List {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Alias for `list` with the same caveats.
    Scan {
        #[arg(long)]
        store_id: u64,
        #[arg(long)]
        group_id: u64,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
}

#[derive(Subcommand, Debug)]
enum BenchVerb {
    /// Run a workload (`read`, `write`, `list`, or `mix`).
    Run {
        /// Workload kind: `read | write | list | mix`.
        workload: String,
        /// Store id whose `listen_addr` is dialed for gRPC ops.
        #[arg(long, default_value_t = 1)]
        store_id: u64,
        #[arg(long, default_value_t = 1)]
        group_id: u64,
        /// Number of independent gRPC channels (1..=64).
        #[arg(long, default_value_t = 4)]
        connections: u32,
        /// Number of worker tasks (1..=1000).
        #[arg(long, default_value_t = 8)]
        threads: u32,
        /// Test duration in seconds.
        #[arg(long, default_value_t = 5)]
        duration_secs: u64,
        /// Distinct keys per worker key space.
        #[arg(long, default_value_t = 1_000)]
        key_space: u64,
        /// Per-op value size in bytes.
        #[arg(long, default_value_t = 64)]
        value_size: usize,
        /// Optional explicit run id; defaults to a timestamp-based one.
        #[arg(long)]
        run_id: Option<String>,
    },
    /// Run a built-in stress scenario (`burst`, `soak`, `hotread`).
    Stress {
        scenario: String,
        #[arg(long, default_value_t = 1)]
        store_id: u64,
        #[arg(long, default_value_t = 1)]
        group_id: u64,
    },
    /// Re-render a previously-saved report.
    Report { run_id: String },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");

    runtime.block_on(async move { dispatch(cli).await })
}

async fn dispatch(mut cli: Cli) -> ExitCode {
    let command = std::mem::replace(&mut cli.command, Group::Cluster { verb: ClusterVerb::Status });
    match command {
        Group::Cluster { verb } => match verb {
            ClusterVerb::Status => run_cluster_status(&cli).await,
            ClusterVerb::Topology => run_cluster_topology(&cli).await,
            ClusterVerb::Inspect { .. } => not_implemented("cluster inspect"),
        },
        Group::Rack { verb } => run_rack_verb(&cli, verb),
        Group::Node { verb } => run_node_verb(&cli, verb).await,
        Group::Server { verb } => run_server_verb(&cli, verb).await,
        Group::Store { verb } => run_store_verb(&cli, verb).await,
        Group::Paxos { verb } => run_paxos_verb(&cli, verb).await,
        Group::Replica { verb } => run_replica_verb(&cli, verb).await,
        Group::Kv { verb } => run_kv_verb(&cli, verb).await,
        Group::Bench { verb } => run_bench_verb(&cli, verb).await,
    }
}

fn not_implemented(what: &str) -> ExitCode {
    eprintln!("crowkv: '{what}' is not implemented yet (C0/C1 skeleton).");
    ExitCode::from(1)
}

fn config_path(cli: &Cli) -> Result<PathBuf, ExitCode> {
    if let Some(p) = &cli.config {
        return Ok(p.clone());
    }
    ConsoleConfig::default_path().ok_or_else(|| {
        eprintln!("error: cannot determine config path (no $HOME, no --config / $CROWKV_CONSOLE_CONFIG)");
        ExitCode::from(1)
    })
}

fn load_config(cli: &Cli) -> Result<ConsoleConfig, ExitCode> {
    let path = config_path(cli)?;
    ConsoleConfig::load(&path).map_err(|e| {
        eprintln!("error: load config {}: {e}", path.display());
        ExitCode::from(1)
    })
}

fn save_config(cli: &Cli, cfg: &ConsoleConfig) -> Result<(), ExitCode> {
    let path = config_path(cli)?;
    cfg.save(&path).map_err(|e| {
        eprintln!("error: save config {}: {e}", path.display());
        ExitCode::from(2)
    })
}

/// Resolve which servers to poll for `cluster *` commands:
/// 1. `--server <url>` / `CROWKV_SERVER` env (single URL).
/// 2. Persisted registry.
fn resolve_targets(cli: &Cli) -> Result<Vec<String>, ExitCode> {
    if let Some(url) = &cli.server {
        return Ok(vec![url.clone()]);
    }
    let cfg = load_config(cli)?;
    if cfg.servers.is_empty() {
        eprintln!("error: no servers registered. Use `crowkv server add --id <id> --url <url>` or pass --server.");
        return Err(ExitCode::from(1));
    }
    Ok(cfg.server_urls())
}

async fn fetch_snapshot(cli: &Cli) -> Result<ClusterSnapshot, ExitCode> {
    let targets = resolve_targets(cli)?;
    topology::aggregate(&targets).await.map_err(|e| {
        eprintln!("error: aggregate failed: {e}");
        ExitCode::from(2)
    })
}

async fn run_cluster_status(cli: &Cli) -> ExitCode {
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

async fn run_cluster_topology(cli: &Cli) -> ExitCode {
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

fn print_json<T: serde::Serialize>(v: &T) -> ExitCode {
    match serde_json::to_string_pretty(v) {
        Ok(s) => {
            println!("{s}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: json encode: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run_server_verb(cli: &Cli, verb: ServerVerb) -> ExitCode {
    match verb {
        ServerVerb::Add { id, url } => server_add(cli, id, url),
        ServerVerb::Remove { id } => server_remove(cli, &id),
        ServerVerb::List => server_list(cli),
        ServerVerb::Deploy { id, node, mgmt_port, grpc_port } => server_deploy(cli, id, node, mgmt_port, grpc_port).await,
        ServerVerb::Start { .. } => not_implemented("server start (C3: use 'server deploy --id <existing>' after stop)"),
        ServerVerb::Stop { server_id } => server_stop(cli, &server_id).await,
    }
}

fn run_rack_verb(cli: &Cli, verb: RackVerb) -> ExitCode {
    match verb {
        RackVerb::Add { id, name } => rack_add(cli, id, name),
        RackVerb::Remove { id } => rack_remove(cli, &id),
        RackVerb::List => rack_list(cli),
    }
}

async fn run_node_verb(cli: &Cli, verb: NodeVerb) -> ExitCode {
    match verb {
        NodeVerb::Add {
            id,
            rack,
            host,
            ssh_port,
            ssh_user,
            ssh_key,
            ssh_password,
        } => node_add(
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
        ),
        NodeVerb::Remove { id } => node_remove(cli, &id),
        NodeVerb::List => node_list(cli),
        NodeVerb::Ping { node } => node_ping(cli, &node).await,
    }
}

async fn node_ping(cli: &Cli, node_id: &str) -> ExitCode {
    let cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let Some(node) = cfg.node(node_id) else {
        eprintln!("error: unknown node {node_id}");
        return ExitCode::from(1);
    };
    if !node.ssh_enabled() {
        eprintln!("error: node {node_id} has no ssh_user — set one with `node add --ssh-user`");
        return ExitCode::from(1);
    }
    match crowkv_console_ssh::probe(node).await {
        Ok(()) => {
            println!("ok: {} ({}@{}:{}) reachable", node.id, node.ssh_user, node.host, node.ssh_port);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: ping failed: {e}");
            ExitCode::from(2)
        }
    }
}

fn rack_add(cli: &Cli, id: String, name: String) -> ExitCode {
    println!("adding rack {id}");
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if let Err(e) = cfg.add_rack(RackEntry { id, name }) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    ExitCode::SUCCESS
}

fn rack_remove(cli: &Cli, id: &str) -> ExitCode {
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if let Err(e) = cfg.remove_rack(id) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    println!("removed rack {id}");
    ExitCode::SUCCESS
}

fn rack_list(cli: &Cli) -> ExitCode {
    let cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if cli.json {
        return print_json(&cfg.racks);
    }
    if cfg.racks.is_empty() {
        println!("(no racks)");
        return ExitCode::SUCCESS;
    }
    println!("{:<16}  NAME", "ID");
    for r in &cfg.racks {
        println!("{:<16}  {}", r.id, r.name);
    }
    ExitCode::SUCCESS
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

fn node_add(cli: &Cli, args: NodeAddArgs) -> ExitCode {
    println!("adding node {}", args.id);
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if let Err(e) = cfg.add_node(NodeEntry {
        id: args.id,
        rack_id: args.rack_id,
        host: args.host,
        ssh_port: args.ssh_port,
        ssh_user: args.ssh_user,
        ssh_key: args.ssh_key,
        ssh_password: args.ssh_password,
    }) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    ExitCode::SUCCESS
}

fn node_remove(cli: &Cli, id: &str) -> ExitCode {
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if let Err(e) = cfg.remove_node(id) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    println!("removed node {id}");
    ExitCode::SUCCESS
}

fn node_list(cli: &Cli) -> ExitCode {
    let cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if cli.json {
        return print_json(&cfg.nodes);
    }
    if cfg.nodes.is_empty() {
        println!("(no nodes)");
        return ExitCode::SUCCESS;
    }
    println!("{:<16}  {:<12}  {:<20}  SSH_USER", "ID", "RACK", "HOST");
    for n in &cfg.nodes {
        println!("{:<16}  {:<12}  {:<20}  {}", n.id, n.rack_id, n.host, n.ssh_user);
    }
    ExitCode::SUCCESS
}

async fn server_deploy(cli: &Cli, id: String, node_id: String, mgmt_port: u16, grpc_port: u16) -> ExitCode {
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };

    let node = if let Some(n) = cfg.node(&node_id) {
        n.clone()
    } else {
        eprintln!("error: unknown node {node_id}");
        return ExitCode::from(1);
    };

    // C3: enforce "one server per node" on the console side.
    if cfg.servers.iter().any(|s| s.node_id.as_deref() == Some(&node_id)) {
        eprintln!("error: node {node_id} already hosts a deployed server");
        return ExitCode::from(1);
    }
    if cfg.server(&id).is_some() {
        eprintln!("error: server id {id} already exists");
        return ExitCode::from(1);
    }

    let req = DeployRequest {
        server_id: id.clone(),
        mgmt_port,
        grpc_port,
        binary: None,
    };
    let deployed = if node.ssh_enabled() {
        // SSH path requires the operator to point us at the binary on
        // the remote host. Use $CROWKV_SERVER_BIN if set, else assume
        // it's on the remote $PATH as `crowkv-server`.
        let server_bin = std::env::var("CROWKV_SERVER_BIN").unwrap_or_else(|_| "crowkv-server".to_string());
        match crowkv_console_ssh::deploy_via_ssh(&req, &node, &server_bin).await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("error: ssh deploy failed: {e}");
                return ExitCode::from(2);
            }
        }
    } else {
        match lifecycle::deploy_local(&req, &node).await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("error: local deploy failed: {e}");
                return ExitCode::from(2);
            }
        }
    };

    let entry = ServerEntry {
        id: deployed.server_id.clone(),
        url: deployed.mgmt_url.clone(),
        node_id: Some(node_id.clone()),
        grpc_url: Some(deployed.grpc_url.clone()),
        pid: Some(deployed.pid),
    };
    if let Err(e) = cfg.add_server(entry) {
        // Very unlikely since we checked above, but be defensive.
        eprintln!("warning: deploy succeeded (pid {}) but registry update failed: {e}", deployed.pid);
        return ExitCode::from(2);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }

    println!("deployed server {id} -> {} (pid {}, grpc {})", deployed.mgmt_url, deployed.pid, deployed.grpc_url);
    ExitCode::SUCCESS
}

async fn server_stop(cli: &Cli, id: &str) -> ExitCode {
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let entry = if let Some(s) = cfg.server(id) {
        s.clone()
    } else {
        eprintln!("error: unknown server {id}");
        return ExitCode::from(1);
    };
    let Some(pid) = entry.pid else {
        eprintln!("error: server {id} was not deployed by this console (no pid)");
        return ExitCode::from(1);
    };

    // SSH stop if the server's node is SSH-enabled; else local kill.
    let result = match entry.node_id.as_deref().and_then(|nid| cfg.node(nid)) {
        Some(node) if node.ssh_enabled() => crowkv_console_ssh::stop_via_ssh(node, pid).await,
        _ => lifecycle::stop_pid(pid),
    };
    match result {
        Ok(true) => println!("sent SIGTERM to pid {pid}"),
        Ok(false) => println!("pid {pid} was already gone"),
        Err(e) => {
            eprintln!("error: stop: {e}");
            return ExitCode::from(2);
        }
    }
    // Clear the server entry entirely so the node is free for redeploy.
    let _ = cfg.remove_server(id);
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    ExitCode::SUCCESS
}

fn server_add(cli: &Cli, id: String, url: String) -> ExitCode {
    println!("adding server {id} -> {url}");
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if let Err(e) = cfg.add_server(ServerEntry::new(id, url)) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    ExitCode::SUCCESS
}

fn server_remove(cli: &Cli, id: &str) -> ExitCode {
    let mut cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if let Err(e) = cfg.remove_server(id) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    if let Err(c) = save_config(cli, &cfg) {
        return c;
    }
    println!("removed server {id}");
    ExitCode::SUCCESS
}

fn server_list(cli: &Cli) -> ExitCode {
    let cfg = match load_config(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    if cli.json {
        return print_json(&cfg.servers);
    }
    if cfg.servers.is_empty() {
        println!("(no servers registered)");
        return ExitCode::SUCCESS;
    }
    println!("{:<16}  URL", "ID");
    for s in &cfg.servers {
        println!("{:<16}  {}", s.id, s.url);
    }
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

/// Resolve a single management target URL for `store / paxos / replica`
/// verbs. Priority: `--server` override (URL or registry id), then the
/// sole entry in the registry. Errors if the registry is empty or has
/// multiple entries and `--server` wasn't supplied.
fn resolve_single_target(cli: &Cli) -> Result<String, ExitCode> {
    if let Some(raw) = &cli.server {
        // Allow either a bare URL or a registry id.
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(raw.clone());
        }
        let cfg = load_config(cli)?;
        return cfg.server(raw).map(|s| s.url.clone()).ok_or_else(|| {
            eprintln!("error: --server {raw}: not a URL and no registry entry with that id");
            ExitCode::from(1)
        });
    }
    let cfg = load_config(cli)?;
    match cfg.servers.len() {
        0 => {
            eprintln!("error: no servers registered. Use `crowkv server add ...` or pass --server <url|id>.");
            Err(ExitCode::from(1))
        }
        1 => Ok(cfg.servers[0].url.clone()),
        _ => {
            eprintln!("error: multiple servers registered; pass --server <url|id> to pick one");
            Err(ExitCode::from(1))
        }
    }
}

fn mgmt_client(cli: &Cli) -> Result<ServerClient, ExitCode> {
    let url = resolve_single_target(cli)?;
    ServerClient::new(url).map_err(|e| {
        eprintln!("error: build client: {e}");
        ExitCode::from(2)
    })
}

async fn run_store_verb(cli: &Cli, verb: StoreVerb) -> ExitCode {
    let client = match mgmt_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match verb {
        StoreVerb::Add {
            store_id,
            group_id,
            replica_id,
            port,
        } => match client
            .add_store(&AddStoreRequest {
                store_id,
                group_id,
                replica_id,
                port,
            })
            .await
        {
            Ok(summary) => {
                println!(
                    "added store {} (listen={}, groups={})",
                    summary.store_id,
                    summary.listen_addr.as_deref().unwrap_or("-"),
                    summary.group_count
                );
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: add store: {e}");
                ExitCode::from(2)
            }
        },
        StoreVerb::Remove { store_id } => match client.remove_store(store_id).await {
            Ok(()) => {
                println!("removed store {store_id}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: remove store: {e}");
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
                println!("{:>8}  {:>8}  LISTEN", "STORE", "GROUPS");
                for s in &stores {
                    println!("{:>8}  {:>8}  {}", s.store_id, s.group_count, s.listen_addr.as_deref().unwrap_or("-"));
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
                println!("store {} listen={}", detail.store_id, detail.listen_addr.as_deref().unwrap_or("-"));
                for g in &detail.groups {
                    println!(
                        "  group {} leader={} local_replica={} remotes={}",
                        g.group_id, g.leader_id, g.local_replica_id, g.remote_count
                    );
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: inspect store: {e}");
                ExitCode::from(2)
            }
        },
    }
}

async fn run_paxos_verb(cli: &Cli, verb: PaxosVerb) -> ExitCode {
    let client = match mgmt_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match verb {
        PaxosVerb::Add { store_id, group_id, replica_id } => match client.add_group(store_id, &AddGroupRequest { group_id, replica_id }).await {
            Ok(()) => {
                println!("added group {group_id} to store {store_id}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: add group: {e}");
                ExitCode::from(2)
            }
        },
        PaxosVerb::Remove { store_id, group_id } => match client.remove_group(store_id, group_id).await {
            Ok(()) => {
                println!("removed group {group_id} from store {store_id}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: remove group: {e}");
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
                println!("{:>8}  {:>8}  {:>10}  REMOTES", "GROUP", "LEADER", "LOCAL_REPL");
                for g in &groups {
                    println!("{:>8}  {:>8}  {:>10}  {}", g.group_id, g.leader_id, g.local_replica_id, g.remote_count);
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: list groups: {e}");
                ExitCode::from(2)
            }
        },
        PaxosVerb::Inspect { store_id, group_id } => match client.list_remotes(store_id, group_id).await {
            Ok(remotes) => {
                if cli.json {
                    return print_json(&remotes);
                }
                if remotes.is_empty() {
                    println!("(no remote replicas)");
                    return ExitCode::SUCCESS;
                }
                println!("{:>10}  ENDPOINT", "REPLICA");
                for r in &remotes {
                    println!("{:>10}  {}", r.replica_id, r.endpoint);
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: inspect group: {e}");
                ExitCode::from(2)
            }
        },
    }
}

async fn run_replica_verb(cli: &Cli, verb: ReplicaVerb) -> ExitCode {
    let client = match mgmt_client(cli) {
        Ok(c) => c,
        Err(c) => return c,
    };
    match verb {
        ReplicaVerb::Add {
            store_id,
            group_id,
            replica_id,
            endpoint,
        } => {
            let remotes = vec![RemoteReplicaInfo { replica_id, endpoint }];
            match client.add_remotes(store_id, group_id, &remotes).await {
                Ok(()) => {
                    println!("added remote replica {replica_id} to group {group_id}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: add replica: {e}");
                    ExitCode::from(2)
                }
            }
        }
        ReplicaVerb::Remove { store_id, group_id, replica_id } => match client.remove_remote(store_id, group_id, replica_id).await {
            Ok(()) => {
                println!("removed remote replica {replica_id} from group {group_id}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: remove replica: {e}");
                ExitCode::from(2)
            }
        },
    }
}

/// Resolve a store's gRPC `host:port` from the management API. The
/// store's `listen_addr` is `0.0.0.0:N`; we replace the host with the
/// management URL's host so the operator dialing remotely picks up the
/// right interface.
async fn resolve_kv_endpoint(cli: &Cli, store_id: u64) -> Result<String, ExitCode> {
    let mgmt_url = resolve_single_target(cli)?;
    let mgmt = ServerClient::new(mgmt_url.clone()).map_err(|e| {
        eprintln!("error: build client: {e}");
        ExitCode::from(2)
    })?;
    let detail = mgmt.get_store(store_id).await.map_err(|e| {
        eprintln!("error: lookup store {store_id}: {e}");
        ExitCode::from(2)
    })?;
    let listen = detail.listen_addr.ok_or_else(|| {
        eprintln!("error: store {store_id} has no listen_addr (server still starting?)");
        ExitCode::from(2)
    })?;
    // listen_addr is "0.0.0.0:N" or "[::]:N"; rewrite the host.
    let port = listen.rsplit(':').next().unwrap_or("");
    let host = mgmt_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split(':')
        .next()
        .unwrap_or("127.0.0.1");
    Ok(format!("{host}:{port}"))
}

async fn run_kv_verb(cli: &Cli, verb: KvVerb) -> ExitCode {
    match verb {
        KvVerb::Put {
            store_id,
            group_id,
            key,
            value,
            value_file,
            client_id,
            seq,
        } => {
            kv_put(
                cli,
                store_id,
                group_id,
                KvPutArgs {
                    key: &key,
                    value,
                    value_file,
                    client_id,
                    seq,
                },
            )
            .await
        }
        KvVerb::Get { store_id, group_id, key, hex } => kv_get(cli, store_id, group_id, &key, hex).await,
        KvVerb::Delete {
            store_id,
            group_id,
            key,
            client_id,
            seq,
        } => kv_delete(cli, store_id, group_id, &key, client_id, seq).await,
        KvVerb::List {
            store_id,
            group_id,
            prefix,
            limit,
        }
        | KvVerb::Scan {
            store_id,
            group_id,
            prefix,
            limit,
        } => kv_scan(cli, store_id, group_id, &prefix, limit).await,
    }
}

struct KvPutArgs<'a> {
    key: &'a str,
    value: Option<String>,
    value_file: Option<PathBuf>,
    client_id: u64,
    seq: u64,
}

async fn kv_put(cli: &Cli, store_id: u64, group_id: u64, args: KvPutArgs<'_>) -> ExitCode {
    let value_bytes = match (args.value, args.value_file) {
        (Some(v), None) => v.into_bytes(),
        (None, Some(p)) => match std::fs::read(&p) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("error: read --value-file {}: {e}", p.display());
                return ExitCode::from(1);
            }
        },
        (None, None) => {
            eprintln!("error: --value or --value-file is required");
            return ExitCode::from(1);
        }
        (Some(_), Some(_)) => unreachable!("clap conflicts_with"),
    };
    let endpoint = match resolve_kv_endpoint(cli, store_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let mut client = match KvClient::connect(endpoint).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: kv connect: {e}");
            return ExitCode::from(2);
        }
    };
    match client.put(group_id, args.key.as_bytes(), &value_bytes, args.client_id, args.seq).await {
        Ok(out) => {
            if cli.json {
                return print_json(&serde_json::json!({"ok": true, "revision": out.revision, "request_id": out.request_id}));
            }
            println!("ok: rev={} req={}", out.revision, out.request_id);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: put: {e}");
            ExitCode::from(2)
        }
    }
}

async fn kv_get(cli: &Cli, store_id: u64, group_id: u64, key: &str, hex: bool) -> ExitCode {
    let endpoint = match resolve_kv_endpoint(cli, store_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let mut client = match KvClient::connect(endpoint).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: kv connect: {e}");
            return ExitCode::from(2);
        }
    };
    match client.get(group_id, key.as_bytes()).await {
        Ok(GetOutcome::NotFound) => {
            if cli.json {
                return print_json(&serde_json::json!({"found": false}));
            }
            println!("(not found)");
            ExitCode::from(3)
        }
        Ok(GetOutcome::Found { value, revision }) => {
            if cli.json {
                return print_json(&serde_json::json!({
                    "found": true,
                    "revision": revision,
                    "value_hex": hex_encode(&value),
                    "value_utf8": String::from_utf8_lossy(&value),
                }));
            }
            if hex {
                println!("{}", hex_encode(&value));
            } else {
                // print full bytes verbatim to stdout; lossless for binary values.
                use std::io::Write;
                let mut out = std::io::stdout().lock();
                let _ = out.write_all(&value);
                let _ = out.write_all(b"\n");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: get: {e}");
            ExitCode::from(2)
        }
    }
}

async fn kv_delete(cli: &Cli, store_id: u64, group_id: u64, key: &str, client_id: u64, seq: u64) -> ExitCode {
    let endpoint = match resolve_kv_endpoint(cli, store_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let mut client = match KvClient::connect(endpoint).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: kv connect: {e}");
            return ExitCode::from(2);
        }
    };
    match client.delete(group_id, key.as_bytes(), client_id, seq).await {
        Ok(out) => {
            if cli.json {
                return print_json(&serde_json::json!({"ok": true, "revision": out.revision}));
            }
            println!("ok: rev={}", out.revision);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: delete: {e}");
            ExitCode::from(2)
        }
    }
}

async fn kv_scan(cli: &Cli, store_id: u64, group_id: u64, prefix: &str, limit: u32) -> ExitCode {
    let endpoint = match resolve_kv_endpoint(cli, store_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let mut client = match KvClient::connect(endpoint).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: kv connect: {e}");
            return ExitCode::from(2);
        }
    };
    match client.scan(group_id, prefix.as_bytes(), limit).await {
        Ok(_) => unreachable!("scan currently always errors"),
        Err(e) => {
            eprintln!("error: scan/list: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run_bench_verb(cli: &Cli, verb: BenchVerb) -> ExitCode {
    match verb {
        BenchVerb::Run {
            workload,
            store_id,
            group_id,
            connections,
            threads,
            duration_secs,
            key_space,
            value_size,
            run_id,
        } => {
            bench_run(
                cli,
                BenchRunArgs {
                    workload,
                    store_id,
                    group_id,
                    connections,
                    threads,
                    duration_secs,
                    key_space,
                    value_size,
                    run_id,
                },
                cli.json,
            )
            .await
        }
        BenchVerb::Stress { scenario, store_id, group_id } => bench_stress(cli, scenario, store_id, group_id, cli.json).await,
        BenchVerb::Report { run_id } => bench_report(&run_id, cli.json),
    }
}

struct BenchRunArgs {
    workload: String,
    store_id: u64,
    group_id: u64,
    connections: u32,
    threads: u32,
    duration_secs: u64,
    key_space: u64,
    value_size: usize,
    run_id: Option<String>,
}

async fn bench_run(cli: &Cli, args: BenchRunArgs, json: bool) -> ExitCode {
    use crowkv_console_bench::{run_bench, BenchConfig, WorkloadKind};
    use std::time::Duration;

    let kind = match WorkloadKind::parse(&args.workload) {
        Ok(k) => k,
        Err(bad) => {
            eprintln!("error: unknown workload {bad:?} (expected: read|write|list|mix)");
            return ExitCode::from(1);
        }
    };
    let endpoint = match resolve_kv_endpoint(cli, args.store_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let mut cfg = BenchConfig::defaults(endpoint, kind);
    cfg.store_id = args.store_id;
    cfg.group_id = args.group_id;
    cfg.connections = args.connections;
    cfg.threads = args.threads;
    cfg.duration = Duration::from_secs(args.duration_secs);
    cfg.key_space = args.key_space;
    cfg.value_size = args.value_size;
    cfg.run_id = args.run_id;
    match run_bench(cfg).await {
        Ok((report, path)) => {
            if json {
                return print_json(&report);
            }
            println!("{}", report.human_summary());
            println!("\nreport: {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: bench run: {e}");
            ExitCode::from(2)
        }
    }
}

async fn bench_stress(cli: &Cli, scenario: String, store_id: u64, group_id: u64, json: bool) -> ExitCode {
    use crowkv_console_bench::{run_bench, stress_scenario};

    let endpoint = match resolve_kv_endpoint(cli, store_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let Some(mut cfg) = stress_scenario(&scenario, endpoint) else {
        eprintln!("error: unknown stress scenario {scenario:?} (try: burst|soak|hotread)");
        return ExitCode::from(1);
    };
    cfg.store_id = store_id;
    cfg.group_id = group_id;
    match run_bench(cfg).await {
        Ok((report, path)) => {
            if json {
                return print_json(&report);
            }
            println!("{}", report.human_summary());
            println!("\nreport: {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: bench stress: {e}");
            ExitCode::from(2)
        }
    }
}

fn bench_report(run_id: &str, json: bool) -> ExitCode {
    use crowkv_console_bench::BenchReport;

    let Some(dir) = BenchReport::default_dir() else {
        eprintln!("error: cannot resolve $HOME for report dir");
        return ExitCode::from(1);
    };
    let path = dir.join(format!("{run_id}.json"));
    match BenchReport::read_from(&path) {
        Ok(r) => {
            if json {
                return print_json(&r);
            }
            println!("{}", r.human_summary());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: read report {}: {e}", path.display());
            ExitCode::from(1)
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}
