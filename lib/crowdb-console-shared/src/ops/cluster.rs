// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cluster-level operations: status, topology, init, reset, clean.
//!
//! `init` bootstraps group 0 (store 0, group 0) on the selected nodes,
//! wires remotes, and writes the hardware + KV-cluster topology into
//! group-0 sysdata. `reset` tears down the cluster in dependency order.
//! `clean` removes orphaned sysdata entries without touching running
//! servers.

use std::collections::HashSet;

use crowdb_protocol::common::{HwStatus, NodeValue, RackValue, ReplicaValue};
use crowdb_protocol::mgmt::{RemoteReplicaInfo, SystemInitRequest};

use crate::clients::http::ServerClient;
use crate::config::{NodeEntry, RackEntry, ReplicaEntry, ServerEntry, ServiceType};
use crate::error::{Error, Result};
use crate::lifecycle::{self, crowdb_kv_server_bin, DeployRequest};
use crate::ops::OpContext;
use crate::test_ports;

/// Summary of a completed cluster init.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InitSummary {
    pub store_id: u64,
    pub group_id: u64,
    pub nodes: Vec<(u64, u64)>,
}

fn server_client(ctx: &OpContext, node_id: u64) -> Result<ServerClient> {
    let url = ctx.node_mgmt_url(node_id)?;
    ServerClient::new(&url).map_err(|e| Error::UpstreamRpc {
        node_id: url,
        status: format!("client build: {e}"),
    })
}

async fn rpc_endpoint_for_store(ctx: &OpContext, node_id: u64, store_id: u64) -> Option<String> {
    let client = server_client(ctx, node_id).ok()?;
    let stores = client.topology().await.ok()?;
    for s in &stores {
        if s.store_id == store_id {
            if let Some(addr) = &s.listen_addr {
                let stripped = addr
                    .strip_prefix("http://")
                    .or_else(|| addr.strip_prefix("https://"))
                    .unwrap_or(addr);
                let remapped = stripped
                    .strip_prefix("0.0.0.0:")
                    .map_or_else(|| stripped.to_string(), |port| format!("127.0.0.1:{port}"));
                return Some(remapped);
            }
        }
    }
    None
}

/// Seed the KV client with the group-0 leader endpoint after init.
/// For single-node, use the node's own RPC endpoint. For multi-node,
/// wait for election to complete and read the leader from topology.
async fn seed_leader_after_init(
    ctx: &OpContext,
    single_node: bool,
    succeeded: &[(u64, u64)],
    mgmt_seeds: &[String],
) {
    if single_node {
        for (nid, _) in succeeded {
            if let Some(ep) = rpc_endpoint_for_store(ctx, *nid, 0).await {
                ctx.kv().seed_leader(0, 0, ep);
                break;
            }
        }
        return;
    }
    if let Some(leader_url) = wait_for_leader(mgmt_seeds, 0, 0, std::time::Duration::from_secs(10)).await {
        if let Ok(sc) = ServerClient::new(&leader_url) {
            if let Ok(topo) = sc.topology().await {
                for store in &topo {
                    if store.store_id == 0 {
                        for group in &store.groups {
                            if group.group_id == 0 && group.leader_id > 0 {
                                for remote in &group.remotes {
                                    if remote.id == group.leader_id {
                                        let ep = remote.endpoint.clone();
                                        let stripped = ep
                                            .strip_prefix("http://")
                                            .or_else(|| ep.strip_prefix("https://"))
                                            .unwrap_or(&ep);
                                        let remapped = stripped.strip_prefix("0.0.0.0:").map_or_else(
                                            || stripped.to_string(),
                                            |port| format!("127.0.0.1:{port}"),
                                        );
                                        ctx.kv().seed_leader(0, 0, remapped);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Initialize the cluster by bootstrapping group 0 on the listed nodes.
///
/// # Errors
/// Returns [`Error::Validation`] if `nodes` is empty;
/// [`Error::NodeUnreachable`] if a node is not reachable;
/// [`Error::UpstreamRpc`] if `system/init` fails on a node.
#[allow(clippy::too_many_lines)]
pub async fn init(ctx: &OpContext, nodes: &[u64]) -> Result<InitSummary> {
    if nodes.is_empty() {
        return Err(Error::Validation {
            field: "nodes".into(),
            message: "nodes list must not be empty".into(),
        });
    }

    let mut seen = HashSet::new();
    let mut target_nodes = nodes.to_vec();
    target_nodes.retain(|nid| seen.insert(*nid));
    let single_node = target_nodes.len() == 1;

    // Phase 1: call /system/init on each node concurrently.
    let init_results: Vec<Result<(u64, u64)>> =
        futures::future::join_all(target_nodes.iter().enumerate().map(|(i, nid)| {
            let nid = *nid;
            async move {
                let client = server_client(ctx, nid)?;
                client.health().await.map_err(|e| Error::NodeUnreachable {
                    node_id: nid.to_string(),
                    reason: e.to_string(),
                })?;
                let replica_id = 1 + i as u64;
                let req = SystemInitRequest {
                    replica_id,
                    start_election: single_node,
                };
                match client.system_init(&req).await {
                    Ok(_) => Ok((nid, replica_id)),
                    Err(e) => {
                        let is_already_init = matches!(
                            &e,
                            Error::UpstreamRpc { status, .. } if status.contains("409")
                        );
                        if is_already_init {
                            Ok((nid, replica_id))
                        } else {
                            Err(Error::UpstreamRpc {
                                node_id: nid.to_string(),
                                status: format!("system/init failed: {e}"),
                            })
                        }
                    }
                }
            }
        }))
        .await;

    // Collect successes; on any failure, roll back and return the first error.
    let mut succeeded: Vec<(u64, u64)> = Vec::new();
    let mut first_err: Option<Error> = None;
    for res in init_results {
        match res {
            Ok(entry) => succeeded.push(entry),
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }
    if let Some(e) = first_err {
        // Rollback: remove group 0 on nodes that succeeded (concurrently).
        futures::future::join_all(succeeded.iter().filter_map(|(ok_nid, _)| {
            server_client(ctx, *ok_nid).ok().map(|c| async move {
                let _ = c.remove_group(0, 0).await;
            })
        }))
        .await;
        return Err(e);
    }

    // Phase 2: wire remotes for multi-node. Fetch all peer endpoints
    // concurrently, then wire each node's remotes.
    if !single_node {
        // Fetch all endpoints in parallel.
        let endpoints: Vec<Option<String>> =
            futures::future::join_all(succeeded.iter().map(|(peer_nid, _)| {
                let peer_nid = *peer_nid;
                async move { rpc_endpoint_for_store(ctx, peer_nid, 0).await }
            }))
            .await;

        // Wire remotes on each node concurrently.
        futures::future::join_all(succeeded.iter().enumerate().map(|(i, (nid, _))| {
            let nid = *nid;
            let remotes: Vec<RemoteReplicaInfo> = succeeded
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .filter_map(|(j, (_, peer_rid))| {
                    endpoints.get(j).and_then(|ep| {
                        ep.as_ref().map(|ep| RemoteReplicaInfo {
                            replica_id: *peer_rid,
                            endpoint: ep.clone(),
                            voting: true,
                        })
                    })
                })
                .collect();
            async move {
                if remotes.is_empty() {
                    return;
                }
                if let Ok(client) = server_client(ctx, nid) {
                    let _ = client.add_remote_replicas(0, 0, &remotes).await;
                }
            }
        }))
        .await;
    }

    // Phase 3: persist topology in local config.
    let store_nodes: Vec<u64> = succeeded.iter().map(|(n, _)| *n).collect();
    let replicas: Vec<ReplicaEntry> = succeeded
        .iter()
        .map(|(nid, rid)| ReplicaEntry {
            replica_id: *rid,
            node_id: *nid,
        })
        .collect();
    {
        let mut cfg = ctx.config_mut();
        cfg.record_store(0, store_nodes.clone());
        cfg.record_group(0, 0, replicas);
    }

    // Phase 4: seed the KV client with the group-0 leader endpoint so
    // `write_topology_to_sysdata` (which uses `ctx.sysmd()`) can reach
    // group 0. Without this, the shared `CrowdbKvClient` may have a
    // stale or dummy leader hint from before init.
    let mgmt_seeds: Vec<String> = succeeded
        .iter()
        .filter_map(|(nid, _)| ctx.node_mgmt_url(*nid).ok())
        .collect();
    ctx.kv().set_mgmt_seeds(mgmt_seeds.clone());

    // For multi-node init, the election driver starts after remotes are
    // wired (Phase 2) but the leader isn't elected yet. Wait for the
    // leader before seeding + writing sysdata — otherwise sysdata writes
    // fail with "not leader" and callers see spurious errors.
    seed_leader_after_init(ctx, single_node, &succeeded, &mgmt_seeds).await;

    // Phase 5: write hardware + KV-cluster topology into group-0 sysdata.
    write_topology_to_sysdata(ctx, &store_nodes, &succeeded).await;

    Ok(InitSummary {
        store_id: 0,
        group_id: 0,
        nodes: succeeded,
    })
}

/// Write the hardware hierarchy + KV-cluster topology from the local
/// config into group-0 sysdata. Best-effort: individual write failures
/// are logged and skipped.
async fn write_topology_to_sysdata(ctx: &OpContext, store_nodes: &[u64], succeeded: &[(u64, u64)]) {
    let cfg_snapshot = ctx.config().clone();
    let sysmd = ctx.sysmd();

    // All sysmd keys (racks, nodes, stores, groups, replicas) are
    // independent — write them concurrently to avoid sequential RTTs.
    let mut writes: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Hardware hierarchy.
    for rack in &cfg_snapshot.racks {
        let sysmd = sysmd.clone();
        let rack_id = rack.id;
        let value = RackValue {
            status: HwStatus::Up as i32,
            node_ids: Vec::new(),
        };
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_rack(rack_id, &value).await;
        }));
    }
    for node in &cfg_snapshot.nodes {
        let sysmd = sysmd.clone();
        let rack_id = node.rack_id;
        let node_id = node.id;
        let value = NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: Vec::new(),
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        };
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_node(rack_id, node_id, &value).await;
        }));
    }

    // KV-cluster topology.
    {
        let sysmd = sysmd.clone();
        let node_ids = store_nodes.to_vec();
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_store(0, &node_ids).await;
        }));
    }
    {
        let sysmd = sysmd.clone();
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_group(0, 0).await;
        }));
    }
    for (nid, rid) in succeeded {
        let sysmd = sysmd.clone();
        let endpoint = cfg_snapshot
            .server_for_node(*nid)
            .and_then(|s| s.rpc_url.clone())
            .unwrap_or_default();
        let value = ReplicaValue {
            store_id: 0,
            group_id: 0,
            replica_id: *rid,
            node_id: *nid,
            role: String::new(),
            voting: true,
            endpoint,
        };
        writes.push(tokio::spawn(async move {
            let _ = sysmd.add_replica(&value).await;
        }));
    }

    for h in writes {
        let _ = h.await;
    }
}

/// Get cluster status: list all stores from group-0 sysdata.
///
/// # Errors
/// Returns an error if the group-0 sysdata read fails.
pub async fn status(ctx: &OpContext) -> Result<Vec<crowdb_protocol::common::StoreValue>> {
    ctx.sysmd().list_stores().await.map_err(Into::into)
}

/// Get the topology view from a node's `/topology` endpoint.
///
/// # Errors
/// Returns [`Error::NotFound`] if no server is deployed on the node.
pub async fn topology(ctx: &OpContext, node_id: u64) -> Result<Vec<crate::snapshot::StoreView>> {
    let client = server_client(ctx, node_id)?;
    client.topology().await
}

/// Reset the cluster: tear down all groups, stores, and sysdata in
/// dependency order. Stops all running servers first.
///
/// # Errors
/// Returns an error if any teardown step fails (best-effort: continues
/// on partial failures and returns the first error).
pub async fn destroy(ctx: &OpContext) -> Result<()> {
    let cfg = ctx.config().clone();

    // Phase 1: stop all running servers (Kv, Diskdb, Rpc).
    for server in &cfg.servers {
        if let Some(pid) = server.pid {
            let _ = crate::lifecycle::stop_pid(pid);
        }
    }

    // Phase 2: remove all non-system groups from each KV node.
    for server in &cfg.servers {
        if server.service_type != crate::config::ServiceType::Kv {
            continue;
        }
        if let Some(node_id) = server.node_id {
            if let Ok(client) = server_client(ctx, node_id) {
                if let Ok(stores) = client.topology().await {
                    for s in &stores {
                        if s.store_id == 0 {
                            continue;
                        }
                        let _ = client.remove_store(s.store_id).await;
                    }
                }
                // Remove group 0 last.
                let _ = client.remove_group(0, 0).await;
            }
        }
    }

    // Phase 3: clear sysdata (best-effort).
    let sysmd = ctx.sysmd();
    let stores = sysmd.list_stores().await.unwrap_or_default();
    for s in &stores {
        let _ = sysmd.remove_store(s.store_id).await;
    }

    // Phase 4: clear local config.
    {
        let mut cfg = ctx.config_mut();
        cfg.stores.clear();
        cfg.groups.clear();
        cfg.servers.clear();
    }

    Ok(())
}

/// Remove orphaned sysdata entries (stores/groups/replicas that have
/// no corresponding running server). Does not stop any running
/// servers.
///
/// # Errors
/// Returns an error if the sysdata scan fails.
pub async fn reset(ctx: &OpContext) -> Result<()> {
    let sysmd = ctx.sysmd();
    let stores = sysmd.list_stores().await?;

    // For each store, check if any hosting node has a running server.
    let cfg = ctx.config().clone();
    for store in &stores {
        let mut any_alive = false;
        for node_id in &store.node_ids {
            if cfg.server_for_node(*node_id).is_some() {
                if let Ok(client) = server_client(ctx, *node_id) {
                    if client.health().await.is_ok() {
                        any_alive = true;
                        break;
                    }
                }
            }
        }
        if !any_alive {
            let _ = sysmd.remove_store(store.store_id).await;
        }
    }

    Ok(())
}

/// Result of [`clean`] — wipe user data + wait for re-election.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CleanResult {
    pub new_leader: String,
    pub wiped_nodes: u64,
}

/// Wipe user data on every node (drop + recreate WAL + engine for
/// the target store/group) and wait for re-election. When targeting
/// group 0, preserves sysdata + topology — servers stay running, only
/// user data is cleared. For benchmarks, prefer targeting a non-system
/// group (store 0, group 1+) to avoid touching group-0 sysdata.
///
/// # Errors
/// Returns an error if no servers are configured.
pub async fn clean(ctx: &OpContext, store_id: u64, group_id: u64) -> Result<CleanResult> {
    let cfg = ctx.config().clone();
    let mut mgmt_urls: Vec<String> = cfg.servers.iter().map(|s| s.url.clone()).collect();
    mgmt_urls.sort();
    mgmt_urls.dedup();
    if mgmt_urls.is_empty() {
        return Err(Error::Validation {
            field: "servers".into(),
            message: "no servers in config".into(),
        });
    }

    // Wipe user data on every node concurrently.
    let mut wiped = 0u64;
    let mut handles = Vec::with_capacity(mgmt_urls.len());
    for url in &mgmt_urls {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let Ok(sc) = ServerClient::new(&url) else {
                return false;
            };
            sc.wipe_user_data(store_id, group_id)
                .await
                .is_ok_and(|r| r.accepted)
        }));
    }
    for h in handles {
        if let Ok(true) = h.await {
            wiped += 1;
        }
    }

    // Wait for re-election: poll topology until a leader is found.
    let leader = wait_for_leader(&mgmt_urls, store_id, group_id, std::time::Duration::from_secs(10)).await;
    Ok(CleanResult {
        new_leader: leader.unwrap_or_default(),
        wiped_nodes: wiped,
    })
}

/// Poll `/topology` on every server until a leader for the target
/// store/group is elected (`leader_id` > 0), or `timeout` elapses.
async fn wait_for_leader(
    mgmt_urls: &[String],
    store_id: u64,
    group_id: u64,
    timeout: std::time::Duration,
) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::time::Instant::now() >= deadline {
            return None;
        }
        // Poll each node's topology endpoint. Only return the mgmt URL
        // of the node whose local replica IS the leader — the caller
        // (seed_leader_after_init) needs an HTTP mgmt URL to query
        // topology and extract the leader's RPC endpoint. Returning a
        // remote's RPC endpoint here causes seed_leader_after_init to
        // fail silently (ServerClient::new + topology() on a non-HTTP
        // endpoint), leaving the KV client without a leader hint.
        for url in mgmt_urls {
            let Ok(sc) = ServerClient::new(url) else { continue };
            if let Ok(stores) = sc.topology().await {
                for store in &stores {
                    if store.store_id != store_id {
                        continue;
                    }
                    for group in &store.groups {
                        if group.group_id == group_id
                            && group.leader_id > 0
                            && group.leader_id == group.local_replica_id
                        {
                            return Some(url.clone());
                        }
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Summary of a completed local deploy.
#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalDeploySummary {
    pub node_count: usize,
    pub rack_id: u64,
    pub node_ids: Vec<u64>,
    pub init_summary: InitSummary,
}

/// Deploy a local N-node KV cluster on `127.0.0.1`: creates rack 1,
/// nodes 1..=N in the config, forks a `crowdb-kv-server` on each node
/// with auto-allocated ports, then bootstraps group 0 via [`init`].
///
/// The `workspace_dir` is used as the runtime root for the spawned
/// servers (logs, data). If `None`, a temp directory is used.
///
/// # Errors
/// Optional server tunables for [`local_deploy`]. `None` fields leave
/// the spawned server's own default in effect.
#[derive(Debug, Clone, Default)]
pub struct KvDeployTunables {
    /// `--rpc-workers` value.
    pub rpc_workers: Option<u32>,
    /// `--peer-pool-size` value.
    pub peer_pool_size: Option<usize>,
    /// `--max-inflight` value.
    pub max_inflight: Option<usize>,
    /// `--coalesce-max-keys` value.
    pub coalesce_max_keys: Option<usize>,
    /// `--coalesce-drain-threshold` value.
    pub coalesce_drain_threshold: Option<usize>,
    /// `--event-write` flag.
    pub event_write: Option<bool>,
    /// `--send-queue-capacity` value.
    pub send_queue_capacity: Option<u32>,
    /// `--metrics-interval` value in seconds. `None` leaves the
    /// spawned server's own default (5s) in effect.
    pub metrics_interval: Option<u64>,
    /// `--kv-backend` value (e.g. `"file"`, `"block"`, `"mem-block"`).
    /// `None` leaves the spawned server's own default (`file`) in effect.
    pub kv_backend: Option<String>,
    /// `--wal-backend` value (e.g. `"file"`, `"mem-block"`, `"block-device"`).
    /// `None` leaves the spawned server's own default in effect.
    pub wal_backend: Option<String>,
    /// `--no-fsync` flag. `None` leaves the spawned server's default (fsync on).
    pub no_fsync: Option<bool>,
}

/// Deploy a local N-node KV cluster on `127.0.0.1`: creates rack 1,
/// nodes 1..=N in the config, forks a `crowdb-kv-server` on each node
/// with auto-allocated ports, then bootstraps group 0 via [`init`].
///
/// The `workspace_dir` is used as the runtime root for the spawned
/// servers (logs, data). If `None`, a temp directory is used.
///
/// `tunables` overrides server-side flags (`--event-write`,
/// `--peer-pool-size`, etc.). `None` uses server defaults.
///
/// # Errors
/// Returns [`Error::NotFound`] if the `crowdb-kv-server` binary cannot
/// be located. Returns [`Error::Io`] on spawn/readiness failures.
pub async fn local_deploy(
    ctx: &OpContext,
    node_count: usize,
    workspace_dir: Option<&std::path::Path>,
    tunables: Option<&KvDeployTunables>,
) -> Result<LocalDeploySummary> {
    if node_count == 0 {
        return Err(Error::Validation {
            field: "node_count".into(),
            message: "node_count must be >= 1".into(),
        });
    }

    let bin = crowdb_kv_server_bin().ok_or_else(|| Error::NotFound {
        kind: "binary".into(),
        id: "crowdb-kv-server".into(),
    })?;
    if !bin.exists() {
        return Err(Error::NotFound {
            kind: "binary".into(),
            id: bin.display().to_string(),
        });
    }

    let workspace = workspace_dir.map_or_else(default_workspace, std::path::PathBuf::from);
    std::fs::create_dir_all(&workspace)?;

    let rack_id: u64 = 1;
    let node_ids: Vec<u64> = (1..=u64::try_from(node_count).unwrap_or(u64::MAX)).collect();

    write_rack_and_nodes(ctx, rack_id, &node_ids);
    deploy_servers(ctx, &bin, &workspace, rack_id, &node_ids, tunables).await?;

    // Re-seed the group-0 leader hint to the first deployed server's
    // RPC endpoint so sysdata writes during `init` target the right node.
    if let Some(first) = ctx.config().servers.first() {
        if let Some(rpc_url) = &first.rpc_url {
            let endpoint = rpc_url
                .strip_prefix("http://")
                .or_else(|| rpc_url.strip_prefix("https://"))
                .unwrap_or(rpc_url)
                .to_string();
            ctx.seed_group0_leader(endpoint);
        }
    }

    let init_summary = init(ctx, &node_ids).await?;

    Ok(LocalDeploySummary {
        node_count,
        rack_id,
        node_ids,
        init_summary,
    })
}

/// Default workspace path for `local_deploy` when no explicit
/// `workspace_dir` is provided. Uses a project-local `cli-deploy/`
/// directory (resolved from CWD) so logs and data survive for
/// inspection instead of being lost in `/tmp`.
fn default_workspace() -> std::path::PathBuf {
    let base = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    base.join("cli-deploy").join(format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ))
}

/// Phase 1: write rack 1 + nodes 1..=N into the config (idempotent).
fn write_rack_and_nodes(ctx: &OpContext, rack_id: u64, node_ids: &[u64]) {
    let mut cfg = ctx.config_mut();
    if cfg.racks.iter().all(|r| r.id != rack_id) {
        let _ = cfg.add_rack(RackEntry {
            id: rack_id,
            name: format!("rack-{rack_id}"),
        });
    }
    for nid in node_ids {
        if cfg.nodes.iter().all(|n| n.id != *nid) {
            let _ = cfg.add_node(NodeEntry {
                id: *nid,
                rack_id,
                host: "127.0.0.1".into(),
                ssh_port: 22,
                ssh_user: String::new(),
                ssh_key: None,
                ssh_password: None,
            });
        }
    }
}

/// Phase 2: fork a `crowdb-kv-server` on each node (skips nodes that
/// already have a deployed server).
///
/// # Errors
/// Returns [`Error::Io`] on spawn/readiness failures.
async fn deploy_servers(
    ctx: &OpContext,
    bin: &std::path::Path,
    workspace: &std::path::Path,
    rack_id: u64,
    node_ids: &[u64],
    tunables: Option<&KvDeployTunables>,
) -> Result<()> {
    let port_base = test_ports::unique_test_port_range(u16::try_from(node_ids.len() * 2).unwrap_or(u16::MAX));
    for (i, nid) in node_ids.iter().enumerate() {
        let offset = u16::try_from(i * 2).unwrap_or(u16::MAX);
        let rest_port = port_base + offset;
        let rpc_port = port_base + offset + 1;

        // Skip if a server is already deployed on this node.
        if ctx.config().server_for_node(*nid).is_some() {
            continue;
        }

        let req = DeployRequest {
            server_id: nid.to_string(),
            rest_port,
            rpc_port,
            election_profile: Some("e2e".into()),
            binary: Some(bin.to_path_buf()),
            rpc_workers: tunables.and_then(|t| t.rpc_workers),
            peer_pool_size: tunables.and_then(|t| t.peer_pool_size),
            max_inflight: tunables.and_then(|t| t.max_inflight),
            coalesce_max_keys: tunables.and_then(|t| t.coalesce_max_keys),
            coalesce_drain_threshold: tunables.and_then(|t| t.coalesce_drain_threshold),
            event_write: tunables.and_then(|t| t.event_write),
            send_queue_capacity: tunables.and_then(|t| t.send_queue_capacity),
            metrics_interval: tunables.and_then(|t| t.metrics_interval),
            kv_backend: tunables.and_then(|t| t.kv_backend.clone()),
            wal_backend: tunables.and_then(|t| t.wal_backend.clone()),
            no_fsync: tunables.and_then(|t| t.no_fsync).unwrap_or(false),
            ..Default::default()
        };
        let node_entry = NodeEntry {
            id: *nid,
            rack_id,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        };
        // Each node gets its own subdirectory under the workspace so
        // WAL data, btree data, and logs are isolated per kv-server.
        let node_dir = workspace
            .join(format!("rack{rack_id}"))
            .join(format!("node{nid}"));
        std::fs::create_dir_all(node_dir.join("log"))?;
        std::fs::create_dir_all(node_dir.join("bin"))?;
        let deployed = lifecycle::deploy_local_in_dir(&req, &node_entry, &node_dir).await?;

        let mut cfg = ctx.config_mut();
        cfg.add_server(ServerEntry {
            id: nid.to_string(),
            url: deployed.mgmt_url.clone(),
            node_id: Some(*nid),
            rpc_url: Some(deployed.rpc_url.clone()),
            rest_port: Some(rest_port),
            rpc_port: Some(rpc_port),
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: Some(deployed.pid),
            service_type: ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        })?;
    }
    Ok(())
}

// ── RPC fb-server deploy ──────────────────────────────────────────

/// Summary of a completed RPC fb-server deploy.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RpcDeploySummary {
    pub port: u16,
    pub pid: u32,
    pub io_engines: u32,
    pub io_workers: u32,
    pub nagle: bool,
}

/// Configuration for [`local_deploy_rpc`].
#[derive(Debug, Clone)]
pub struct RpcDeployConfig {
    /// Listen port. 0 = auto-allocate.
    pub port: u16,
    pub io_engines: u32,
    pub io_workers: u32,
    pub enable_nagle: bool,
    pub metrics_interval: u32,
}

impl Default for RpcDeployConfig {
    fn default() -> Self {
        Self {
            port: 0,
            io_engines: 1,
            io_workers: 1,
            enable_nagle: false,
            metrics_interval: 2,
        }
    }
}

/// Deploy a standalone `crowdb-rpc-fb-server` (C++ echo server) on
/// `127.0.0.1`. Spawns the process, waits for the `listening port=`
/// readiness line on stdout, and records the PID + port in the config
/// so `cluster destroy` can stop it.
///
/// Unlike [`local_deploy`], this does not create rack/node entries or
/// bootstrap group 0 — the fb-server is a raw RPC echo endpoint with
/// no KV layer.
///
/// # Errors
/// Returns [`Error::NotFound`] if the binary cannot be located.
/// Returns [`Error::Io`] on spawn or readiness failures.
pub async fn local_deploy_rpc(
    ctx: &OpContext,
    cfg: &RpcDeployConfig,
    workspace_dir: Option<&std::path::Path>,
) -> Result<RpcDeploySummary> {
    let bin = lifecycle::crowdb_rpc_fb_server_bin().ok_or_else(|| Error::NotFound {
        kind: "binary".into(),
        id: "crowdb-rpc-fb-server".into(),
    })?;
    if !bin.exists() {
        return Err(Error::NotFound {
            kind: "binary".into(),
            id: bin.display().to_string(),
        });
    }

    let port = if cfg.port == 0 {
        test_ports::unique_test_port()
    } else {
        cfg.port
    };

    let workspace = workspace_dir.map_or_else(default_workspace, std::path::PathBuf::from);
    let log_dir = workspace.join("log");
    std::fs::create_dir_all(&log_dir)?;

    let mut cmd = std::process::Command::new(&bin);
    cmd.arg(format!("--port={port}"))
        .arg(format!("--io_engines={}", cfg.io_engines))
        .arg(format!("--io_workers={}", cfg.io_workers))
        .arg(format!("--logdir={}", log_dir.display()))
        .arg(format!("--metrics_interval={}", cfg.metrics_interval));
    if cfg.enable_nagle {
        cmd.arg("--enable_nagle");
    }
    // Redirect stdout/stderr to a log file so the child doesn't get
    // SIGPIPE when the CLI exits. The fb-server also writes structured
    // logs to --logdir, but stdout has the "listening port=" line.
    let stdout_path = log_dir.join("fb-server.stdout.log");
    let stdout = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stdout_path)
        .map_err(Error::Io)?;
    let stderr = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(stdout_path)
        .map_err(Error::Io)?;
    cmd.stdout(std::process::Stdio::from(stdout))
        .stderr(std::process::Stdio::from(stderr));

    let child = cmd.spawn()?;

    let pid = child.id();
    let ready = wait_for_rpc_ready(port, pid, 10).await;

    if !ready {
        let _ = lifecycle::stop_pid(pid);
        return Err(Error::Io(std::io::Error::other(format!(
            "crowdb-rpc-fb-server did not bind on port {port} within 10s"
        ))));
    }

    // Record in config so reset() can stop it.
    let server_id = format!("rpc-fb-{pid}");
    let mut config = ctx.config_mut();
    config.add_server(ServerEntry {
        id: server_id.clone(),
        url: format!("http://127.0.0.1:{port}"),
        node_id: None,
        rpc_url: Some(format!("http://127.0.0.1:{port}")),
        rest_port: None,
        rpc_port: Some(port),
        auto_start: false,
        binary: Some(bin.display().to_string()),
        election_profile: None,
        pid: Some(pid),
        service_type: ServiceType::Rpc,
        rpc_workers: None,
        no_fsync: false,
    })?;

    Ok(RpcDeploySummary {
        port,
        pid,
        io_engines: cfg.io_engines,
        io_workers: cfg.io_workers,
        nagle: cfg.enable_nagle,
    })
}

/// Poll-connect to `127.0.0.1:port` until success or timeout. Drops
/// the stdout pipe (the fb-server writes logs to `--logdir`, not stdout
/// — stdout is only used for the `listening port=` line which we don't
/// need since we know the port).
async fn wait_for_rpc_ready(port: u16, pid: u32, timeout_secs: u64) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        if !lifecycle::process_is_alive(pid) {
            return false;
        }
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
