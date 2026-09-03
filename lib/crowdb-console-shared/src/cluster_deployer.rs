// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowdbClusterDeployer` — reusable cluster lifecycle manager.
//!
//! Encapsulates the full cluster deploy/wait/cleanup cycle so that
//! the bench fixture, CLI, and E2E tests share one implementation:
//! reset, then provision racks/nodes, deploy KV servers, `cluster_init`,
//! stores/groups, (optional) diskdb + disk-groups, wait healthy,
//! collect info, then stop.
//!
//! The deployer talks to a `crowdb-web` instance via `ConsoleClient`.
//! It does not spawn `crowdb-web` itself — the caller is responsible
//! for starting the console (embedded or external) and passing its
//! base URL.

use std::time::{Duration, Instant};

use crowdb_protocol::port_alloc::{self, PortAllocConfig};
use crowdb_protocol::ServicePort;

use crate::clients::console::{
    AddDiskBody, AddDiskGroupBody, AddRackBody, ConsoleClient, CreateGroupBody, CreateStoreBody,
    DeployNodeServerBody, ResetResult,
};
use crate::cluster::{NodeId, RackId};
use crate::config::NodeEntry;
use crate::diskdb::DeployDiskdbBody;
use crate::error::{Error, Result};

/// Threshold above which a phase is considered slow (warning).
const SLOW_THRESHOLD: Duration = Duration::from_secs(2);
/// Threshold above which a phase is considered very slow (error).
const VERY_SLOW_THRESHOLD: Duration = Duration::from_secs(5);

/// Time a phase and log at the appropriate level.
/// Returns the elapsed duration so callers can aggregate.
fn log_phase_time(phase: &str, start: Instant) -> Duration {
    let elapsed = start.elapsed();
    let ms = elapsed.as_millis();
    if elapsed >= VERY_SLOW_THRESHOLD {
        tracing::error!(
            phase = phase,
            elapsed_ms = ms,
            "deployer phase '{}' took {ms}ms (very slow, expected <{}ms)",
            phase,
            VERY_SLOW_THRESHOLD.as_millis()
        );
    } else if elapsed >= SLOW_THRESHOLD {
        tracing::warn!(
            phase = phase,
            elapsed_ms = ms,
            "deployer phase '{}' took {ms}ms (slow, expected <{}ms)",
            phase,
            SLOW_THRESHOLD.as_millis()
        );
    } else {
        tracing::debug!(
            phase = phase,
            elapsed_ms = ms,
            "deployer phase '{}' took {ms}ms",
            phase
        );
    }
    elapsed
}

/// Collected cluster info after a successful `start`.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub racks: Vec<RackId>,
    pub nodes: Vec<NodeInfo>,
    pub stores: Vec<StoreInfo>,
    pub diskdb_instances: Vec<DiskdbInfo>,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub rack_id: RackId,
    pub pid: u32,
    pub mgmt_url: String,
    pub rpc_url: String,
    pub rest_port: u16,
    pub rpc_port: u16,
}

#[derive(Debug, Clone)]
pub struct StoreInfo {
    pub store_id: u64,
    pub nodes: Vec<NodeId>,
    pub groups: Vec<GroupInfo>,
}

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub group_id: u64,
    pub leader_node_id: Option<NodeId>,
    pub leader_endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DiskdbInfo {
    pub node_id: NodeId,
    pub pid: u32,
    pub rpc_port: u16,
}

/// Topology descriptor — what to deploy.
#[derive(Debug, Clone)]
pub struct TopologyDescriptor {
    pub node_count: usize,
    pub store_count: usize,
    pub groups_per_store: usize,
    pub replicas_per_group: usize,
    pub rack_base: u64,
    pub node_base: u64,
    pub store_base: u64,
    pub group_base: u64,
    /// Deploy a diskdb instance on each node.
    pub deploy_diskdb: bool,
    /// Disk-groups per node (0 = none).
    pub disk_groups_per_node: usize,
    /// Disks per disk-group (0 = none).
    pub disks_per_group: usize,
}

impl Default for TopologyDescriptor {
    fn default() -> Self {
        Self {
            node_count: 3,
            store_count: 1,
            groups_per_store: 1,
            replicas_per_group: 3,
            rack_base: 100,
            node_base: 100,
            store_base: 800,
            group_base: 8000,
            deploy_diskdb: false,
            disk_groups_per_node: 0,
            disks_per_group: 0,
        }
    }
}

/// Simple 3-node / 1-store / 1-group topology (no diskdb).
#[must_use]
pub fn simple() -> TopologyDescriptor {
    TopologyDescriptor::default()
}

/// 8-node / 2-store / 2-group topology (no diskdb).
#[must_use]
pub fn complex() -> TopologyDescriptor {
    TopologyDescriptor {
        node_count: 8,
        store_count: 2,
        groups_per_store: 2,
        replicas_per_group: 3,
        rack_base: 200,
        node_base: 200,
        store_base: 900,
        group_base: 9000,
        deploy_diskdb: false,
        disk_groups_per_node: 0,
        disks_per_group: 0,
    }
}

/// Reusable cluster lifecycle manager.
///
/// Wraps a `ConsoleClient` and drives the full deploy/wait/cleanup
/// cycle. The caller starts the `crowdb-web` instance (embedded or
/// external) and passes its base URL to [`CrowdbClusterDeployer::new`].
pub struct CrowdbClusterDeployer {
    client: ConsoleClient,
    info: Option<ClusterInfo>,
    /// Node IDs that we deployed (for stop).
    deployed_node_ids: Vec<NodeId>,
    /// Node IDs that have diskdb (for stop).
    diskdb_node_ids: Vec<NodeId>,
}

impl CrowdbClusterDeployer {
    /// Create a new deployer pointing at a running `crowdb-web` instance.
    ///
    /// # Errors
    /// Fails if the `ConsoleClient` cannot be constructed.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: ConsoleClient::new(base_url)?,
            info: None,
            deployed_node_ids: Vec::new(),
            diskdb_node_ids: Vec::new(),
        })
    }

    /// Borrow the underlying `ConsoleClient` for ad-hoc API calls.
    #[must_use]
    pub fn client(&self) -> &ConsoleClient {
        &self.client
    }

    /// Collected cluster info after `start`. `None` until `start`
    /// succeeds.
    #[must_use]
    pub fn info(&self) -> Option<&ClusterInfo> {
        self.info.as_ref()
    }

    /// `POST /internal/reset` — full cluster destroy. Fast when no
    /// servers are running (<0.1s); graceful shutdown when they are.
    ///
    /// # Errors
    /// Surfaces HTTP errors from the console.
    pub async fn reset(&self) -> Result<ResetResult> {
        let t = Instant::now();
        let result = self.client.reset_all().await;
        log_phase_time("reset", t);
        result
    }

    /// Full cluster start: reset, provision, `cluster_init`,
    /// stores/groups, (optional) diskdb, wait healthy, collect info.
    ///
    /// # Errors
    /// Returns an error if any provisioning call fails or the cluster
    /// doesn't become healthy within the timeout.
    pub async fn start(&mut self, topo: &TopologyDescriptor) -> Result<()> {
        self.start_with_timeout(topo, Duration::from_secs(30)).await
    }

    /// Same as `start` but with a configurable health-check timeout.
    ///
    /// # Errors
    /// Returns an error if any provisioning call fails or the cluster
    /// doesn't become healthy within `timeout`.
    pub async fn start_with_timeout(&mut self, topo: &TopologyDescriptor, timeout: Duration) -> Result<()> {
        let total_start = Instant::now();

        let t = Instant::now();
        self.reset().await?;
        log_phase_time("reset", t);

        let t = Instant::now();
        let (racks, nodes) = self.provision_racks_and_nodes(topo).await?;
        log_phase_time("provision_racks_and_nodes", t);

        let t = Instant::now();
        let node_infos = self.deploy_kv_servers(&nodes, topo).await?;
        log_phase_time("deploy_kv_servers", t);

        let t = Instant::now();
        self.client.cluster_init(&nodes).await?;
        log_phase_time("cluster_init", t);

        let t = Instant::now();
        let stores = self.create_stores_and_groups(&nodes, topo).await?;
        log_phase_time("create_stores_and_groups", t);

        let t = Instant::now();
        let diskdb_instances = self.deploy_diskdb_instances(&nodes, topo).await?;
        log_phase_time("deploy_diskdb_instances", t);

        let t = Instant::now();
        self.create_disk_groups_and_disks(&nodes, topo).await?;
        log_phase_time("create_disk_groups_and_disks", t);

        let t = Instant::now();
        self.wait_healthy(&stores, timeout).await?;
        log_phase_time("wait_healthy", t);

        let t = Instant::now();
        let stores = self.collect_leader_info(stores).await?;
        log_phase_time("collect_leader_info", t);

        self.info = Some(ClusterInfo {
            racks,
            nodes: node_infos,
            stores,
            diskdb_instances,
        });

        let total = total_start.elapsed();
        let total_ms = total.as_millis();
        if total >= VERY_SLOW_THRESHOLD {
            tracing::error!(
                total_ms = total_ms,
                nodes = topo.node_count,
                stores = topo.store_count,
                "deployer start() took {total_ms}ms total (very slow)"
            );
        } else if total >= SLOW_THRESHOLD {
            tracing::warn!(
                total_ms = total_ms,
                nodes = topo.node_count,
                stores = topo.store_count,
                "deployer start() took {total_ms}ms total (slow)"
            );
        } else {
            tracing::info!(
                total_ms = total_ms,
                nodes = topo.node_count,
                stores = topo.store_count,
                "deployer start() took {total_ms}ms total"
            );
        }
        Ok(())
    }

    /// Create racks + nodes (1:1 mapping, each node on its own rack).
    async fn provision_racks_and_nodes(
        &self,
        topo: &TopologyDescriptor,
    ) -> Result<(Vec<RackId>, Vec<NodeId>)> {
        let mut racks = Vec::with_capacity(topo.node_count);
        let mut nodes = Vec::with_capacity(topo.node_count);
        for i in 0..topo.node_count {
            let rack_id = topo.rack_base + i as u64;
            let node_id = topo.node_base + i as u64;
            self.client
                .add_rack(&AddRackBody {
                    id: rack_id,
                    name: format!("rack-{rack_id}"),
                })
                .await?;
            self.client
                .add_node(
                    rack_id,
                    &NodeEntry {
                        id: node_id,
                        rack_id,
                        host: "127.0.0.1".into(),
                        ssh_port: 22,
                        ssh_user: String::new(),
                        ssh_key: None,
                        ssh_password: None,
                    },
                )
                .await?;
            racks.push(rack_id);
            nodes.push(node_id);
        }
        Ok((racks, nodes))
    }

    /// Deploy KV servers in parallel (each deploy polls /health).
    async fn deploy_kv_servers(
        &mut self,
        nodes: &[NodeId],
        topo: &TopologyDescriptor,
    ) -> Result<Vec<NodeInfo>> {
        let port_cfg = PortAllocConfig::default();
        let n = u16::try_from(nodes.len()).unwrap_or(u16::MAX);
        let rest_ports =
            port_alloc::alloc_port_range(ServicePort::KvServerMgmt, 0, n, &port_cfg).map_err(|e| {
                Error::Validation {
                    field: "port_alloc".into(),
                    message: e.to_string(),
                }
            })?;
        let rpc_ports =
            port_alloc::alloc_port_range(ServicePort::KvServerListen, 0, n, &port_cfg).map_err(|e| {
                Error::Validation {
                    field: "port_alloc".into(),
                    message: e.to_string(),
                }
            })?;
        let mut deploy_handles = Vec::with_capacity(nodes.len());
        for (i, &node_id) in nodes.iter().enumerate() {
            let rest_port = rest_ports[i];
            let rpc_port = rpc_ports[i];
            let body = DeployNodeServerBody {
                rest_port,
                rpc_port,
                binary: None,
                election_profile: Some("e2e".into()),
                ..Default::default()
            };
            let client = self.client.clone();
            deploy_handles.push(tokio::spawn(async move {
                client
                    .deploy_node_server(node_id, &body)
                    .await
                    .map(|d| (node_id, d, rest_port, rpc_port))
            }));
        }
        let mut node_infos = Vec::with_capacity(nodes.len());
        for handle in deploy_handles {
            let (node_id, deployed, rest_port, rpc_port) = handle
                .await
                .map_err(|e| Error::Config(format!("deploy task join: {e}")))??;
            node_infos.push(NodeInfo {
                id: node_id,
                rack_id: topo.rack_base + (node_id - topo.node_base),
                pid: deployed.pid,
                mgmt_url: deployed.mgmt_url,
                rpc_url: deployed.rpc_url,
                rest_port,
                rpc_port,
            });
        }
        self.deployed_node_ids = nodes.to_vec();
        Ok(node_infos)
    }

    /// Create stores + groups.
    async fn create_stores_and_groups(
        &self,
        nodes: &[NodeId],
        topo: &TopologyDescriptor,
    ) -> Result<Vec<StoreInfo>> {
        let mut stores = Vec::with_capacity(topo.store_count);
        for s in 0..topo.store_count {
            let store_id = topo.store_base + s as u64;
            let store_nodes = nodes
                .iter()
                .copied()
                .take(topo.replicas_per_group.min(nodes.len()))
                .collect::<Vec<_>>();
            self.client
                .add_store(&CreateStoreBody {
                    store_id,
                    nodes: store_nodes.clone(),
                })
                .await?;

            let mut groups = Vec::with_capacity(topo.groups_per_store);
            for g in 0..topo.groups_per_store {
                let group_id = topo.group_base + (s * topo.groups_per_store + g) as u64;
                let group_nodes = nodes
                    .iter()
                    .copied()
                    .take(topo.replicas_per_group.min(nodes.len()))
                    .collect::<Vec<_>>();
                self.client
                    .add_group(
                        store_id,
                        &CreateGroupBody {
                            group_id,
                            replica_id: 1,
                            nodes: group_nodes,
                        },
                    )
                    .await?;
                groups.push(GroupInfo {
                    group_id,
                    leader_node_id: None,
                    leader_endpoint: None,
                });
            }
            stores.push(StoreInfo {
                store_id,
                nodes: store_nodes,
                groups,
            });
        }
        Ok(stores)
    }

    /// Deploy diskdb instances (optional, based on topology).
    async fn deploy_diskdb_instances(
        &mut self,
        nodes: &[NodeId],
        topo: &TopologyDescriptor,
    ) -> Result<Vec<DiskdbInfo>> {
        let mut diskdb_instances = Vec::new();
        if topo.deploy_diskdb {
            let port_cfg = PortAllocConfig::default();
            let n = u16::try_from(nodes.len()).unwrap_or(u16::MAX);
            let listen_ports = port_alloc::alloc_port_range(ServicePort::DiskdbListen, 0, n, &port_cfg)
                .map_err(|e| Error::Validation {
                    field: "port_alloc".into(),
                    message: e.to_string(),
                })?;
            let http_ports =
                port_alloc::alloc_port_range(ServicePort::DiskdbHttp, 0, n, &port_cfg).map_err(|e| {
                    Error::Validation {
                        field: "port_alloc".into(),
                        message: e.to_string(),
                    }
                })?;
            let rpc_ports =
                port_alloc::alloc_port_range(ServicePort::DiskdbRpc, 0, n, &port_cfg).map_err(|e| {
                    Error::Validation {
                        field: "port_alloc".into(),
                        message: e.to_string(),
                    }
                })?;
            for (i, &node_id) in nodes.iter().enumerate() {
                let rpc_port = rpc_ports[i];
                let listen_port = listen_ports[i];
                let http_port = http_ports[i];
                let result = self
                    .client
                    .deploy_diskdb(
                        node_id,
                        &DeployDiskdbBody {
                            rpc_port,
                            listen_port: Some(listen_port),
                            http_port: Some(http_port),
                        },
                    )
                    .await?;
                diskdb_instances.push(DiskdbInfo {
                    node_id,
                    pid: result.pid,
                    rpc_port,
                });
            }
            self.diskdb_node_ids = nodes.to_vec();
        }
        Ok(diskdb_instances)
    }

    /// Create disk-groups + disks (optional, based on topology).
    async fn create_disk_groups_and_disks(&self, nodes: &[NodeId], topo: &TopologyDescriptor) -> Result<()> {
        if topo.disk_groups_per_node == 0 {
            return Ok(());
        }
        for &node_id in nodes {
            for dg in 0..topo.disk_groups_per_node {
                let dg_id = 1 + dg as u64;
                self.client
                    .add_disk_group(
                        node_id,
                        &AddDiskGroupBody {
                            id: dg_id,
                            name: format!("dg-{dg_id}"),
                        },
                    )
                    .await?;
                for d in 0..topo.disks_per_group {
                    let disk_id = format!("{:016x}{:016x}", node_id * 1000 + dg_id, d + 1);
                    self.client
                        .add_disk(
                            node_id,
                            dg_id,
                            &AddDiskBody {
                                disk_id,
                                disk_type: "Hdd".into(),
                                capacity_bytes: 4 * 1024 * 1024 * 1024 * 1024,
                                zone_size_bytes: 32 * 1024 * 1024 * 1024,
                                unit_size_bytes: 1024 * 1024,
                                device_path: String::new(),
                            },
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Collect leader info for each group after health check passes.
    async fn collect_leader_info(&self, mut stores: Vec<StoreInfo>) -> Result<Vec<StoreInfo>> {
        for store in &mut stores {
            for group in &mut store.groups {
                if let Ok(view) = self.client.get_group(store.store_id, group.group_id).await {
                    group.leader_node_id = view
                        .replicas
                        .iter()
                        .find(|r| r.role == crate::cluster::ReplicaRole::Leader)
                        .map(|r| r.node_id);
                }
                if let Ok(ep) = self.client.resolve_endpoint(store.store_id, group.group_id).await {
                    if !ep.rpc_url.is_empty() {
                        group.leader_endpoint = Some(ep.rpc_url);
                    }
                }
            }
        }
        Ok(stores)
    }

    /// Wait until every group has an elected leader.
    ///
    /// # Errors
    /// Returns `Error::Config` if any group doesn't elect a leader
    /// within `timeout`.
    pub async fn wait_healthy(&self, stores: &[StoreInfo], timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        for store in stores {
            for group in &store.groups {
                loop {
                    if let Ok(view) = self.client.get_group(store.store_id, group.group_id).await {
                        if view
                            .replicas
                            .iter()
                            .any(|r| r.role == crate::cluster::ReplicaRole::Leader)
                        {
                            break;
                        }
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Error::Config(format!(
                            "no leader for store {} group {} within {:?}",
                            store.store_id, group.group_id, timeout
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Ok(())
    }

    /// Stop all deployed KV servers + diskdb instances. Does NOT reset
    /// config — use `reset` for that. Idempotent.
    ///
    /// # Errors
    /// Best-effort: logs errors but does not fail.
    pub async fn stop(&mut self) {
        let total_start = Instant::now();

        // Stop diskdb + KV servers in parallel for speed.
        let t = Instant::now();
        let diskdb_count = self.diskdb_node_ids.len();
        let kv_count = self.deployed_node_ids.len();

        let mut stop_handles = Vec::new();
        let client = self.client.clone();
        let diskdb_ids = std::mem::take(&mut self.diskdb_node_ids);
        let kv_ids = std::mem::take(&mut self.deployed_node_ids);
        for &node_id in &diskdb_ids {
            let client = client.clone();
            stop_handles.push(tokio::spawn(async move {
                let _ = client.stop_diskdb(node_id).await;
            }));
        }
        for &node_id in &kv_ids {
            let client = client.clone();
            stop_handles.push(tokio::spawn(async move {
                let _ = client.stop_node_server(node_id).await;
            }));
        }
        for handle in stop_handles {
            let _ = handle.await;
        }
        log_phase_time("stop_all_servers", t);

        self.info = None;

        let total = total_start.elapsed();
        let total_ms = total.as_millis();
        if total >= VERY_SLOW_THRESHOLD {
            tracing::error!(
                total_ms = total_ms,
                diskdb_count = diskdb_count,
                kv_count = kv_count,
                "deployer stop() took {total_ms}ms total (very slow)"
            );
        } else if total >= SLOW_THRESHOLD {
            tracing::warn!(
                total_ms = total_ms,
                diskdb_count = diskdb_count,
                kv_count = kv_count,
                "deployer stop() took {total_ms}ms total (slow)"
            );
        } else {
            tracing::info!(
                total_ms = total_ms,
                diskdb_count = diskdb_count,
                kv_count = kv_count,
                "deployer stop() took {total_ms}ms total"
            );
        }
    }

    /// Full teardown: stop all servers + reset config. Idempotent.
    ///
    /// # Errors
    /// Best-effort on stop; surfaces reset errors.
    pub async fn teardown(&mut self) -> Result<()> {
        let total_start = Instant::now();
        self.stop().await;

        let t = Instant::now();
        self.reset().await?;
        log_phase_time("teardown_reset", t);

        let total = total_start.elapsed();
        let total_ms = total.as_millis();
        if total >= VERY_SLOW_THRESHOLD {
            tracing::error!("deployer teardown() took {total_ms}ms total (very slow)");
        } else if total >= SLOW_THRESHOLD {
            tracing::warn!("deployer teardown() took {total_ms}ms total (slow)");
        } else {
            tracing::info!("deployer teardown() took {total_ms}ms total");
        }
        Ok(())
    }
}

impl Drop for CrowdbClusterDeployer {
    fn drop(&mut self) {
        if !self.deployed_node_ids.is_empty() {
            tracing::warn!(
                "CrowdbClusterDeployer dropped without stop() — {} nodes still deployed",
                self.deployed_node_ids.len()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_topology_defaults() {
        let t = simple();
        assert_eq!(t.node_count, 3);
        assert_eq!(t.store_count, 1);
        assert_eq!(t.groups_per_store, 1);
        assert!(!t.deploy_diskdb);
    }

    #[test]
    fn complex_topology_defaults() {
        let t = complex();
        assert_eq!(t.node_count, 8);
        assert_eq!(t.store_count, 2);
        assert_eq!(t.groups_per_store, 2);
    }
}
