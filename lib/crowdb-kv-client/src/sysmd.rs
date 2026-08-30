// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc)]

//! [`CrowdbSysmdClient`] — unified facade for group-0 system metadata.
//!
//! Group 0 (store 0, group 0) holds all cluster system metadata: the
//! hardware hierarchy (rack/node/disk-group/disk), the KV-cluster
//! topology (store/group/replica), and the service instance registry
//! (diskdb/kv-server/diskio/chunkdb registrations).
//!
//! Historically these were three separate client types
//! ([`HardwareClient`], [`KVClusterMetaClient`],
//! [`ServiceRegistryClient`]), each wrapping the same
//! [`CrowdbKvClient`] pinned to group 0. `CrowdbSysmdClient` brings
//! the `sysmd` concept — defined in
//! `doc/design/kv/design-crowdb-kv-group0.md` — into the client layer
//! as a single facade with domain methods (`add_rack`, `list_nodes`,
//! `add_store`, …). New code should use this type; the three
//! sub-clients remain public for existing callers until they migrate.

use std::sync::Arc;

use crowdb_protocol::common::{
    DiskGroupUsageSummary, DiskId, HostedGroup, HwStatus, InstanceValue, ServiceExtra,
};
use crowdb_protocol::common::{GroupValue, NodeValue, RackValue, ReplicaValue, StoreValue};
use crowdb_protocol::common_type::{DiskGroupId, GroupId, InstanceId, NodeId, RackId, ReplicaId, StoreId};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskValue};
use crowdb_protocol::sysdata::{DiskGroupEntry, DiskdbOwnerEntry, KVGroupBindEntry};

use crate::hardware::{DiskRecord, HardwareCapacitySummary, HardwareClient};
use crate::kv_cluster::KVClusterMetaClient;
use crate::service_registry::ServiceRegistryClient;
use crate::{CrowdbKvClient, Result};

/// Unified client for group-0 system metadata.
///
/// Wraps a single `Arc<CrowdbKvClient>` (seeded with a group-0 leader
/// endpoint) and exposes domain methods across the three sysmd
/// sub-areas: hardware hierarchy, KV-cluster topology, and service
/// registry. Each method delegates to the corresponding sub-client.
#[derive(Clone)]
pub struct CrowdbSysmdClient {
    hw: HardwareClient,
    meta: KVClusterMetaClient,
    svc: ServiceRegistryClient,
}

impl CrowdbSysmdClient {
    /// Build a `CrowdbSysmdClient` from an owned `CrowdbKvClient`.
    /// The client must have its topology seeded with a group-0 leader
    /// endpoint (via `seed_leader(0, 0, endpoint)` or `/topology`
    /// discovery).
    #[must_use]
    pub fn new(kv: CrowdbKvClient) -> Self {
        let shared = Arc::new(kv);
        Self::from_shared(shared)
    }

    /// Build a `CrowdbSysmdClient` from an already-shared
    /// `CrowdbKvClient`. All three sub-clients share the same `Arc`,
    /// so topology cache and connection pool are reused.
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self {
            hw: HardwareClient::from_shared(Arc::clone(&kv)),
            meta: KVClusterMetaClient::from_shared(Arc::clone(&kv)),
            svc: ServiceRegistryClient::from_shared(kv),
        }
    }

    /// Access the underlying `CrowdbKvClient` (data-plane + topology
    /// cache shared by all sub-clients).
    #[must_use]
    pub fn kv(&self) -> &CrowdbKvClient {
        self.hw.kv()
    }

    /// Access the shared `Arc<CrowdbKvClient>`.
    #[must_use]
    pub fn shared_kv(&self) -> Arc<CrowdbKvClient> {
        self.hw.shared_kv()
    }

    // ── hardware: rack ──────────────────────────────────────────

    pub async fn add_rack(&self, rack_id: RackId, value: &RackValue) -> Result<()> {
        self.hw.add_rack(rack_id, value).await
    }
    pub async fn get_rack(&self, rack_id: RackId) -> Result<Option<RackValue>> {
        self.hw.get_rack(rack_id).await
    }
    pub async fn list_racks(&self) -> Result<Vec<(RackId, RackValue)>> {
        self.hw.list_racks().await
    }
    pub async fn remove_rack(&self, rack_id: RackId) -> Result<()> {
        self.hw.remove_rack(rack_id).await
    }
    pub async fn remove_rack_cascade(&self, rack_id: RackId) -> Result<()> {
        self.hw.remove_rack_cascade(rack_id).await
    }
    pub async fn set_rack_status(&self, rack_id: RackId, status: HwStatus) -> Result<()> {
        self.hw.set_rack_status(rack_id, status).await
    }

    // ── hardware: node ──────────────────────────────────────────

    pub async fn add_node(&self, rack_id: RackId, node_id: NodeId, value: &NodeValue) -> Result<()> {
        self.hw.add_node(rack_id, node_id, value).await
    }
    pub async fn get_node(&self, rack_id: RackId, node_id: NodeId) -> Result<Option<NodeValue>> {
        self.hw.get_node(rack_id, node_id).await
    }
    pub async fn list_nodes(&self) -> Result<Vec<(RackId, NodeId, NodeValue)>> {
        self.hw.list_nodes().await
    }
    pub async fn list_nodes_in_rack(&self, rack_id: RackId) -> Result<Vec<(NodeId, NodeValue)>> {
        self.hw.list_nodes_in_rack(rack_id).await
    }
    pub async fn remove_node(&self, rack_id: RackId, node_id: NodeId) -> Result<()> {
        self.hw.remove_node(rack_id, node_id).await
    }
    pub async fn remove_node_cascade(&self, rack_id: RackId, node_id: NodeId) -> Result<()> {
        self.hw.remove_node_cascade(rack_id, node_id).await
    }
    pub async fn set_node_status(&self, rack_id: RackId, node_id: NodeId, status: HwStatus) -> Result<()> {
        self.hw.set_node_status(rack_id, node_id, status).await
    }

    // ── hardware: disk-group ────────────────────────────────────

    pub async fn add_disk_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        value: &DiskGroupValue,
    ) -> Result<()> {
        self.hw.add_disk_group(rack_id, node_id, dg_id, value).await
    }
    pub async fn get_disk_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<DiskGroupEntry>> {
        self.hw.get_disk_group(rack_id, node_id, dg_id).await
    }
    pub async fn list_disk_groups(&self) -> Result<Vec<DiskGroupEntry>> {
        self.hw.list_disk_groups().await
    }
    pub async fn list_disk_groups_on_node(
        &self,
        rack_id: RackId,
        node_id: NodeId,
    ) -> Result<Vec<DiskGroupEntry>> {
        self.hw.list_disk_groups_on_node(rack_id, node_id).await
    }
    pub async fn remove_disk_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<()> {
        self.hw.remove_disk_group(rack_id, node_id, dg_id).await
    }
    pub async fn remove_disk_group_cascade(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<()> {
        self.hw.remove_disk_group_cascade(rack_id, node_id, dg_id).await
    }
    pub async fn set_disk_group_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        status: HwStatus,
    ) -> Result<()> {
        self.hw
            .set_disk_group_status(rack_id, node_id, dg_id, status)
            .await
    }

    // ── hardware: disk ──────────────────────────────────────────

    pub async fn add_disk(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
        value: &DiskValue,
    ) -> Result<()> {
        self.hw.add_disk(rack_id, node_id, dg_id, disk_id, value).await
    }
    pub async fn get_disk(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
    ) -> Result<Option<DiskValue>> {
        self.hw.get_disk(rack_id, node_id, dg_id, disk_id).await
    }
    pub async fn list_disks_in_group(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Vec<(DiskId, DiskValue)>> {
        self.hw.list_disks_in_group(rack_id, node_id, dg_id).await
    }
    pub async fn list_all_disks(&self) -> Result<Vec<DiskRecord>> {
        self.hw.list_all_disks().await
    }
    pub async fn remove_disk(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
    ) -> Result<()> {
        self.hw.remove_disk(rack_id, node_id, dg_id, disk_id).await
    }
    pub async fn remove_disk_cascade(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
    ) -> Result<()> {
        self.hw
            .remove_disk_cascade(rack_id, node_id, dg_id, disk_id)
            .await
    }
    pub async fn set_disk_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        disk_id: &DiskId,
        status: HwStatus,
    ) -> Result<()> {
        self.hw
            .set_disk_status(rack_id, node_id, dg_id, disk_id, status)
            .await
    }

    // ── hardware: owner/bind maps + capacity ────────────────────

    pub async fn set_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        instance_id: u64,
        lease_expiry_ms: u64,
    ) -> Result<()> {
        self.hw
            .set_owner(rack_id, node_id, dg_id, instance_id, lease_expiry_ms)
            .await
    }
    pub async fn get_owner(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<DiskdbOwnerEntry>> {
        self.hw.get_owner(rack_id, node_id, dg_id).await
    }
    pub async fn list_owners(&self) -> Result<Vec<DiskdbOwnerEntry>> {
        self.hw.list_owners().await
    }
    pub async fn remove_owner(&self, rack_id: RackId, node_id: NodeId, dg_id: DiskGroupId) -> Result<()> {
        self.hw.remove_owner(rack_id, node_id, dg_id).await
    }
    pub async fn set_bind(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
        store_id: u64,
        group_id: u64,
    ) -> Result<()> {
        self.hw
            .set_bind(rack_id, node_id, dg_id, store_id, group_id)
            .await
    }
    pub async fn get_bind(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        dg_id: DiskGroupId,
    ) -> Result<Option<KVGroupBindEntry>> {
        self.hw.get_bind(rack_id, node_id, dg_id).await
    }
    pub async fn list_binds(&self) -> Result<Vec<KVGroupBindEntry>> {
        self.hw.list_binds().await
    }
    pub async fn remove_bind(&self, rack_id: RackId, node_id: NodeId, dg_id: DiskGroupId) -> Result<()> {
        self.hw.remove_bind(rack_id, node_id, dg_id).await
    }
    pub async fn remove_disk_group_usage(&self, dg_id: DiskGroupId) -> Result<()> {
        self.hw.remove_disk_group_usage(dg_id).await
    }
    pub async fn capacity_summary(&self) -> Result<HardwareCapacitySummary> {
        self.hw.capacity_summary().await
    }

    // ── KV-cluster topology: store ──────────────────────────────

    pub async fn add_store(&self, store_id: StoreId, node_ids: &[u64]) -> Result<()> {
        self.meta.add_store(store_id, node_ids).await
    }
    pub async fn get_store(&self, store_id: StoreId) -> Result<Option<StoreValue>> {
        self.meta.get_store(store_id).await
    }
    pub async fn list_stores(&self) -> Result<Vec<StoreValue>> {
        self.meta.list_stores().await
    }
    pub async fn remove_store(&self, store_id: StoreId) -> Result<()> {
        self.meta.remove_store(store_id).await
    }

    // ── KV-cluster topology: group ──────────────────────────────

    pub async fn add_group(&self, store_id: StoreId, group_id: GroupId) -> Result<()> {
        self.meta.add_group(store_id, group_id).await
    }
    pub async fn get_group(&self, store_id: StoreId, group_id: GroupId) -> Result<Option<GroupValue>> {
        self.meta.get_group(store_id, group_id).await
    }
    pub async fn list_groups_in_store(&self, store_id: StoreId) -> Result<Vec<GroupValue>> {
        self.meta.list_groups_in_store(store_id).await
    }
    pub async fn remove_group(&self, store_id: StoreId, group_id: GroupId) -> Result<()> {
        self.meta.remove_group(store_id, group_id).await
    }

    // ── KV-cluster topology: replica ────────────────────────────

    pub async fn add_replica(&self, value: &ReplicaValue) -> Result<()> {
        self.meta.add_replica(value).await
    }
    pub async fn get_replica(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        replica_id: ReplicaId,
    ) -> Result<Option<ReplicaValue>> {
        self.meta.get_replica(store_id, group_id, replica_id).await
    }
    pub async fn list_replicas_in_group(
        &self,
        store_id: StoreId,
        group_id: GroupId,
    ) -> Result<Vec<ReplicaValue>> {
        self.meta.list_replicas_in_group(store_id, group_id).await
    }
    pub async fn remove_replica(
        &self,
        store_id: StoreId,
        group_id: GroupId,
        replica_id: ReplicaId,
    ) -> Result<()> {
        self.meta.remove_replica(store_id, group_id, replica_id).await
    }

    // ── service registry ────────────────────────────────────────

    pub async fn register_service(
        &self,
        service: &str,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        extra: &ServiceExtra,
    ) -> Result<()> {
        self.svc.register(service, instance_id, rpc_endpoint, extra).await
    }
    pub async fn unregister_service(&self, service: &str, instance_id: InstanceId) -> Result<()> {
        self.svc.unregister(service, instance_id).await
    }
    pub async fn read_service_instances(&self, service: &str) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.svc.read_all_instances(service).await
    }

    // ── service registry: diskdb convenience ────────────────────

    pub async fn register_diskdb(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        owned_dg_ids: &[u64],
        group_usages: &[DiskGroupUsageSummary],
    ) -> Result<()> {
        self.svc
            .register_diskdb(instance_id, rpc_endpoint, owned_dg_ids, group_usages)
            .await
    }
    pub async fn read_all_diskdb_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.svc.read_all_diskdb_instances().await
    }

    // ── service registry: kv-server convenience ─────────────────

    pub async fn register_kv_server(
        &self,
        instance_id: InstanceId,
        rpc_endpoint: &str,
        hosted_stores: &[u64],
        hosted_groups: &[HostedGroup],
        health: &str,
        data_root: &str,
    ) -> Result<()> {
        self.svc
            .register_kv_server(
                instance_id,
                rpc_endpoint,
                hosted_stores,
                hosted_groups,
                health,
                data_root,
            )
            .await
    }
    pub async fn read_all_kv_server_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.svc.read_all_kv_server_instances().await
    }

    // ── service registry: chunkdb convenience ───────────────────

    pub async fn register_chunkdb(&self, instance_id: InstanceId, rpc_endpoint: &str) -> Result<()> {
        self.svc.register_chunkdb(instance_id, rpc_endpoint).await
    }
    pub async fn read_all_chunkdb_instances(&self) -> Result<Vec<(InstanceId, InstanceValue)>> {
        self.svc.read_all_chunkdb_instances().await
    }
}
