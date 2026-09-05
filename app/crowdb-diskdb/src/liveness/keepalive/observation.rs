use super::{warn, DiskGroupId, HashMap, HwStatus, KeepAlive, ObservedDiskGroup, Ordering};

impl KeepAlive {
    pub(super) async fn observe_group0(&self) -> Option<Vec<ObservedDiskGroup>> {
        let owners = match self.hw.list_owners().await {
            Ok(owners) => owners,
            Err(error) => {
                warn!(%error, "sync: read owner map failed");
                self.record_observation_failure();
                return None;
            }
        };
        let binds = match self.hw.list_binds().await {
            Ok(binds) => binds,
            Err(error) => {
                warn!(%error, "sync: read bind map failed");
                self.record_observation_failure();
                return None;
            }
        };
        let bind_map: HashMap<DiskGroupId, (u64, u64)> = binds
            .into_iter()
            .map(|b| (b.dg_id, (b.store_id, b.group_id)))
            .collect();
        let owned: Vec<_> = owners
            .into_iter()
            .filter(|owner| owner.instance_id == self.container.instance_id)
            .collect();

        let mut observed = Vec::with_capacity(owned.len());
        for owner in owned {
            let Some(&bind) = bind_map.get(&owner.dg_id) else {
                warn!(dg_id = owner.dg_id, "sync: owned disk-group has no bind");
                self.record_observation_failure();
                return None;
            };
            observed.push(self.observe_disk_group(owner, bind).await?);
        }
        Some(observed)
    }

    pub(super) async fn observe_disk_group(
        &self,
        owner: crowdb_protocol::DiskdbOwnerEntry,
        bind: (u64, u64),
    ) -> Option<ObservedDiskGroup> {
        let node = match self.hw.get_node(owner.rack_id, owner.node_id).await {
            Ok(Some(node)) => node,
            Ok(None) => {
                warn!(
                    rack_id = owner.rack_id,
                    node_id = owner.node_id,
                    "sync: owned node is absent"
                );
                self.record_observation_failure();
                return None;
            }
            Err(error) => {
                warn!(%error, rack_id = owner.rack_id, node_id = owner.node_id, "sync: read node failed");
                self.record_observation_failure();
                return None;
            }
        };
        let group = match self
            .hw
            .get_disk_group(owner.rack_id, owner.node_id, owner.dg_id)
            .await
        {
            Ok(Some(group)) => group,
            Ok(None) => {
                warn!(dg_id = owner.dg_id, "sync: owned disk-group record is absent");
                self.record_observation_failure();
                return None;
            }
            Err(error) => {
                warn!(%error, dg_id = owner.dg_id, "sync: read disk-group failed");
                self.record_observation_failure();
                return None;
            }
        };
        let disks = match self
            .hw
            .list_disks_in_group(owner.rack_id, owner.node_id, owner.dg_id)
            .await
        {
            Ok(disks) => disks,
            Err(error) => {
                warn!(%error, dg_id = owner.dg_id, "sync: list disks failed");
                self.record_observation_failure();
                return None;
            }
        };
        let Ok(node_status) = HwStatus::try_from(node.status) else {
            warn!(status = node.status, "sync: invalid node status");
            self.record_observation_failure();
            return None;
        };
        let Ok(group_status) = HwStatus::try_from(group.value.status) else {
            warn!(status = group.value.status, "sync: invalid disk-group status");
            self.record_observation_failure();
            return None;
        };
        Some(ObservedDiskGroup {
            owner,
            bind,
            node_status,
            group_status,
            disks,
        })
    }

    pub(super) fn record_observation_failure(&self) {
        let count = self.missed_count.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.config.miss_threshold {
            self.container.enter_degraded_mode();
        }
    }
}
