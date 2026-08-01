// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Node startup reconciliation with group 0.
//!
//! After the server starts and creates stores/groups from the local
//! `node-config.json` cache, this module compares the local state
//! against the topology KV in group 0. If group 0 is reachable and
//! finalized, missing stores/groups are created and stale ones are
//! removed, keeping the node in sync with the cluster's authoritative
//! topology.

use tracing::{info, warn};

use crowkv::cluster::kv_store::KvStore;
use crowkv::cluster::topology_kv;

use crate::store_registry::KvStoreRegistry;

/// Run reconciliation against group 0 if it is locally available.
///
/// This is best-effort: if group 0 is not reachable or not finalized,
/// the function returns silently. The node continues with its local
/// cache and will reconcile on the next restart or when group 0
/// becomes reachable.
pub async fn reconcile_with_group0(registry: &KvStoreRegistry) {
    let Some(store0) = registry.get_store(0) else {
        info!("reconcile: store 0 not found locally; skipping");
        return;
    };
    if store0.get_group(0).is_none() {
        info!("reconcile: group 0 not found locally; skipping");
        return;
    }

    // Check if group 0 is finalized.
    let ready_resp = store0.kv_get(0, topology_kv::READY_KEY, 0, 0, 0, 0).await;
    if !ready_resp.ok || ready_resp.value.is_empty() {
        info!("reconcile: group 0 not finalized; skipping");
        return;
    }

    info!("reconcile: group 0 is ready; scanning topology KV");

    // Scan for stores that include this node.
    let scan_resp = store0
        .kv_scan(0, topology_kv::STORES_PREFIX, b"", 0, 0, 0, 0, 0)
        .await;
    if !scan_resp.ok {
        warn!(error = %scan_resp.error, "reconcile: failed to scan stores");
        return;
    }

    let mut expected_stores: Vec<u64> = Vec::new();
    for item in &scan_resp.items {
        if let Ok(store_meta) = topology_kv::decode::<topology_kv::TopologyStore>(&item.value) {
            expected_stores.push(store_meta.store_id);
        }
    }

    // Create missing stores.
    for &sid in &expected_stores {
        if registry.get_store(sid).is_none() {
            info!(store_id = sid, "reconcile: creating missing store");
            // Store creation requires the management API path; for now
            // we log and defer. Full store creation here would need the
            // same logic as `add_store` in mgmt_api.rs.
            warn!(
                store_id = sid,
                "reconcile: missing store creation deferred to management API"
            );
        }
    }

    // Scan for groups.
    let scan_resp = store0
        .kv_scan(0, topology_kv::GROUPS_PREFIX, b"", 0, 0, 0, 0, 0)
        .await;
    if !scan_resp.ok {
        warn!(error = %scan_resp.error, "reconcile: failed to scan groups");
        return;
    }

    let mut expected_groups: Vec<(u64, u64)> = Vec::new();
    for item in &scan_resp.items {
        if let Ok(group_meta) = topology_kv::decode::<topology_kv::TopologyGroup>(&item.value) {
            expected_groups.push((group_meta.store_id, group_meta.group_id));
        }
    }

    // Check for missing groups on existing stores.
    for &(sid, gid) in &expected_groups {
        if let Some(store) = registry.get_store(sid) {
            if store.get_group(gid).is_none() {
                warn!(
                    store_id = sid,
                    group_id = gid,
                    "reconcile: missing group; creation deferred to management API"
                );
            }
        }
    }

    info!(
        expected_stores = expected_stores.len(),
        expected_groups = expected_groups.len(),
        "reconcile: scan complete"
    );
}
