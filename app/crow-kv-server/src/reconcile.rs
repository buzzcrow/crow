// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Node startup reconciliation with group 0.
//!
//! After the server starts and creates stores/groups from the local
//! `node-config.json` cache, this module compares the local state
//! against the KV-cluster topology records in group 0. If group 0 is
//! reachable, missing stores/groups are noted and stale ones are
//! reported, keeping the node in sync with the cluster's authoritative
//! topology.
//!
//! The old `/topology/ready` readiness flag is gone — an empty
//! `/kv/store/` prefix scan means "not yet initialized" and the node
//! just skips reconciliation silently.

use tracing::{info, warn};

use crow_kv::cluster::kv_store::KvStore;

use crate::store_registry::KvStoreRegistry;

/// Run reconciliation against group 0 if it is locally available.
///
/// This is best-effort: if group 0 is not reachable or has no
/// `/kv/store/` records, the function returns silently. The node
/// continues with its local cache and will reconcile on the next
/// restart or when group 0 becomes reachable.
pub async fn reconcile_with_group0(registry: &KvStoreRegistry) {
    let Some(store0) = registry.get_store(0) else {
        info!("reconcile: store 0 not found locally; skipping");
        return;
    };
    if store0.get_group(0).is_none() {
        info!("reconcile: group 0 not found locally; skipping");
        return;
    }

    // Scan for stores under the /kv/store/ text-path prefix.
    let stores_prefix = b"/kv/store/";
    let scan_resp = store0
        .kv_scan(0, stores_prefix, b"", b"", 0, 0, 0, false, false, 0, 0, 0)
        .await;
    if !scan_resp.ok {
        warn!(error = %scan_resp.error, "reconcile: failed to scan /kv/store/");
        return;
    }

    if scan_resp.items.is_empty() {
        info!("reconcile: group 0 has no /kv/store/ records; not yet initialized, skipping");
        return;
    }

    info!("reconcile: group 0 has topology records; scanning");

    let mut expected_stores: Vec<u64> = Vec::new();
    for item in &scan_resp.items {
        if let Ok(store_meta) = serde_json::from_slice::<crow_protocol::common::StoreValue>(&item.value) {
            expected_stores.push(store_meta.store_id);
        }
    }

    // Create missing stores.
    for &sid in &expected_stores {
        if registry.get_store(sid).is_none() {
            warn!(
                store_id = sid,
                "reconcile: missing store creation deferred to management API"
            );
        }
    }

    // Scan for groups under the /kv/group/ text-path prefix.
    let groups_prefix = b"/kv/group/";
    let scan_resp = store0
        .kv_scan(0, groups_prefix, b"", b"", 0, 0, 0, false, false, 0, 0, 0)
        .await;
    if !scan_resp.ok {
        warn!(error = %scan_resp.error, "reconcile: failed to scan /kv/group/");
        return;
    }

    let mut expected_groups: Vec<(u64, u64)> = Vec::new();
    for item in &scan_resp.items {
        if let Ok(group_meta) = serde_json::from_slice::<crow_protocol::common::GroupValue>(&item.value) {
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
