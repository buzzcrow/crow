// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Node startup reconciliation with group 0.
//!
//! After restore mode loads every local store/group from disk (replaying
//! the WAL and applying `node-config.json` membership — which already
//! wires remote replicas for the common case), this module compares the
//! local state against the KV-cluster topology records in group 0:
//!
//! - **Fallback wiring** — if a local group came up with no remote
//!   replicas (`node-config.json` was missing or stale for it), seed its
//!   remotes from group 0's `/kv/replica/` records via a group rebuild.
//! - **Verification** — for groups that already have remotes, log any
//!   peer present in group 0 but not wired locally. The live membership
//!   is not forcibly overwritten (it may be legitimately ahead of group
//!   0 during an in-flight reconfiguration).
//!
//! Best-effort: if group 0 is not reachable or has no `/kv/replica/`
//! records, the function returns silently. The node continues with its
//! local state and retries on the next restart.

use std::collections::HashMap;

use tracing::{info, warn};

use crow_kv::cluster::kv_store::KvStore;

use crate::group_rebuild::rebuild_group_with_new_remotes;
use crate::store_registry::KvStoreRegistry;

/// Peers for one group, keyed by `(store_id, group_id)`.
type PeersByGroup = HashMap<(u64, u64), Vec<(u64, String, bool)>>;

/// Run reconciliation against group 0 if it is locally available.
pub async fn reconcile_with_group0(registry: &KvStoreRegistry) {
    let Some(store0) = registry.get_store(0) else {
        info!("reconcile: store 0 not found locally; skipping");
        return;
    };
    if store0.get_group(0).is_none() {
        info!("reconcile: group 0 not found locally; skipping");
        return;
    }

    let Some(replicas) = scan_replica_records(&store0).await else {
        return;
    };
    if replicas.is_empty() {
        info!("reconcile: group 0 has no /kv/replica/ records; skipping");
        return;
    }

    let plan = plan_reconcile(&replicas, registry);
    execute_reconcile(&plan, registry);
}

/// A decoded `/kv/replica/` record.
#[derive(Debug, Clone)]
pub struct ReplicaRecord {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    pub endpoint: String,
    pub voting: bool,
}

/// One group's reconcile decision: seed remotes (fallback) or log
/// mismatches (verify).
#[derive(Debug, Default)]
pub struct ReconcileAction {
    pub store_id: u64,
    pub group_id: u64,
    /// Remotes to seed via rebuild (only set when the group currently
    /// has no remotes).
    pub seed_remotes: Vec<(u64, String, bool)>,
    /// Peers in group 0 not wired locally (verify-only, logged as warn).
    pub mismatches: Vec<(u64, String)>,
}

/// Pure decision logic: given decoded group-0 replica records and the
/// local registry, compute what to do for each local group. No side
/// effects — the caller executes the plan via [`execute_reconcile`].
pub fn plan_reconcile(records: &[ReplicaRecord], registry: &KvStoreRegistry) -> Vec<ReconcileAction> {
    // Group records by (store_id, group_id) -> Vec<(replica_id, endpoint, voting)>.
    let mut by_group: PeersByGroup = HashMap::new();
    for r in records {
        by_group.entry((r.store_id, r.group_id)).or_default().push((
            r.replica_id,
            r.endpoint.clone(),
            r.voting,
        ));
    }

    let mut actions = Vec::new();
    for ((sid, gid), peers) in &by_group {
        let Some(store) = registry.get_store(*sid) else {
            continue;
        };
        let Some(group) = store.get_group(*gid) else {
            continue;
        };
        let local_id = group.local_replica().id;
        let existing = group.remote_replica_info();

        let peers_minus_self: Vec<(u64, String, bool)> = peers
            .iter()
            .filter(|(rid, _, _)| *rid != local_id)
            .cloned()
            .collect();

        if existing.is_empty() {
            // Fallback: node-config.json was missing/stale. Seed from group 0.
            if peers_minus_self.is_empty() {
                continue;
            }
            actions.push(ReconcileAction {
                store_id: *sid,
                group_id: *gid,
                seed_remotes: peers_minus_self,
                mismatches: Vec::new(),
            });
        } else {
            // Verify: collect peers in group 0 not present locally.
            let mismatches: Vec<(u64, String)> = peers_minus_self
                .iter()
                .filter(|(rid, _, _)| !existing.iter().any(|(eid, _, _)| eid == rid))
                .map(|(rid, ep, _)| (*rid, ep.clone()))
                .collect();
            if !mismatches.is_empty() {
                actions.push(ReconcileAction {
                    store_id: *sid,
                    group_id: *gid,
                    seed_remotes: Vec::new(),
                    mismatches,
                });
            }
        }
    }
    actions
}

/// Execute a reconcile plan: rebuild groups for seed actions, log warn
/// for mismatch actions.
fn execute_reconcile(plan: &[ReconcileAction], registry: &KvStoreRegistry) {
    let mut seeded = 0usize;
    let mut mismatches = 0usize;
    for action in plan {
        if !action.seed_remotes.is_empty() {
            if let Some(store) = registry.get_store(action.store_id) {
                if let Some(group) = store.get_group(action.group_id) {
                    let new_group = rebuild_group_with_new_remotes(&group, &action.seed_remotes);
                    store.add_group(new_group);
                    seeded += 1;
                    info!(
                        store_id = action.store_id,
                        group_id = action.group_id,
                        peer_count = action.seed_remotes.len(),
                        "reconcile: seeded remotes from group 0 (node-config.json was empty)"
                    );
                }
            }
        }
        for (rid, ep) in &action.mismatches {
            mismatches += 1;
            warn!(
                store_id = action.store_id,
                group_id = action.group_id,
                replica_id = rid,
                endpoint = %ep,
                "reconcile: group 0 has peer not wired locally"
            );
        }
    }
    info!(
        actions = plan.len(),
        seeded, mismatches, "reconcile: scan complete"
    );
}

/// Prefix-scan group 0 for `/kv/replica/` records and decode each value
/// as a `ReplicaValue`. Returns `None` if the scan fails (group 0
/// unreachable / not yet led); returns `Some(vec)` (possibly empty) on
/// success.
async fn scan_replica_records(
    store0: &crow_kv::cluster::px_kv_store::PxKvStore,
) -> Option<Vec<ReplicaRecord>> {
    let prefix = b"/kv/replica/";
    let resp = store0
        .kv_scan(0, prefix, b"", b"", 0, 0, 0, false, false, 0, 0, 0)
        .await;
    if !resp.ok {
        warn!(error = %resp.error, "reconcile: failed to scan /kv/replica/");
        return None;
    }
    let mut out = Vec::new();
    for item in &resp.items {
        match serde_json::from_slice::<crow_protocol::common::ReplicaValue>(&item.value) {
            Ok(v) => out.push(ReplicaRecord {
                store_id: v.store_id,
                group_id: v.group_id,
                replica_id: v.replica_id,
                endpoint: v.endpoint,
                voting: v.voting,
            }),
            Err(e) => {
                warn!(error = %e, "reconcile: failed to decode ReplicaValue; skipping record");
            }
        }
    }
    Some(out)
}
