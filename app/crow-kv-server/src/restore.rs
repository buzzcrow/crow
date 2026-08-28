// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Restore mode: rebuild stores/groups from local disk on restart.
//!
//! When group 0 is present on disk (`<wal_root>/store0/group0`), the
//! server boots in restore mode: it scans `<wal_root>` for every
//! `store{S}/group{G}` directory, loads each via
//! [`crate::startup::create_group_with_wal`] (which replays the WAL,
//! opens the crow-tree engine, and applies the persisted membership
//! from `node-config.json` — including remote-replica endpoints), and
//! starts the stores. No `--stores`/`--groups` CLI args are needed;
//! local disk is the source of truth for which stores/groups this node
//! hosts. Group 0 is consulted afterward by
//! [`crate::reconcile::reconcile_with_group0`] as verification and as
//! the fallback when `node-config.json` is missing/stale for a group.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crow_kv::cluster::kv_server::KvServer;
use crow_kv::cluster::local_replica::PxLocalReplicaRole;
use crow_kv::cluster::node_config::NodeConfigStore;
use crow_kv::cluster::px_kv_store::PxKvStore;

use crate::mgmt::persisted_port_for_store;
use crate::startup::create_group_with_wal;
use crate::store_registry::KvStoreRegistry;

/// A local `(store_id, group_id)` pair discovered by scanning `waldata`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalGroup {
    pub store_id: u64,
    pub group_id: u64,
}

/// Scan `<wal_root>` for `store{S}/group{G}` directories.
///
/// Returns the list sorted by `(store_id, group_id)`. Empty if
/// `wal_root` does not exist or contains no matching entries.
///
/// # Errors
/// Returns an IO error if `wal_root` cannot be read.
pub async fn scan_local_groups(wal_root: &Path) -> io::Result<Vec<LocalGroup>> {
    let mut out: Vec<LocalGroup> = Vec::new();
    let mut entries = match tokio::fs::read_dir(wal_root).await {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    while let Some(store_entry) = entries.next_entry().await? {
        let store_name = store_entry.file_name();
        let store_name = store_name.to_string_lossy();
        let Some(store_id) = parse_store_dir(&store_name) else {
            debug!(entry = %store_name, "scan: skip non-store entry in waldata");
            continue;
        };
        if !store_entry.file_type().await?.is_dir() {
            continue;
        }
        let mut group_entries = tokio::fs::read_dir(store_entry.path()).await?;
        while let Some(group_entry) = group_entries.next_entry().await? {
            let group_name = group_entry.file_name();
            let group_name = group_name.to_string_lossy();
            let Some(group_id) = parse_group_dir(&group_name) else {
                debug!(store_id, entry = %group_name, "scan: skip non-group entry");
                continue;
            };
            if !group_entry.file_type().await?.is_dir() {
                continue;
            }
            out.push(LocalGroup { store_id, group_id });
        }
    }
    out.sort_by_key(|g| (g.store_id, g.group_id));
    Ok(out)
}

/// True if `<wal_root>/store0/group0` exists (group 0 is on disk).
#[must_use]
pub fn group0_exists(wal_root: &Path) -> bool {
    wal_root.join("store0").join("group0").exists()
}

/// Load every local store/group from disk and start them.
///
/// Groups `local_groups` by `store_id`, creates each `PxKvStore`
/// (port from the `--ports` pool, else `persisted_port_for_store`,
/// else OS-assigned 0), then calls [`create_group_with_wal`] per group
/// with [`PxLocalReplicaRole::Follower`] (the election driver
/// promotes), `add_group`, `store.start()`, and registers the store.
/// Reuses `create_and_start_stores`' skip-on-error policy: a failed
/// group is logged and skipped, the store still starts with its other
/// groups.
///
/// The local `replica_id` for each group is read from `node-config.json`
/// (where it was persisted when the group was first created). If the
/// entry is missing (e.g. first boot after migrate, or stale config),
/// falls back to the `replica_id` CLI arg.
///
/// # Panics
/// Panics if the computed bind address `"0.0.0.0:{port}"` fails to
/// parse as a `SocketAddr` (only possible if `port > 65535`, which
/// the port pool / persisted-port logic prevents).
pub async fn load_local_groups(
    local_groups: &[LocalGroup],
    replica_id: u64,
    registry: &Arc<KvStoreRegistry>,
) {
    // Group by store_id, preserving scan (sorted) order.
    let mut by_store: Vec<(u64, Vec<u64>)> = Vec::new();
    for lg in local_groups {
        if let Some((_, groups)) = by_store.iter_mut().find(|(s, _)| *s == lg.store_id) {
            groups.push(lg.group_id);
        } else {
            by_store.push((lg.store_id, vec![lg.group_id]));
        }
    }

    for (store_id, group_ids) in by_store {
        let port = persisted_port_for_store(&registry.config.config_root, store_id)
            .await
            .unwrap_or_else(|| registry.next_port().unwrap_or(0));
        let addr: SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        debug!(store_id, bind_addr = %addr, "restore: creating PxKvStore");
        let mut store = PxKvStore::new(store_id, addr);
        store.rpc_workers = registry.rpc_workers;
        if let Some(ref mr) = registry.metrics_registry {
            store.set_metrics_registry(Arc::clone(mr));
        }
        store.set_scan_byte_budget(registry.config.server.scan_byte_budget);
        store.set_peer_pool_size(registry.config.server.peer_pool_size);
        store.set_enable_nagle(registry.config.server.enable_nagle);
        store.set_quickack(registry.config.server.quickack);
        store.set_event_write(registry.config.server.event_write);
        store.set_send_queue_capacity(registry.config.server.send_queue_capacity);
        let store = Arc::new(store);

        for group_id in group_ids {
            // Read the persisted replica_id from node-config.json;
            // fall back to the CLI arg if not found.
            let effective_replica_id = persisted_replica_id(&registry.config.config_root, store_id, group_id)
                .await
                .unwrap_or(replica_id);
            debug!(
                store_id,
                group_id,
                replica_id = effective_replica_id,
                "restore: creating group from disk"
            );
            let group = match create_group_with_wal(
                store_id,
                group_id,
                effective_replica_id,
                PxLocalReplicaRole::Follower,
                &registry.config,
                registry.wal_backend.clone(),
                registry.crowtree_backend,
            )
            .await
            {
                Ok(group) => group,
                Err(e) => {
                    warn!(
                        store_id,
                        group_id,
                        error = %e,
                        "restore: failed to load group from disk; skipping"
                    );
                    continue;
                }
            };
            store.add_group(group);
        }

        if let Err(e) = store.start().await {
            warn!(store_id, port, error = %e, "restore: failed to start store; skipping");
            continue;
        }

        info!(
            store_id,
            listen_addr = ?store.listen_addr(),
            group_count = store.group_count(),
            "restore: PxKvStore started from disk"
        );
        registry.add_store(store_id, store);
    }
}

/// Parse `store{S}` → `S`. Returns `None` if the name does not match.
fn parse_store_dir(name: &str) -> Option<u64> {
    name.strip_prefix("store").and_then(|rest| rest.parse().ok())
}

/// Parse `group{G}` → `G`. Returns `None` if the name does not match.
fn parse_group_dir(name: &str) -> Option<u64> {
    name.strip_prefix("group").and_then(|rest| rest.parse().ok())
}

/// Read the persisted `replica_id` for `(store_id, group_id)` from
/// `node-config.json`. Returns `None` if the file or entry is missing.
async fn persisted_replica_id(config_root: &Path, store_id: u64, group_id: u64) -> Option<u64> {
    let store = NodeConfigStore::new(config_root);
    let config = store.load().await.ok()?;
    config.group(store_id, group_id).map(|g| g.replica_id)
}
