// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_config::GroupConfigStore;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::{PxElectionConfig, WalConfig};
use crowkv::kv::{CrowtreeBackend, CrowtreeEngine, CrowtreeOptions, KVEngine};
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine};

use crate::store_registry::KvEngineKind;

/// Load persisted group config from the config file and apply it to the group.
///
/// The config file lives at `{config_root}/store{store_id}_group{group_id}.bin`.
/// If present, the group is seeded with the durable membership so it does not
/// start as a `quorum=1` singleton in the restore window. The config store is
/// also set on the group so future `persist_config` calls write to the same
/// file.
async fn maybe_apply_persisted_config(group: &mut PxGroup, config_root: &Path, store_id: u64) {
    let store = GroupConfigStore::new(config_root, store_id, group.group_id());
    match store.load().await {
        Ok(Some(config)) => {
            if config.group_id == group.group_id() {
                group.apply_config(&config);
            }
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(
                group_id = group.group_id(),
                error = %e,
                "failed to load persisted group config"
            );
        }
    }
    group.set_config_store(store);
}

#[must_use]
pub fn store_wal_root(wal_root: &Path, store_id: u64) -> PathBuf {
    wal_root.join(format!("store{store_id}"))
}

/// Durable per-group crowtree directory path: `{data_root}/store{store_id}/group{group_id}`.
/// Only used when `--kv-engine crowtree` is selected. Both `TextPageStore` and
/// `BlockPageStore` expect a directory path (`TextPageStore` creates a subdirectory
/// `{path}/{store_id}-{group_id}/`, `BlockPageStore` creates `.blk-*` files
/// directly in `path`).
#[must_use]
pub fn store_crowtree_path(data_root: &Path, store_id: u64, group_id: u64) -> PathBuf {
    data_root
        .join(format!("store{store_id}"))
        .join(format!("group{group_id}"))
}

/// Open (creating on first boot) the durable crowtree engine backing
/// `(store_id, group_id)`'s learner, boxed for [`PxLearner::with_engine`]
/// via [`PxLocalReplica::restore_from_replay_with_engine`].
///
/// # Errors
///
/// Returns an I/O error if the parent directory cannot be created, or if
/// `CrowtreeEngine::open` fails (e.g. a corrupt or unreadable file).
async fn open_crowtree_engine(
    data_root: &Path,
    store_id: u64,
    group_id: u64,
    backend: CrowtreeBackend,
) -> io::Result<Box<dyn KVEngine>> {
    let path = store_crowtree_path(data_root, store_id, group_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::create_dir_all(&path).await?;
    let opt = CrowtreeOptions {
        path: Some(path.to_string_lossy().into_owned()),
        backend,
        store_id: u32::try_from(store_id).unwrap_or(0),
        group_id: u32::try_from(group_id).unwrap_or(0),
        ..Default::default()
    };
    // `CrowtreeEngine::open` is a synchronous FFI call; called here inline
    // (not `spawn_blocking`) consistent with `CrowtreeEngine`'s own
    // documented policy of calling the still-fully-synchronous crowtree
    // core directly rather than adding a thread-pool hop with no genuine
    // asynchrony behind it (see `crowkv::kv::CrowtreeEngine`'s docs). This
    // runs once per group at boot, not on a hot path.
    let engine = CrowtreeEngine::open(&opt)
        .map_err(|e| io::Error::other(format!("CrowtreeEngine::open({}) failed: {e:?}", path.display())))?;
    Ok(Box::new(engine))
}

/// Create a live group by replaying any existing WAL, restoring the local
/// replica state, attaching a fresh `WalEngine`, and seeding the next proposal
/// slot / segment id.
///
/// # Errors
///
/// Returns any I/O or replay/restore error encountered while scanning the
/// existing WAL, creating the new WAL engine, opening the durable crowtree
/// engine (when selected), or rebuilding the local replica.
#[allow(clippy::too_many_arguments)]
pub async fn create_group_with_wal(
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    initial_role: PxLocalReplicaRole,
    election_cfg: PxElectionConfig,
    wal_root: &Path,
    config_root: &Path,
    wal_backend: Arc<IoBackend>,
    kv_engine: KvEngineKind,
    data_root: &Path,
    crowtree_backend: CrowtreeBackend,
    skip_fsync: bool,
) -> io::Result<PxGroup> {
    let mut wal_config = WalConfig::with_root(store_wal_root(wal_root, store_id));
    if std::env::var("CROWKV_WAL_TEXT").as_deref() == Ok("1") {
        wal_config.wal_record_format = crowkv::wal::WalRecordFormat::TextLine;
    }
    wal_config.wal_skip_fsync = skip_fsync;
    let replay = replay_group(&wal_backend, &wal_config.wal_disks, group_id).await?;
    let wal = WalEngine::create(wal_backend, wal_config, group_id).await?;
    wal.set_next_segment_id(replay.max_segment_id.saturating_add(1).max(1));

    let mut local_replica = match kv_engine {
        KvEngineKind::Memory => {
            PxLocalReplica::restore_from_replay(replica_id, initial_role, &replay).await?
        }
        KvEngineKind::Crowtree => {
            let engine = open_crowtree_engine(data_root, store_id, group_id, crowtree_backend).await?;
            PxLocalReplica::restore_from_replay_with_engine(replica_id, initial_role, &replay, engine).await?
        }
    };
    local_replica.set_wal(wal);

    let mut group = PxGroup::new(group_id, local_replica);
    maybe_apply_persisted_config(&mut group, config_root, store_id).await;
    // If the caller initialized the group as Leader, the proposal leadership
    // gate (current_term == proposing_term) will not open until the term is
    // stamped. The election driver handles this on a real win, but groups
    // created/restored as Leader (e.g., single-replica management API) may
    // serve proposals before the driver runs, so stamp it synchronously here.
    if initial_role == PxLocalReplicaRole::Leader {
        let term = group.local_replica().current_term_snapshot();
        group.stamp_proposing_term(term);
    }
    group.set_election_config(election_cfg);
    let next_slot = group
        .local_replica()
        .highest_seen_slot()
        .max(group.local_replica().last_chosen_slot())
        .max(group.local_replica().contiguous_applied())
        .saturating_add(1)
        .max(1);
    group.set_next_slot(next_slot);
    Ok(group)
}
