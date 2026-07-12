use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_config::GroupConfigStore;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::{PxElectionConfig, WalConfig};
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine};

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

/// Create a live group by replaying any existing WAL, restoring the local
/// replica state, attaching a fresh `WalEngine`, and seeding the next proposal
/// slot / segment id.
///
/// # Errors
///
/// Returns any I/O or replay/restore error encountered while scanning the
/// existing WAL, creating the new WAL engine, or rebuilding the local replica.
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
) -> io::Result<PxGroup> {
    let wal_config = WalConfig::with_root(store_wal_root(wal_root, store_id));
    let replay = replay_group(&wal_backend, &wal_config.wal_disks, group_id).await?;
    let wal = WalEngine::create(wal_backend, wal_config, group_id).await?;
    wal.set_next_segment_id(replay.max_segment_id.saturating_add(1).max(1));
    wal.set_snapshot_slot(replay.snapshot_slot);

    let mut local_replica = PxLocalReplica::restore_from_replay(replica_id, initial_role, &replay).await?;
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
