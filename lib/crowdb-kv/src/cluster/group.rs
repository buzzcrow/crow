// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::missing_fields_in_debug)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use dashmap::DashMap;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::cluster::group_config::GroupConfigStore;
use crate::cluster::group_election::{LeaderElection, PendingLeaderHandoff, ReadBarrierOutcome};
use crate::cluster::group_fetchgap::run_fetchgap_driver;
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::node_config::NodeConfigStore;
use crate::cluster::remote_replica::PxRemoteReplica;
use crate::cluster::replica::Replica;
use crate::common::config::{AdmissionPolicy, CrowDBConfig, PaxosConfig};
use crate::metrics::{Counter, Gauge, LatencySummary};
use crate::paxos::roles::{Acceptor, DedupTag, PxBallot, PxLogEntry, SlotIndex};
use crate::paxos::{PxGroupId, PxNodeId};

pub(crate) use crate::cluster::group_accept::AcceptAttempt;
pub(crate) use crate::cluster::group_inflight::{
    InflightAdmission, InflightRegistryHandles, RemoteFoldCtx, ReplyFold,
};
pub(crate) use crate::cluster::group_prepare::PrepareAttempt;

/// Registry-based metric handles for write-path instrumentation.
/// Created by [`PxGroup::set_metrics_registry`] and stored in a
/// `OnceLock` for lock-free hot-path reads. Mirrors `ReadRegistryHandles`.
pub(crate) struct WriteRegistryHandles {
    pub(crate) propose_e2e: Arc<LatencySummary>,
    pub(crate) prepare_phase: Arc<LatencySummary>,
    pub(crate) accept_phase: Arc<LatencySummary>,
    pub(crate) accept_quorum_rpc: Arc<LatencySummary>,
}

/// Registry-based metric handles for read-path instrumentation.
/// Created by [`PxGroup::set_metrics_registry`] and stored in a
/// `OnceLock` for lock-free hot-path reads. Mirrors the pattern of
/// `ElectionRegistryHandles` on `PxLocalReplica`.
pub(crate) struct ReadRegistryHandles {
    pub(crate) lease_path: Arc<Counter>,
    pub(crate) readindex_path: Arc<Counter>,
    pub(crate) readindex_rounds: Arc<Counter>,
    pub(crate) minslot_fallback: Arc<Counter>,
    pub(crate) barrier: Arc<LatencySummary>,
    pub(crate) engine_get: Arc<LatencySummary>,
    /// R35 apply-fence wait latency (fast path is a single atomic load).
    pub(crate) apply_fence: Arc<LatencySummary>,
    pub(crate) lease_valid: Arc<Gauge>,
    pub(crate) contiguous_applied: Arc<Gauge>,
    pub(crate) safe_slot: Arc<Gauge>,
}

/// Pending `ReadIndex` barrier batch: the waiters that arrived while the
/// round leader's heartbeat round was in flight. Held in
/// `PxGroup::pending_read_barrier` only for the duration of one `ReadIndex`
/// round; the leader drains and resolves all waiters with its outcome
/// (carrying the pre-round `read_slot` freshness floor) on completion.
pub(crate) struct PendingReadBarrier {
    pub(crate) waiters: Vec<tokio::sync::oneshot::Sender<ReadBarrierOutcome>>,
}

/// R45: an accumulating batch for the *next* Paxos round. Held in
/// `PxGroup::coalescer` while a round is in flight — ops that arrive
/// during the round append here instead of starting their own round.
/// When the in-flight round completes, this batch is drained and
/// becomes the next round (if non-empty). If it fills to `max_keys`
/// before the round completes, it is flushed immediately as a
/// concurrent round (the "multiple pipelines" path).
#[derive(Default)]
pub(crate) struct PendingBatch {
    pub(crate) op_bodies: Vec<u8>,
    pub(crate) op_count: u16,
    pub(crate) tags: Vec<DedupTag>,
    pub(crate) waiters: Vec<tokio::sync::oneshot::Sender<ProposeResult>>,
}

pub struct PxGroup {
    pub group_id: PxGroupId,
    pub(crate) cached_quorum: usize,
    pub(crate) local_replica: PxLocalReplica,
    pub(crate) remote_replicas: Vec<RemoteReplicaKind>,
    pub(crate) valid_replica_count: usize,
    pub(crate) next_slot: AtomicU64,
    /// Unified cluster configuration held by this group. Replaces the
    /// former individual `force_classic` / `wal_early_ack` /
    /// `async_engine_apply` bool fields and `election_cfg`. Set wholesale
    /// via [`Self::set_from_config`]; individual setters delegate into
    /// `self.config.*` so the held config stays the source of truth.
    pub(crate) config: CrowDBConfig,
    /// Per-leadership-tenure [`CancellationToken`]. Cancelled in
    /// [`Self::shutdown`] and by every step-down trigger. The bulk-Phase-1
    /// sweep and the election driver both honor it.
    pub(crate) tenure_cancel: CancellationToken,
    /// `JoinHandle` of the spawned election driver (`None` while the driver
    /// has not been started or is disabled). Wrapped in an async mutex so
    /// `shutdown` can `await` it cooperatively without blocking other
    /// readers of `self`.
    pub(crate) driver_handle: AsyncMutex<Option<JoinHandle<()>>>,
    /// `JoinHandle` of the spawned engine-durability + WAL GC maintenance
    /// loop ([`crate::cluster::group_maintenance`]), `None` until started.
    /// Wrapped in an async mutex so `shutdown` can `await` it
    /// cooperatively, matching `driver_handle`. Shares `tenure_cancel` as
    /// its cancellation source, so cancelling that (shutdown, or group
    /// replacement in `PxKvStore`) stops both tasks together.
    pub(crate) maintenance_handle: AsyncMutex<Option<JoinHandle<()>>>,
    /// R65: `JoinHandle` of the spawned `FetchGap` driver (follower-side
    /// catch-up). `None` until started. Cancelled via `tenure_cancel`.
    pub(crate) fetchgap_handle: AsyncMutex<Option<JoinHandle<()>>>,
    /// Handoff from a freshly elected candidate to the upcoming
    /// `run_leader_state` invocation. Holds `(term, peer_floor,
    /// peer_ceiling)` for bulk Phase 1. Consumed once on Leader-state
    /// entry.
    pub(crate) pending_leader_handoff: parking_lot::Mutex<Option<PendingLeaderHandoff>>,
    /// Term stamped on becoming leader. The propose leadership gate
    /// accepts a proposal only when the local replica's `role == Leader`
    /// **and** its `current_term == proposing_term`. Mismatch on either
    /// field means the leader tenure ended (the driver stepped down or
    /// moved to a new term) and the proposal must fail fast with
    /// `NotLeader` instead of racing into Paxos with stale identity.
    ///
    /// Default `0` matches the default `current_term` of a freshly
    /// constructed [`PxLocalReplica`], so testkit pinned-leader groups
    /// pass the gate without explicit stamping.
    pub(crate) proposing_term: AtomicU64,
    /// Whether the current leader tenure may serve linearizable reads from the
    /// local learner state. A freshly elected leader that came from replay-only
    /// restore must first finish bulk Phase 1 recovery and locally relearn the
    /// chosen prefix before it can safely serve reads.
    pub(crate) leader_read_ready: AtomicBool,
    /// Last-known `contiguous_applied` per voting peer, refreshed from
    /// heartbeat replies. Peers never heard from are absent (treated as
    /// `0`), which keeps [`Self::group_safe_slot`] conservative until every
    /// member has reported. Only meaningful while this replica is leader.
    pub(crate) peer_applied: parking_lot::Mutex<HashMap<PxNodeId, SlotIndex>>,
    /// Group safe-slot: `min(contiguous_applied)` across the local replica
    /// and all voting peers. Every slot `<= group_safe_slot` is applied on a
    /// majority-and-then-some — specifically on *every* member that has
    /// reported — so a bounded-stale read served at this slot reflects state
    /// no follower can contradict. Recomputed at the end of each quorum
    /// heartbeat round. `0` means "not yet established".
    pub(crate) group_safe_slot: AtomicU64,
    /// Last-known `durable_snapshot_slot` per voting peer, refreshed from
    /// heartbeat replies alongside `peer_applied`. Peers never heard from
    /// are absent (treated as `0`). Only meaningful while this replica is
    /// leader.
    pub(crate) peer_durable: parking_lot::Mutex<HashMap<PxNodeId, SlotIndex>>,
    /// Group durable-snapshot watermark: `min(local durable_snapshot_slot,
    /// max(peer durable_snapshot_slot))` -- the real "durable on leader
    /// plus at least one peer" watermark (`snapshot_slot`,
    /// gossiped piggybacked on the same
    /// heartbeat round as `group_safe_slot`. Taking the *max* over peers
    /// (not `min`, unlike `group_safe_slot`) is deliberate: the design only
    /// requires *one* peer beyond the leader to durably have a slot, so the
    /// furthest-along peer alone is always a sufficient witness -- a
    /// straggler peer must not hold this watermark back the way it holds
    /// back `group_safe_slot` (which requires *every* member). `0` means
    /// "not yet established" (no peer has reported, or this replica has no
    /// local WAL to report its own snapshot progress). Recomputed at the
    /// end of each quorum heartbeat round.
    pub(crate) group_snapshot_slot: AtomicU64,
    /// In-flight proposal admission gate. Holds N semaphores (one per
    /// queue), each sized to `ceil(max_inflight / N)` permits. Each
    /// in-flight `propose` call holds one permit for its duration.
    /// Depending on `policy`, a full queue either fails fast with
    /// `ProposeResult::Busy` (Reject) or blocks on `acquire().await`
    /// (Queue).
    pub(crate) inflight: InflightAdmission,
    /// Optional file-based config store for persisting group membership.
    /// Set via [`Self::set_config_store`] when the group has a WAL directory.
    /// When set, [`Self::persist_config`] writes to the config file instead
    /// of the WAL metadata lane.
    pub(crate) config_store: Option<GroupConfigStore>,
    /// Optional per-node config store (node-config.json). When set,
    /// [`Self::persist_config`] writes to the combined node config file
    /// instead of the legacy per-group file. Takes priority over
    /// `config_store`.
    pub(crate) node_config_store: Option<(NodeConfigStore, u64, u64)>,
    /// Monotonic counter bumped by exactly 1 whenever a mutation changes
    /// the *voting set* (a voting member added/removed, or an existing
    /// remote's voting flag flips) -- never for a non-voting add/remove,
    /// since that cannot affect quorum size. Stamped on outgoing
    /// `Prepare`/`Accept` and checked for an exact match on the receiving
    /// side: any two adjacent single-degree membership configs are only
    /// guaranteed to have overlapping quorums, not any two arbitrary
    /// configs separated by more than one change, so "close enough"
    /// epoch matching is not safe -- only exact match is.
    pub(crate) membership_epoch: AtomicU64,
    /// Slot covered by the last `persist_snapshot()` call. Used by
    /// `run_pass` to gate expensive disk snapshots on a slot-advance
    /// threshold (`snapshot_slot_threshold`).
    pub(crate) last_snapshot_slot: AtomicU64,
    /// Wall-clock time of the last `persist_snapshot()` call. Used by
    /// `run_pass` to gate expensive disk snapshots on a time threshold
    /// (`snapshot_time_threshold_ms`).
    pub(crate) last_snapshot_time: parking_lot::Mutex<std::time::Instant>,
    /// Wall-clock time of the last `wal.flush_all()` call. Used by
    /// `run_pass` to gate periodic WAL durable flushes on
    /// `wal_flush_interval_ms`.
    pub(crate) last_wal_flush_time: parking_lot::Mutex<std::time::Instant>,
    /// Gap 5 step 2: notified when a memtable freeze happens in the C++
    /// engine, so the maintenance loop can flush immediately instead of
    /// waiting for the next tick. Shared with the learner's apply path
    /// via `set_flush_signal` so `apply_entry` can call `notify_one()`.
    pub(crate) flush_notify: Arc<tokio::sync::Notify>,
    /// Gap 5 step 4: count of flushes since the last `persist_snapshot`.
    /// When this reaches `snapshot_flush_count_threshold`, trigger a
    /// snapshot regardless of the slot/time thresholds. This is a proxy
    /// for "enough new data has landed in L1 to be worth persisting".
    pub(crate) flushes_since_snapshot: AtomicU64,
    /// Optional registry handles for read-path metrics. Set via
    /// [`Self::set_metrics_registry`] when a registry is wired.
    /// `None` in tests / no-registry mode.
    pub(crate) read_handles: OnceLock<ReadRegistryHandles>,
    /// Optional registry handles for write-path metrics. Set via
    /// [`Self::set_metrics_registry`] when a registry is wired.
    pub(crate) write_handles: OnceLock<WriteRegistryHandles>,
    /// Pending `ReadIndex` barrier batch. `Some` only while a `ReadIndex`
    /// heartbeat round is in flight; concurrent reads that arrive during
    /// the round enqueue a waiter here instead of starting their own
    /// round. The round leader drains and resolves all waiters on round
    /// completion (success → `Ready`, step-down → `NotLeader`, no-quorum
    /// → `NoQuorum`). See `linearizable_read_barrier`.
    pub(crate) pending_read_barrier: parking_lot::Mutex<Option<PendingReadBarrier>>,
    /// Test-only gate that holds the next `ReadIndex` heartbeat round open
    /// until the test releases it, so concurrent reads deterministically
    /// batch onto one round. Set via `set_readindex_round_gate_for_tests`
    /// under the `test-util` feature; `None` in production.
    #[cfg(feature = "test-util")]
    pub(crate) readindex_round_gate: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    /// Test-only gate that holds the next coalescer round open until the
    /// test releases it, so concurrent ops deterministically join the
    /// pending batch. Consumed by the first round that runs after this call.
    #[cfg(feature = "test-util")]
    pub(crate) coalesce_round_gate: parking_lot::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    /// R36 self-weak back-reference, set once the group is wrapped in an
    /// [`Arc`] via [`Self::set_self_weak`]. The coalescer's per-batch timer
    /// task upgrades this to spawn a flush without the group holding a
    /// strong self-reference (which would leak). `None` until set (test
    /// groups not wrapped in `Arc`); the coalescer falls back to the
    /// direct single-op path when unset.
    pub(crate) self_weak: OnceLock<Weak<PxGroup>>,
    /// R45 coalescer state. `None` when idle (no round in flight, no
    /// batch accumulating); `Some(PendingBatch)` while a Paxos round is
    /// in flight and ops are accumulating for the next round. Drained on
    /// round completion to start the next round, or on `max_keys`
    /// overflow to start a concurrent round.
    pub(crate) coalescer: parking_lot::Mutex<Option<PendingBatch>>,
    /// Fixed `max_keys` for coalescing batches. Set from config; 0 disables
    /// coalescing. When a batch fills to this size, it flushes as a
    /// concurrent round.
    pub(crate) coalesce_max_keys: std::sync::atomic::AtomicU16,
    /// Last coalescer activity timestamp (micros since `UNIX_EPOCH`).
    /// Updated on every enqueue and round completion. The watchdog
    /// checks this to detect stuck batches.
    pub(crate) coalesce_last_activity_us: std::sync::atomic::AtomicU64,
    /// Single long-running watchdog task handle. Started lazily on
    /// first enqueue.
    pub(crate) coalesce_watchdog_handle: OnceLock<tokio::task::JoinHandle<()>>,
    /// R59 snapshot versioning API: per-group snapshot handle registry.
    /// Each entry is a pinned `SnapshotHandle` with a lease/expiry. Reaped
    /// lazily on `create`/`list`/`scan` when the lease has elapsed.
    pub(crate) snapshots: DashMap<u64, Arc<SnapshotHandle>>,
    /// Monotonic counter for snapshot handle ids within this group.
    pub(crate) next_snapshot_handle: AtomicU64,
    /// Watch/notify registry. Per-group; wired into the learner via
    /// `set_watch_registry` so `apply_entry` can emit notifies. Cleared
    /// on leader step-down (drops all watcher tx senders, closing
    /// client streams for clean reconnect).
    pub watch_registry: Arc<crate::cluster::watch_registry::WatchRegistry>,
}

/// R59 snapshot versioning API: a pinned point-in-time-consistent L1 view.
/// Created by `kv_create_snapshot` (flush + `snapshot_view`), iterated by
/// `kv_snapshot_scan` (binary-search + linear scan over `entries`),
/// released by `kv_release_snapshot` (drop from the registry). Reaped
/// lazily by lease expiry (default 5 min).
pub(crate) struct SnapshotHandle {
    pub handle: u64,
    pub at_slot: u64,
    pub entries: Vec<crate::kv::SnapshotViewEntry>,
    pub created_at: std::time::Instant,
    pub lease: std::time::Duration,
}

impl SnapshotHandle {
    /// Default lease: 5 minutes. Reaped lazily if a client disconnects
    /// mid-scan without calling `ReleaseSnapshot`.
    pub const DEFAULT_LEASE: std::time::Duration = std::time::Duration::from_secs(300);

    /// Remaining lease duration. `Duration::ZERO` if expired.
    pub(crate) fn lease_remaining(&self) -> std::time::Duration {
        let elapsed = self.created_at.elapsed();
        if elapsed >= self.lease {
            std::time::Duration::ZERO
        } else {
            self.lease.checked_sub(elapsed).unwrap_or_default()
        }
    }

    /// Whether the lease has expired.
    pub(crate) fn expired(&self) -> bool {
        self.created_at.elapsed() >= self.lease
    }
}

impl std::fmt::Debug for PxGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxGroup")
            .field("group_id", &self.group_id)
            .field("cached_quorum", &self.cached_quorum)
            .field("leader_id", &self.leader_id())
            .field("local_replica_id", &self.local_replica.id)
            .field("valid_replica_count", &self.valid_replica_count)
            .field("remote_replicas_len", &self.remote_replicas.len())
            .finish_non_exhaustive()
    }
}

impl PxGroup {
    pub fn new(group_id: PxGroupId, local_replica: PxLocalReplica) -> Self {
        // O6: capture replica_id before the move into the struct for the
        // per-replica snapshot jitter below.
        let replica_id = local_replica.id;
        let mut group = Self {
            group_id,
            cached_quorum: 0,
            local_replica,
            remote_replicas: Vec::new(),
            valid_replica_count: 0,
            next_slot: AtomicU64::new(1),
            // Test path: wal_early_ack / async_engine_apply default false
            // for deterministic synchronous apply; production overwrites via
            // set_from_config.
            config: CrowDBConfig {
                wal_early_ack: false,
                async_engine_apply: false,
                ..CrowDBConfig::default()
            },
            tenure_cancel: CancellationToken::new(),
            driver_handle: AsyncMutex::new(None),
            maintenance_handle: AsyncMutex::new(None),
            fetchgap_handle: AsyncMutex::new(None),
            pending_leader_handoff: parking_lot::Mutex::new(None),
            proposing_term: AtomicU64::new(0),
            leader_read_ready: AtomicBool::new(true),
            peer_applied: parking_lot::Mutex::new(HashMap::new()),
            group_safe_slot: AtomicU64::new(0),
            peer_durable: parking_lot::Mutex::new(HashMap::new()),
            group_snapshot_slot: AtomicU64::new(0),
            inflight: InflightAdmission::new(
                PaxosConfig::DEFAULT.max_inflight_proposals,
                PaxosConfig::DEFAULT.inflight_admission,
            ),
            config_store: None,
            node_config_store: None,
            membership_epoch: AtomicU64::new(0),
            last_snapshot_slot: AtomicU64::new(0),
            // O6: per-replica jitter to stagger snapshot timing. Offset the
            // initial last_snapshot_time backward by replica_id * 200ms so
            // each replica's time-threshold check fires at a different point
            // in the tick cycle, avoiding synchronized I/O spikes across the
            // cluster. Capped at 5s to avoid excessive delay for high IDs.
            last_snapshot_time: parking_lot::Mutex::new(
                std::time::Instant::now()
                    .checked_sub(std::time::Duration::from_millis((replica_id * 200).min(5000)))
                    .unwrap_or_else(std::time::Instant::now),
            ),
            last_wal_flush_time: parking_lot::Mutex::new(std::time::Instant::now()),
            flush_notify: Arc::new(tokio::sync::Notify::new()),
            flushes_since_snapshot: AtomicU64::new(0),
            read_handles: OnceLock::new(),
            write_handles: OnceLock::new(),
            pending_read_barrier: parking_lot::Mutex::new(None),
            #[cfg(feature = "test-util")]
            readindex_round_gate: parking_lot::Mutex::new(None),
            #[cfg(feature = "test-util")]
            coalesce_round_gate: parking_lot::Mutex::new(None),
            self_weak: OnceLock::new(),
            coalescer: parking_lot::Mutex::new(None),
            coalesce_max_keys: std::sync::atomic::AtomicU16::new(0),
            coalesce_last_activity_us: std::sync::atomic::AtomicU64::new(0),
            coalesce_watchdog_handle: OnceLock::new(),
            snapshots: DashMap::new(),
            next_snapshot_handle: AtomicU64::new(1),
            watch_registry: Arc::new(crate::cluster::watch_registry::WatchRegistry::new()),
        };
        // Wire the watch registry into the learner so `apply_entry`
        // can emit notifies on the apply path.
        group
            .local_replica
            .learner
            .set_watch_registry(group_id, Arc::clone(&group.watch_registry));
        // Gap 5 step 2: wire the flush signal so the apply path can wake
        // the maintenance loop when a memtable freeze happens.
        group
            .local_replica
            .learner
            .set_flush_signal(Arc::clone(&group.flush_notify));
        group.recompute_quorum();
        group
    }

    /// Set the file-based config store for this group.
    ///
    /// When set, [`Self::persist_config`] writes the group membership to a
    /// dedicated config file (`store{sid}_group{gid}.json`) in the group's conf directory.
    pub fn set_config_store(&mut self, store: GroupConfigStore) {
        self.config_store = Some(store);
    }

    /// Set the per-node config store for this group.
    ///
    /// When set, `persist_config` writes to `node-config.json`
    /// (the combined per-node cache) instead of the legacy per-group
    /// file. The `(store_id, group_id)` pair identifies this group's
    /// entry within the file.
    pub fn set_node_config_store(&mut self, store: NodeConfigStore, store_id: u64, group_id: u64) {
        self.node_config_store = Some((store, store_id, group_id));
    }

    /// Enable or disable R16b early-ack (declare chosen on remote quorum
    /// without waiting for local WAL persist). Default off.
    pub fn set_wal_early_ack(&mut self, enabled: bool) {
        self.config.wal_early_ack = enabled;
    }

    /// Enable or disable R17 async engine apply (return Chosen before
    /// local engine apply completes). Default off.
    pub fn set_async_engine_apply(&mut self, enabled: bool) {
        self.config.async_engine_apply = enabled;
    }

    /// Apply all tunables from a `CrowDBConfig` wholesale: replaces the
    /// held config, mirrors the election lease onto the local replica,
    /// and reconstructs the inflight admission gate from the paxos
    /// params. Replaces the per-field setter calls at group creation
    /// and rebuild sites.
    pub fn set_from_config(&mut self, config: &CrowDBConfig) {
        self.config = config.clone();
        self.set_election_config(config.election);
        self.set_inflight_config(
            config.paxos.max_inflight_proposals,
            config.paxos.inflight_admission,
        );
        self.coalesce_max_keys.store(
            config.paxos.coalesce_max_keys as u16,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Borrow the unified config held by this group.
    #[must_use]
    pub fn config(&self) -> &CrowDBConfig {
        &self.config
    }

    /// Set the in-flight proposal admission configuration. Must be
    /// called before the group starts serving proposals. Also syncs the
    /// params into `self.config.paxos` so the held config stays the
    /// source of truth.
    pub fn set_inflight_config(&mut self, max_inflight: usize, policy: AdmissionPolicy) {
        self.inflight = InflightAdmission::new(max_inflight, policy);
        self.config.paxos.max_inflight_proposals = max_inflight;
        self.config.paxos.inflight_admission = policy;
    }

    /// Current inflight proposal window size (total permits).
    #[must_use]
    pub fn inflight_window_size(&self) -> usize {
        self.inflight.total_permits()
    }

    /// Current admission policy.
    #[must_use]
    pub fn inflight_admission_policy(&self) -> AdmissionPolicy {
        self.inflight.policy
    }

    /// Get the config store, if set.
    #[must_use]
    pub fn config_store(&self) -> Option<&GroupConfigStore> {
        self.config_store.as_ref()
    }

    /// Get the node config store, if set.
    #[must_use]
    pub fn node_config_store(&self) -> Option<&NodeConfigStore> {
        self.node_config_store.as_ref().map(|(s, _, _)| s)
    }

    /// Get the `store_id` associated with the node config store, if set.
    #[must_use]
    pub fn node_config_store_sid(&self) -> Option<u64> {
        self.node_config_store.as_ref().map(|(_, sid, _)| *sid)
    }

    /// Spawn the per-group engine-durability + WAL GC maintenance loop
    /// ([`crate::cluster::group_maintenance`]).
    ///
    /// Must be called after the group is wrapped in an [`Arc`] so the loop
    /// can hold a [`std::sync::Weak`] back-reference, mirroring
    /// [`crate::cluster::group_election::LeaderElection::start_election_loop`].
    /// No-op when `config.election.election_driver_disabled` is set or when
    /// the loop has already been started.
    pub async fn start_engine_maintenance_loop(self: &Arc<Self>) {
        crate::cluster::group_maintenance::start(self).await;
    }

    /// R65: Start the follower-side `FetchGap` driver. Periodically checks
    /// `gap_slots` and sends `FetchGap` to the leader for missing/stale
    /// slots. Only runs on followers (leader has no gaps — it's the
    /// source of truth). No-op when `election_driver_disabled` or already
    /// started.
    pub async fn start_fetchgap_driver(self: &Arc<Self>) {
        if self.config.election.election_driver_disabled {
            return;
        }
        let mut guard = self.fetchgap_handle.lock().await;
        if guard.is_some() {
            return;
        }
        let weak = Arc::downgrade(self);
        let cancel = self.tenure_cancel.clone();
        let group_id = self.group_id;
        let handle = tokio::spawn(async move {
            run_fetchgap_driver(weak, group_id, cancel).await;
        });
        *guard = Some(handle);
    }

    /// Set the `Weak<PxGroup>` self-reference used by the R36 coalescer to
    /// spawn per-batch flush tasks without holding a strong self-reference.
    /// Must be called once after the group is wrapped in an [`Arc`]; idempotent
    /// (a second call is a no-op). Mirrors the `Weak` back-reference pattern of
    /// the election/maintenance loops.
    pub fn set_self_weak(self: &Arc<Self>) {
        let _ = self.self_weak.set(Arc::downgrade(self));
    }

    // ── Getters ───────────────────────────────────────────────────

    pub fn group_id(&self) -> PxGroupId {
        self.group_id
    }

    pub fn local_replica(&self) -> &PxLocalReplica {
        &self.local_replica
    }

    /// Wire the metrics registry into the local replica and all remote
    /// replicas. Registers election counters, WAL append summary, and
    /// per-peer RPC latency/error handles. Called once during group
    /// creation when a registry is available.
    ///
    /// # Panics
    ///
    /// Panics if the metrics registry mutex is poisoned.
    pub fn set_metrics_registry(
        &self,
        registry: &Arc<std::sync::Mutex<crate::metrics::MetricsRegistry>>,
        store_id: u64,
    ) {
        let group_id = self.group_id;
        self.local_replica
            .set_metrics_registry(registry, store_id, group_id);
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(r) = remote {
                r.set_metrics_registry(registry, store_id, group_id);
            }
        }
        let mut r = registry.lock().expect("metrics registry poisoned");
        let prefix = format!("s.{store_id}.g.{group_id}");
        let inflight_handles = InflightRegistryHandles {
            enqueued: r.register_counter(format!("{prefix}.write.inflight_enqueued.c")),
            wait_us: r.register_summary(format!("{prefix}.write.inflight_wait.l")),
        };
        let _ = self.inflight.handles.set(inflight_handles);
        let read_handles = ReadRegistryHandles {
            lease_path: r.register_counter(format!("{prefix}.read.lease_path.c")),
            readindex_path: r.register_counter(format!("{prefix}.read.readindex_path.c")),
            readindex_rounds: r.register_counter(format!("{prefix}.read.readindex_rounds.c")),
            minslot_fallback: r.register_counter(format!("{prefix}.read.minslot_fallback.c")),
            barrier: r.register_summary(format!("{prefix}.read.barrier.l")),
            engine_get: r.register_summary(format!("{prefix}.read.engine_get.l")),
            apply_fence: r.register_summary(format!("{prefix}.read.apply_fence.l")),
            lease_valid: r.register_gauge(format!("{prefix}.read.lease_valid.g")),
            contiguous_applied: r.register_gauge(format!("{prefix}.read.contiguous_applied.g")),
            safe_slot: r.register_gauge(format!("{prefix}.read.safe_slot.g")),
        };
        let _ = self.read_handles.set(read_handles);
        let write_handles = WriteRegistryHandles {
            propose_e2e: r.register_summary(format!("{prefix}.write.propose_e2e.l")),
            prepare_phase: r.register_summary(format!("{prefix}.write.prepare_phase.l")),
            accept_phase: r.register_summary(format!("{prefix}.write.accept_phase.l")),
            accept_quorum_rpc: r.register_summary(format!("{prefix}.write.accept_quorum_rpc.l")),
        };
        let _ = self.write_handles.set(write_handles);
    }

    /// Borrow optional registry handles for read-path metrics. Returns
    /// `None` when no metrics registry is wired (tests / no-registry mode).
    #[must_use]
    pub(crate) fn read_handles(&self) -> Option<&ReadRegistryHandles> {
        self.read_handles.get()
    }

    pub fn force_classic(&self) -> bool {
        self.config.force_classic
    }

    /// Whether R16b early-ack is enabled (deferred local WAL persist).
    #[must_use]
    pub fn wal_early_ack(&self) -> bool {
        self.config.wal_early_ack
    }

    /// Whether R17 async engine apply is enabled (deferred engine apply).
    #[must_use]
    pub fn async_engine_apply(&self) -> bool {
        self.config.async_engine_apply
    }

    /// Snapshot of the believed leader id for this group. Delegates to the
    /// local replica's election state, which is the single source of truth
    /// (updated by `become_leader` / `become_follower` / `on_heartbeat` /
    /// `on_request_vote`). Returns `0` (the "unknown leader" sentinel) when
    /// the local replica has not yet learned of any leader.
    #[must_use]
    pub fn leader_id(&self) -> PxNodeId {
        self.local_replica.believed_leader_id().unwrap_or(0)
    }

    pub fn quorum(&self) -> usize {
        self.cached_quorum
    }

    /// Current membership-epoch fence value. Stamped on outgoing
    /// `Prepare`/`Accept` and checked for an exact match on the receiving
    /// side (`rpc::px_service`).
    #[must_use]
    pub fn membership_epoch(&self) -> u64 {
        self.membership_epoch.load(Ordering::Acquire)
    }

    /// Group safe-slot snapshot: the highest slot known to be applied on the
    /// local replica **and** every voting peer that has reported. Bounded /
    /// safe-slot reads use this as their freshness floor. `0` until the first
    /// quorum heartbeat round establishes it.
    #[must_use]
    pub fn group_safe_slot(&self) -> SlotIndex {
        self.group_safe_slot.load(Ordering::Acquire)
    }

    /// Group durable-snapshot-watermark snapshot: state at this slot is
    /// durable on this (leader) replica **and** at least one voting peer
    /// (`snapshot_slot`
    /// `0` until the first quorum heartbeat round establishes it (or if this
    /// replica has no local WAL). Feeds `set_gc_watermark`'s `snapshot_slot`
    /// argument in `group_maintenance::run_pass`, replacing the previous
    /// `group_safe_slot` approximation.
    #[must_use]
    pub fn group_snapshot_slot(&self) -> SlotIndex {
        self.group_snapshot_slot.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn leader_read_ready(&self) -> bool {
        self.leader_read_ready.load(Ordering::Acquire)
    }

    // ── R45 coalescer ─────────────────────────────────────

    /// One background-repair step: find the lowest gap in the open prefix
    /// (the first unchosen slot below the highest slot this leader has seen
    /// chosen) and drive classic Paxos to close it.
    ///
    /// Classic Paxos here is self-healing: the Prepare phase adopts any value
    /// already accepted at the gap slot (recovering a half-committed write),
    /// and otherwise fills the slot with an empty `NoOp` so the contiguous
    /// frontier — and thus the group safe-slot — can advance past an abandoned
    /// slot. Distinct from the one-shot bulk Phase 1 run on leader entry: this
    /// runs repeatedly during steady-state leadership. A no-gap leader returns
    /// [`RepairOutcome::NoGap`] without any RPCs, so it is cheap to poll.
    pub(crate) async fn repair_once(&self) -> RepairOutcome {
        let replica = &self.local_replica;
        if replica.role() != crate::cluster::local_replica::PxLocalReplicaRole::Leader {
            return RepairOutcome::NotLeader;
        }
        let contiguous = replica.contiguous_chosen();
        let highest = replica.last_chosen_slot();
        if contiguous >= highest {
            return RepairOutcome::NoGap;
        }
        // The first slot above the contiguous frontier is, by definition, not
        // yet chosen locally — the lowest hole to fill.
        let gap_slot = contiguous + 1;
        let quorum = self.quorum();
        let group_id = self.group_id;
        debug!(
            group_id,
            gap_slot, contiguous, highest, "background repair: filling gap"
        );

        // Always run Phase 1 (classic) so an already-accepted value is
        // adopted rather than overwritten.
        let entry = match self
            .run_prepare_phase(replica, gap_slot, bytes::Bytes::new(), quorum, 0)
            .await
        {
            PrepareAttempt::Proceed { entry, .. } => entry,
            PrepareAttempt::Retry { error, .. } | PrepareAttempt::Fail { error } => {
                debug!(
                    group_id,
                    gap_slot,
                    error = error.keyword(),
                    "repair prepare did not proceed"
                );
                return RepairOutcome::Failed;
            }
        };

        match self.run_accept_phase(replica, &entry, &[], quorum).await {
            AcceptAttempt::Chosen => {
                replica.learn_chosen(&entry, &[]).await;
                self.fan_out_chosen_notice(&entry, group_id);
                debug!(group_id, slot = gap_slot, "background repair filled gap");
                RepairOutcome::Filled { slot: gap_slot }
            }
            AcceptAttempt::Retry { error, .. } | AcceptAttempt::Fail { error } => {
                debug!(
                    group_id,
                    gap_slot,
                    error = error.keyword(),
                    "repair accept did not choose"
                );
                RepairOutcome::Failed
            }
        }
    }

    pub(crate) fn base_entry(&self, slot: u64, payload: bytes::Bytes) -> PxLogEntry {
        PxLogEntry {
            slot,
            ballot: PxBallot::new(0, self.local_replica.id),
            term: self.local_replica.current_term_snapshot(),
            payload,
        }
    }

    pub(crate) fn consider_accepted(adopted: &mut Option<PxLogEntry>, candidate: PxLogEntry) {
        let should_replace = adopted
            .as_ref()
            .map_or(true, |current| candidate.ballot > current.ballot);
        if should_replace {
            *adopted = Some(candidate);
        }
    }

    /// Best-effort fan-out of a `ChosenNotification` to every real
    /// remote in this group after a slot has been chosen. The notice is
    /// fire-and-forget over the per-peer bidi `PxLearnerStream`; failures
    /// are logged at `debug!` and never propagated, since the next
    /// heartbeat (carrying `committed_safe_slot`) will re-converge
    /// peer frontiers regardless.
    ///
    /// `leader_id` is taken from `entry.ballot.leader_id`, matching the
    /// proposer that chose the value. Sequential await rather than
    /// `JoinSet` fan-out is fine for now: each `send_chosen_notice` is
    /// just an mpsc enqueue (capacity = `learner_stream_window_frames`)
    /// once the per-peer bg task is running, so it returns near-
    /// instantly except when a peer is down (in which case it fast-
    /// fails via the connect-retry drain in `learner_stream.rs`).
    pub(crate) fn fan_out_chosen_notice(&self, entry: &PxLogEntry, group_id: u64) {
        let slot = entry.slot;
        let term = entry.term;
        let leader_id = entry.ballot.leader_id;
        let ballot_round = entry.ballot.round;
        for remote in &self.remote_replicas {
            let RemoteReplicaKind::Real(remote) = remote else {
                continue;
            };
            let remote_id = remote.node_id;
            if let Err(err) = remote.send_chosen_notice(slot, term, leader_id, group_id, ballot_round) {
                debug!(group_id, slot, term, remote_id, endpoint = %remote.endpoint, error = %err, "fan_out_chosen_notice: peer notice failed (best-effort)");
            }
        }
    }

    /// R65: Leader-side `FetchGap` handler. Looks up the requested slot
    /// in the local acceptor. If the leader has an accepted value, replies
    /// with it (payload + ballot + term). If the leader doesn't have the
    /// value, returns `None` — the slot is not yet chosen from this
    /// leader's perspective, and the follower should retry later (the
    /// leader's `repair_once` will eventually fill it, or a newer
    /// `ChosenNotice` will re-notify).
    pub(crate) fn handle_fetch_gap(&self, slot: SlotIndex) -> Option<crate::rpc::FetchGapResponse> {
        let replica = &self.local_replica;
        if !replica.is_leader() {
            debug!(
                group_id = self.group_id,
                slot, "handle_fetch_gap: not leader, ignoring"
            );
            return None;
        }
        let entry = replica.acceptor.accepted_at(slot)?;
        let group_id = self.group_id;
        debug!(
            group_id,
            slot,
            round = entry.ballot.round,
            leader_id = entry.ballot.leader_id,
            term = entry.term,
            payload_len = entry.payload.len(),
            "handle_fetch_gap: replying with accepted value"
        );
        Some(crate::rpc::FetchGapResponse {
            version: 1,
            group_id,
            slot: entry.slot,
            term: entry.term,
            ballot_round: entry.ballot.round,
            leader_id: entry.ballot.leader_id,
            payload: entry.payload.clone().into(),
        })
    }

    pub(crate) fn recompute_quorum(&mut self) {
        let voting_count = self.remote_replicas.iter().filter(|r| r.voting()).count()
            + u32::from(self.local_replica.voting()) as usize;
        self.cached_quorum = (voting_count / 2) + 1;
    }
}

/// Remote replica kind - either a real remote replica or a placeholder.
/// `Real` boxes `PxRemoteReplica` (~208 bytes) to keep the enum small
/// (`clippy::large_enum_variant`).
#[derive(Debug)]
pub(crate) enum RemoteReplicaKind {
    Real(Box<PxRemoteReplica>),
    Placeholder,
}

impl RemoteReplicaKind {
    pub(crate) fn endpoint(&self) -> Option<&str> {
        match self {
            Self::Real(r) => Some(r.endpoint.as_str()),
            Self::Placeholder => None,
        }
    }

    pub(crate) fn voting(&self) -> bool {
        match self {
            Self::Real(r) => r.voting,
            Self::Placeholder => false,
        }
    }

    pub(crate) fn as_real(&self) -> Option<&PxRemoteReplica> {
        match self {
            Self::Real(r) => Some(r.as_ref()),
            Self::Placeholder => None,
        }
    }
}

/// Production metrics helpers for `PxGroup`.
impl PxGroup {
    /// Number of in-flight (allocated-but-not-yet-chosen) proposals.
    /// Derived from all admission queues: sum of `window_per_queue -
    /// available_permits`.
    #[must_use]
    pub fn inflight_slot_count(&self) -> u64 {
        self.inflight.occupied()
    }
}

/// Result of a `PxGroup::propose` call.
#[derive(Clone, Debug)]
pub enum ProposeResult {
    Chosen {
        slot: u64,
    },
    NotLeader {
        leader_hint: String,
    },
    /// The proposer sliding window is full; the caller should retry shortly.
    /// Distinct from `Err` so the KV layer can surface a retryable signal.
    Busy,
    Err(String),
}

/// Result of one [`PxGroup::repair_once`] background-repair step.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RepairOutcome {
    /// A gap was found and chosen (recovered value or `NoOp` fill) at `slot`.
    Filled { slot: u64 },
    /// The contiguous frontier already reaches the highest seen slot; nothing
    /// to repair (no RPCs issued).
    NoGap,
    /// This replica is not the leader; repair is a leader-only duty.
    NotLeader,
    /// The gap slot could not be chosen this round (quorum/transport); a later
    /// poll retries.
    Failed,
}
