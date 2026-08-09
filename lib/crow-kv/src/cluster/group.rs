// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::missing_fields_in_debug)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use dashmap::DashMap;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::cluster::group_config::{GroupConfigStore, PxGroupConfig, PxGroupMember};
use crate::cluster::group_election::{LeaderElection, PendingLeaderHandoff, ReadBarrierOutcome, XorShift64};
use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::node_config::NodeConfigStore;
use crate::cluster::remote_replica::PxRemoteReplica;
use crate::cluster::replica::{
    PxReplicaError, Replica, ReplicaClient, ReplicaHandler, StepDownReply, StepDownRequestPayload,
};
use crate::cluster::status::{GroupStatus, InflightStatus, ReadStateView, StatusLevel};
use crate::common::config::{AdmissionPolicy, CrowKVConfig, PaxosConfig};
use crate::common::report::OperationReport;
use crate::metrics::{Counter, Gauge, LatencySummary};
use crate::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crate::paxos::roles::{
    Acceptor, DedupTag, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex,
};
use crate::paxos::{PxGroupId, PxNodeId};

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
/// concurrent round (the "multiple pipelines" path). The `timer` field
/// is reserved for watchdog use (currently a no-op).
#[derive(Default)]
pub(crate) struct PendingBatch {
    pub(crate) op_bodies: Vec<u8>,
    pub(crate) op_count: u16,
    pub(crate) tags: Vec<DedupTag>,
    pub(crate) waiters: Vec<tokio::sync::oneshot::Sender<ProposeResult>>,
    pub(crate) timer: Option<tokio::task::JoinHandle<()>>,
}

/// Watchdog interval (microseconds). A single long-running watchdog task
/// sleeps this long, then checks if there's been no coalescer activity
/// (enqueue or round completion) for this duration. If so, it flushes
/// any stuck non-empty batch. Only fires on inactivity — zero overhead
/// during normal operation.
const WATCHDOG_US: u64 = 1_000_000;

pub struct PxGroup {
    pub group_id: PxGroupId,
    cached_quorum: usize,
    local_replica: PxLocalReplica,
    pub(crate) remote_replicas: Vec<RemoteReplicaKind>,
    pub(crate) valid_replica_count: usize,
    pub(crate) next_slot: AtomicU64,
    /// Unified cluster configuration held by this group. Replaces the
    /// former individual `force_classic` / `wal_early_ack` /
    /// `async_engine_apply` bool fields and `election_cfg`. Set wholesale
    /// via [`Self::set_from_config`]; individual setters delegate into
    /// `self.config.*` so the held config stays the source of truth.
    pub(crate) config: CrowKVConfig,
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
            config: CrowKVConfig {
                wal_early_ack: false,
                async_engine_apply: false,
                ..CrowKVConfig::default()
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
            last_snapshot_time: parking_lot::Mutex::new(std::time::Instant::now()),
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
        };
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

    /// Apply all tunables from a `CrowKVConfig` wholesale: replaces the
    /// held config, mirrors the election lease onto the local replica,
    /// and reconstructs the inflight admission gate from the paxos
    /// params. Replaces the per-field setter calls at group creation
    /// and rebuild sites.
    pub fn set_from_config(&mut self, config: &CrowKVConfig) {
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
    pub fn config(&self) -> &CrowKVConfig {
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

    /// Seed the membership epoch from a persisted/prior value, without
    /// going through the bump logic in [`Self::add_remote_replica`] /
    /// [`Self::remove_remote_replica`]. Used by restart-from-disk restore
    /// (`apply_config`) and by the management-API rebuild pattern that
    /// reconstructs a fresh `PxGroup` and replays its prior remotes.
    pub fn set_membership_epoch(&self, epoch: u64) {
        self.membership_epoch.store(epoch, Ordering::Release);
    }

    /// Adopt `epoch` if it is higher than the current membership epoch,
    /// using a compare-and-swap loop so concurrent callers don't clobber
    /// each other. Returns the epoch now in effect.
    ///
    /// This is the "refresh its membership view" action the proto comment
    /// on `epoch_mismatch` prescribes: when a peer reports a higher epoch
    /// (because it observed a voting-set change we haven't seen yet), we
    /// converge upward so the next Prepare/Accept matches. We never lower
    /// the epoch — a stale proposer must not drag an acceptor backwards.
    pub fn adopt_membership_epoch(&self, epoch: u64) -> u64 {
        let mut current = self.membership_epoch.load(Ordering::Acquire);
        while epoch > current {
            match self
                .membership_epoch
                .compare_exchange(current, epoch, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    info!(
                        group_id = self.group_id,
                        old_epoch = current,
                        new_epoch = epoch,
                        "membership epoch adopted from peer (converging upward)"
                    );
                    return epoch;
                }
                Err(actual) => current = actual,
            }
        }
        current
    }

    /// Bump the membership epoch by exactly 1. Called from
    /// [`Self::add_remote_replica`] / [`Self::remove_remote_replica`]
    /// only when the mutation actually changes the *voting set* --
    /// see the field doc on `membership_epoch` for why non-voting
    /// changes must not bump it.
    fn bump_membership_epoch(&self) {
        let new_epoch = self.membership_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        info!(
            group_id = self.group_id,
            new_epoch, "membership epoch bumped (voting set changed)"
        );
    }

    /// Ask the local replica to step down as leader, if it currently is
    /// one. Self-targeted and self-termed: reads the replica's own live
    /// role/term rather than trusting an external caller's snapshot of
    /// them (which could be stale by the time the call lands), so no
    /// term needs to be threaded through from outside this process.
    /// Delegates to the strict-fence `handle_step_down` — a no-op
    /// (`accepted: false`) if this replica is not currently leader.
    pub fn step_down_if_leader(&self, reason: &str) -> StepDownReply {
        let replica = self.local_replica();
        let term = replica.current_term_snapshot();
        replica.handle_step_down(&StepDownRequestPayload {
            term,
            target_leader_id: replica.id,
            reason: reason.to_string(),
        })
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

    /// Record a voting peer's reported `contiguous_applied` and recompute the
    /// group safe-slot as the min over the local replica plus every voting
    /// peer's last-known applied. A peer that has never reported is treated as
    /// `0`, so the safe-slot only rises once *all* voting members are heard
    /// from — the conservative choice that preserves the bounded-stale read
    /// guarantee. Called from the leader heartbeat round.
    pub(crate) fn note_peer_applied(&self, peer_id: PxNodeId, applied: SlotIndex) {
        let mut peers = self.peer_applied.lock();
        peers.insert(peer_id, applied);
        let mut safe = self.local_replica.contiguous_applied();
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(r) = remote {
                if r.voting {
                    let peer_applied = peers.get(&r.node_id).copied().unwrap_or(0);
                    safe = safe.min(peer_applied);
                }
            }
        }
        drop(peers);
        // Monotonic within a tenure: a transient peer regression cannot pull
        // the published safe-slot backwards (it only ever advances).
        self.group_safe_slot.fetch_max(safe, Ordering::AcqRel);
    }

    /// Record a voting peer's reported `durable_snapshot_slot` and recompute
    /// the group's real "durable on leader + >=1 peer" snapshot watermark
    /// (`snapshot_slot`
    /// as `min(local durable_snapshot_slot, max(voting peer
    /// durable_snapshot_slot))`. A peer that has never reported is treated
    /// as `0` and simply never wins the max (same "absent = 0" convention as
    /// `note_peer_applied`) — unlike `group_safe_slot`, only *one* peer
    /// beyond the leader needs to have a slot durable, so the
    /// furthest-along peer alone is always a sufficient witness; a
    /// straggler peer must not hold this watermark back. Called from the
    /// leader heartbeat round, alongside `note_peer_applied`.
    pub(crate) fn note_peer_durable(&self, peer_id: PxNodeId, durable: SlotIndex) {
        let mut peers = self.peer_durable.lock();
        peers.insert(peer_id, durable);
        let mut best_peer = 0;
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(r) = remote {
                if r.voting {
                    let peer_durable = peers.get(&r.node_id).copied().unwrap_or(0);
                    best_peer = best_peer.max(peer_durable);
                }
            }
        }
        drop(peers);
        let local_durable = self.local_replica.wal().map_or(0, |w| w.snapshot_slot());
        let snapshot = local_durable.min(best_peer);
        // Monotonic within a tenure, same rationale as group_safe_slot.
        self.group_snapshot_slot.fetch_max(snapshot, Ordering::AcqRel);
    }

    /// Clear all peer-applied/-durable tracking and reset the published
    /// group safe-slot and snapshot-slot to `0`. Called at the start of
    /// every leader tenure: both watermarks only ever advance (via
    /// `fetch_max`) *within* a tenure, so without this reset a freshly
    /// elected leader would inherit the previous tenure's elevated
    /// watermarks and stale per-peer state — overstating freshness for
    /// bounded-stale reads / GC eligibility until new heartbeats arrive.
    /// After the reset both watermarks stay at `0` until heartbeats
    /// re-establish them, which is the conservative guarantee their docs
    /// promise.
    pub(crate) fn reset_safe_slot_tracking(&self) {
        self.peer_applied.lock().clear();
        self.group_safe_slot.store(0, Ordering::Release);
        self.peer_durable.lock().clear();
        self.group_snapshot_slot.store(0, Ordering::Release);
    }

    /// Number of remote replica slots (including placeholders).
    pub fn remote_replica_count(&self) -> usize {
        self.remote_replicas.len()
    }

    /// Get a remote replica by ID.
    pub fn get_remote_replica(&self, node_id: PxNodeId) -> Option<&PxRemoteReplica> {
        let idx = node_id as usize;
        self.remote_replicas.get(idx).and_then(|r| r.as_real())
    }

    pub fn member_endpoint(&self, node_id: PxNodeId) -> Option<&str> {
        let idx = node_id as usize;
        self.remote_replicas.get(idx).and_then(|r| r.endpoint())
    }

    /// Return the endpoint of the current leader, if known.
    /// Returns None if local replica is not the leader (caller should forward request).
    pub fn leader_endpoint(&self) -> Option<String> {
        if self.local_replica.is_leader() {
            // Local is leader, no need to forward
            return None;
        }
        // Local is not leader; look up the believed leader's remote endpoint.
        let leader_id = self.local_replica.believed_leader_id()?;
        let idx = leader_id as usize;
        self.remote_replicas
            .get(idx)
            .and_then(|r| r.endpoint())
            .map(str::to_string)
    }

    // ── Setters ───────────────────────────────────────────────────

    pub fn set_force_classic(&mut self, force: bool) {
        self.config.force_classic = force;
    }

    pub fn inherit_local_state_from(&mut self, prior: &Self) {
        self.local_replica = PxLocalReplica::new_inheriting_election_state(prior.local_replica());
        self.next_slot.store(
            self.next_slot
                .load(Ordering::Acquire)
                .max(prior.next_slot.load(Ordering::Acquire)),
            Ordering::Release,
        );
        self.proposing_term
            .store(prior.proposing_term.load(Ordering::Acquire), Ordering::Release);
        self.leader_read_ready
            .store(prior.leader_read_ready(), Ordering::Release);
        self.group_safe_slot
            .store(prior.group_safe_slot(), Ordering::Release);
    }

    pub fn set_next_slot(&self, next_slot: SlotIndex) {
        self.next_slot.store(next_slot.max(1), Ordering::Release);
    }

    /// Set up the group with a list of remote replicas.
    pub fn set_remote_replicas(&mut self, remote_replicas: Vec<PxRemoteReplica>) {
        let max_node_id = remote_replicas.iter().map(|r| r.node_id).max().unwrap_or(0);
        let vec_len = (max_node_id + 1) as usize;
        self.remote_replicas = (0..vec_len).map(|_| RemoteReplicaKind::Placeholder).collect();
        self.valid_replica_count = 0;

        for remote in remote_replicas {
            let idx = remote.node_id as usize;
            if idx < self.remote_replicas.len() {
                self.remote_replicas[idx] = RemoteReplicaKind::Real(Box::new(remote));
                self.valid_replica_count += 1;
            }
        }
        self.recompute_quorum();
    }

    /// Replace the endpoint of a remote replica, inserting it if the slot is
    /// a `Placeholder` or does not yet exist (upsert semantics).
    ///
    /// Returns the previous endpoint string when an existing `Real` replica
    /// was updated, or `None` when a new entry was inserted (from a
    /// `Placeholder` or by extending the vec).
    pub fn update_member_endpoint(
        &mut self,
        node_id: PxNodeId,
        endpoint: impl Into<String>,
    ) -> Option<String> {
        let endpoint = endpoint.into();
        let idx = node_id as usize;

        // Existing Real entry: update in place.
        if let Some(RemoteReplicaKind::Real(remote)) = self.remote_replicas.get_mut(idx) {
            let old_endpoint = remote.endpoint.clone();
            endpoint.clone_into(&mut remote.endpoint);
            return Some(old_endpoint);
        }

        // Placeholder or out-of-range: insert a new Real entry.
        warn!(
            group_id = self.group_id,
            node_id, "update_member_endpoint: inserting new remote (was placeholder or out-of-range)"
        );
        while idx >= self.remote_replicas.len() {
            self.remote_replicas.push(RemoteReplicaKind::Placeholder);
        }
        self.remote_replicas[idx] =
            RemoteReplicaKind::Real(Box::new(PxRemoteReplica::new(node_id, endpoint)));
        self.valid_replica_count = self
            .remote_replicas
            .iter()
            .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
            .count();
        self.recompute_quorum();
        None
    }

    /// Apply a persisted group configuration, wiring remote replicas from the
    /// durable config snapshot. The local replica is skipped.
    ///
    /// This is the restore-window seed (W9): a restarted node that loads a
    /// persisted config file starts with the same intended membership it had
    /// before the crash, instead of starting as a `quorum=1` singleton.
    pub fn apply_config(&mut self, config: &PxGroupConfig) {
        let local_id = self.local_replica().id;
        let remotes: Vec<PxRemoteReplica> = config
            .members
            .iter()
            .filter(|m| m.replica_id != local_id)
            .map(|m| PxRemoteReplica::new(m.replica_id, m.endpoint.clone()).with_voting(m.voting))
            .collect();
        self.set_remote_replicas(remotes);
        self.set_membership_epoch(config.membership_epoch);
        debug!(
            group_id = self.group_id,
            local_id,
            member_count = config.members.len(),
            membership_epoch = config.membership_epoch,
            "applied persisted group config"
        );
    }

    /// Persist the current group membership to a dedicated config file.
    ///
    /// Writes the local replica plus every real remote replica. Non-fatal on
    /// error: the group continues running but logs the failure.
    pub async fn persist_config(&self) {
        let local_id = self.local_replica().id;
        let term = self.local_replica().current_term_snapshot();
        let mut members = Vec::new();
        // Local replica's endpoint is set by the store when the group is
        // added, so all nodes share the same member list in the config file.
        members.push(PxGroupMember {
            replica_id: local_id,
            endpoint: self.local_replica().get_endpoint().unwrap_or_default(),
            voting: self.local_replica().voting(),
        });
        for remote in self.remote_replicas.iter().filter_map(|r| r.as_real()) {
            members.push(PxGroupMember {
                replica_id: remote.node_id,
                endpoint: remote.endpoint.clone(),
                voting: remote.voting,
            });
        }
        let config = PxGroupConfig {
            group_id: self.group_id,
            term,
            members,
            membership_epoch: self.membership_epoch(),
        };
        if let Some((store, sid, _gid)) = &self.node_config_store {
            if let Err(e) = store.save_group(*sid, &config, local_id).await {
                error!(
                    group_id = self.group_id,
                    term,
                    error = %e,
                    "persist group config to node-config.json failed"
                );
            } else {
                info!(
                    group_id = self.group_id,
                    term,
                    replica_count = config.members.len(),
                    "persisted group config to node-config.json"
                );
            }
        } else if let Some(store) = &self.config_store {
            if let Err(e) = store.save(&config).await {
                error!(
                    group_id = self.group_id,
                    term,
                    error = %e,
                    "persist group config failed"
                );
            } else {
                info!(
                    group_id = self.group_id,
                    term,
                    replica_count = config.members.len(),
                    "persisted group config to file"
                );
            }
        }
    }

    // ── Status ────────────────────────────────────────────────────

    /// Point-in-time status of this group: local replica + each remote.
    /// Used by `/topology`.
    #[must_use]
    pub fn status(&self) -> GroupStatus {
        let local_replica = self.local_replica.status();
        let remotes: Vec<_> = self
            .remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(r) => Some(r.status()),
                RemoteReplicaKind::Placeholder => None,
            })
            .collect();

        let local_status = local_replica.status;
        let mut status = local_status;
        let mut messages: Vec<_> = local_replica
            .messages
            .iter()
            .map(|msg| format!("local#{}: {msg}", self.local_replica.id))
            .collect();
        let mut ok_voting = u32::from(self.local_replica.voting() && local_status != StatusLevel::Unhealthy);

        for remote in &remotes {
            let remote_status = remote.status;
            if remote.voting && remote_status == StatusLevel::Ok {
                ok_voting += 1;
            }
            status = StatusLevel::worst(status, remote_status);
            messages.extend(
                remote
                    .messages
                    .iter()
                    .map(|msg| format!("remote#{}: {msg}", remote.id)),
            );
        }

        let quorum = self.cached_quorum as u32;
        if quorum > 0 && ok_voting < quorum {
            if status != StatusLevel::Unhealthy {
                status = StatusLevel::Degraded;
            }
            messages.push(format!(
                "group {}: only {ok_voting}/{} voting replicas reachable (quorum {quorum})",
                self.group_id,
                self.valid_replica_count + 1,
            ));
        }

        let read_state = self.read_handles.get().map(|h| ReadStateView {
            lease_valid: h.lease_valid.snapshot(),
            contiguous_applied: h.contiguous_applied.snapshot(),
            safe_slot: h.safe_slot.snapshot(),
        });

        GroupStatus {
            group_id: self.group_id,
            leader_id: self.leader_id(),
            local_replica_id: local_replica.id,
            force_classic: self.config.force_classic,
            status,
            messages,
            local_replica,
            remotes,
            inflight: Some(InflightStatus {
                window: self.inflight.window,
                policy: self.inflight.policy.label().to_string(),
                occupied: self.inflight.occupied(),
                waiting: self.inflight.waiting.load(Ordering::Relaxed),
                total_enqueued: self.inflight.total_enqueued.load(Ordering::Relaxed),
                total_wait_us: self.inflight.total_wait_us.load(Ordering::Relaxed),
            }),
            read_state,
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────

    /// Cascade shutdown through this group's replicas.
    ///
    /// Iterates real remote replicas and closes their gRPC channels, then
    /// shuts down the local replica (which in turn cascades through
    /// `acceptor` / `learner` / `slot_list` / `kv_store`). Continues on errors;
    /// aggregated `critical:` messages are returned.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(group_id = self.group_id, replica_l_id = self.local_replica.id)
    )]
    pub async fn shutdown(&self, per_layer_timeout: Duration) -> OperationReport {
        let mut report = OperationReport::new();
        info!(
            group_id = self.group_id,
            replica_l_id = self.local_replica.id,
            remote_count = self.valid_replica_count,
            "PxGroup shutdown starting"
        );

        // 0. Cancel the per-tenure token and await the election driver +
        //    engine maintenance loop (both share this same cancel source).
        //    Both are cooperative; a 100 ms scaffold tick is well within
        //    `per_layer_timeout`.
        self.tenure_cancel.cancel();
        if let Some(handle) = self.driver_handle.lock().await.take() {
            match tokio::time::timeout(per_layer_timeout, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    warn!(
                        group_id = self.group_id,
                        error = %join_err,
                        "election driver task panicked during shutdown"
                    );
                }
                Err(_) => {
                    warn!(
                        group_id = self.group_id,
                        timeout_ms = per_layer_timeout.as_millis() as u64,
                        "election driver task did not exit within per-layer timeout"
                    );
                }
            }
        }
        if let Some(handle) = self.maintenance_handle.lock().await.take() {
            match tokio::time::timeout(per_layer_timeout, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => {
                    warn!(
                        group_id = self.group_id,
                        error = %join_err,
                        "engine maintenance task panicked during shutdown"
                    );
                }
                Err(_) => {
                    warn!(
                        group_id = self.group_id,
                        timeout_ms = per_layer_timeout.as_millis() as u64,
                        "engine maintenance task did not exit within per-layer timeout"
                    );
                }
            }
        }
        // R65: stop the FetchGap driver.
        if let Some(handle) = self.fetchgap_handle.lock().await.take() {
            let _ = handle.await;
        }

        // 1. Close remote gRPC channels first so no in-flight RPCs spin.
        for remote in &self.remote_replicas {
            if let RemoteReplicaKind::Real(remote) = remote {
                let sub = remote.shutdown(per_layer_timeout).await;
                report.merge(sub);
            }
        }

        // 2. Shutdown local replica.
        let sub = self.local_replica.shutdown(per_layer_timeout).await;
        report.merge(sub);

        info!(
            group_id = self.group_id,
            error_count = report.errors.len(),
            "PxGroup shutdown complete"
        );
        report
    }

    // ── New-member snapshot join ────────────────────

    /// New-member snapshot join: pull a snapshot for this group from
    /// `peer_endpoint`'s [`crate::rpc::SnapshotService`], import it into
    /// the local engine, and seed the local learner's frontier so the
    /// group's normal repair/heartbeat catch-up only needs to stream the
    /// WAL tail above the snapshot's `at_slot` -- instead of replaying full
    /// Paxos history from slot 1.
    ///
    /// **Precondition:** must be called before this replica is wired into
    /// any group's topology (before any peer can send it `Accept`/
    /// `Heartbeat` RPCs) -- mirrors [`crate::paxos::learner::PxLearner::seed_resume_frontier`]'s
    /// "before any `learn` call" precondition. Intended for a
    /// freshly-constructed, still-empty local replica only; never call this
    /// on a replica with existing local state.
    ///
    /// Returns the snapshot's `at_slot` on success (the frontier this
    /// replica's learner was seeded to).
    ///
    /// # Errors
    /// Returns an error string on any transport, decode, or engine-import
    /// failure.
    pub async fn join_via_snapshot(&self, peer_endpoint: &str) -> Result<u64, String> {
        use crate::rpc::snapshot_service_client::SnapshotServiceClient;
        use crate::rpc::{snapshot_stream_item, SnapshotRequest};
        use tokio_stream::StreamExt;

        let mut client = SnapshotServiceClient::connect(format!("http://{peer_endpoint}"))
            .await
            .map_err(|e| format!("snapshot join: connect to {peer_endpoint} failed: {e}"))?;

        let mut stream = client
            .stream_snapshot(SnapshotRequest {
                group_id: self.group_id,
            })
            .await
            .map_err(|e| format!("snapshot join: StreamSnapshot rpc failed: {e}"))?
            .into_inner();

        let mut header: Option<(u64, u64)> = None;
        let mut bytes = Vec::new();
        while let Some(item) = stream.next().await {
            let item = item.map_err(|e| format!("snapshot join: stream error: {e}"))?;
            match item.payload {
                Some(snapshot_stream_item::Payload::Header(h)) => {
                    header = Some((h.term_at_slot, h.membership_epoch));
                }
                Some(snapshot_stream_item::Payload::Data(chunk)) => {
                    bytes.extend_from_slice(&chunk);
                }
                None => {}
            }
        }
        let (term_at_slot, membership_epoch) =
            header.ok_or_else(|| "snapshot join: stream ended without a header".to_string())?;

        let at_slot = self
            .local_replica
            .learner
            .engine()
            .snapshot_import(&bytes)
            .map_err(|e| format!("snapshot join: engine import failed: {e}"))?;

        info!(
            group_id = self.group_id,
            peer_endpoint,
            at_slot,
            term_at_slot,
            membership_epoch,
            stream_bytes = bytes.len(),
            "snapshot join: imported snapshot, seeding learner frontier"
        );
        self.local_replica
            .learner
            .seed_resume_frontier(at_slot, term_at_slot);
        // Seed the epoch fence from the exporting peer's current value --
        // without this, a fresh join always starts at epoch 0 and could
        // never receive a Prepare/Accept (even as a non-voting catch-up
        // learner) from a group that has ever had a membership change.
        self.set_membership_epoch(membership_epoch);
        Ok(at_slot)
    }

    // ── Add/Remove ────────────────────────────────────────────────

    /// Add a remote replica to the group.
    pub fn add_remote_replica(&mut self, remote: PxRemoteReplica) {
        info!(
            group_id = self.group_id,
            remote_id = remote.node_id,
            endpoint = remote.endpoint,
            "added remote replica to group"
        );

        let idx = remote.node_id as usize;
        // Ensure vec is large enough
        while idx >= self.remote_replicas.len() {
            self.remote_replicas.push(RemoteReplicaKind::Placeholder);
        }

        // Snapshot whether this node_id was already a voting member,
        // to decide below whether this mutation changes the voting set.
        let was_voting = matches!(&self.remote_replicas[idx], RemoteReplicaKind::Real(prev) if prev.voting);
        let new_voting = remote.voting;

        // Check if this was a placeholder before
        if matches!(self.remote_replicas[idx], RemoteReplicaKind::Placeholder) {
            self.valid_replica_count += 1;
        }

        self.remote_replicas[idx] = RemoteReplicaKind::Real(Box::new(remote));
        self.recompute_quorum();

        // Bump the epoch iff this mutation actually changes the voting
        // set (new voting member, promotion, demotion) -- not for a
        // plain non-voting add/re-add or an idempotent re-add at the
        // same voting status (e.g. an endpoint-only update).
        if was_voting != new_voting {
            self.bump_membership_epoch();
        }
    }

    /// Remove a remote replica by node ID. Returns true if it was present.
    pub fn remove_remote_replica(&mut self, node_id: PxNodeId) -> bool {
        let idx = node_id as usize;
        let was_voting = match self.remote_replicas.get(idx) {
            Some(RemoteReplicaKind::Real(prev)) => prev.voting,
            _ => return false,
        };
        info!(
            group_id = self.group_id,
            remote_id = node_id,
            "removed remote replica from group"
        );
        self.remote_replicas[idx] = RemoteReplicaKind::Placeholder;
        self.valid_replica_count -= 1;
        self.recompute_quorum();
        // Removing a non-voting member never changes the voting set.
        if was_voting {
            self.bump_membership_epoch();
        }
        true
    }

    /// Return info about all real remote replicas: `(node_id, endpoint,
    /// voting)`. Callers that rebuild a group (management-API remote
    /// add/remove) must carry `voting` through to the rebuilt
    /// [`PxRemoteReplica`] -- dropping it would silently re-promote a
    /// non-voting (e.g. still-catching-up
    /// remote back to voting on the next unrelated rebuild.
    pub fn remote_replica_info(&self) -> Vec<(PxNodeId, &str, bool)> {
        self.remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(remote) => {
                    Some((remote.node_id, remote.endpoint.as_str(), remote.voting))
                }
                RemoteReplicaKind::Placeholder => None,
            })
            .collect()
    }

    // ── Proposer ──────────────────────────────────────────────

    /// Propose an opaque payload through Paxos. Returns the slot if chosen,
    /// or an error string.
    ///
    /// When R45 coalescing is enabled (`coalesce_max_keys > 0` and the
    /// self-weak is set), the first op starts a Paxos round immediately
    /// and concurrent ops arriving during the round join the next batch
    /// (one slot, one quorum round per batch); each coalesced caller
    /// receives `ProposeResult::Chosen { slot }` for the shared slot.
    /// When coalescing is disabled (`coalesce_max_keys = 0`), this is the
    /// legacy one-proposal-per-key path.
    pub async fn propose(&self, payload: Vec<u8>, client_id: Option<u64>, seq: Option<u64>) -> ProposeResult {
        let replica = &self.local_replica;

        // Leadership gate. Checks BOTH:
        //   * role == Leader  -- captures the role atomic flipped by
        //     become_leader / become_follower.
        //   * current_term == proposing_term -- captures the case where
        //     the local replica advanced into a new term (became
        //     follower under HigherTerm, then re-elected) without the
        //     proposing tenure having stamped the new term yet.
        // Either miss surfaces as `NotLeader { hint: leader_endpoint }`
        // before slot allocation, draining in-flight client proposals.
        let role_is_leader = replica.role() == crate::cluster::local_replica::PxLocalReplicaRole::Leader;
        let current_term = replica.current_term_snapshot();
        let proposing_term = self.proposing_term.load(Ordering::Acquire);
        // Pinned-leader testkit groups construct the local replica with
        // role == Leader and never advance the term, so they pass the
        // gate with current_term == 0 == proposing_term. Production
        // leaders pass once `stamp_proposing_term` has run on tenure entry.
        let gate_pass = role_is_leader && current_term == proposing_term;
        if !gate_pass {
            return ProposeResult::NotLeader {
                leader_hint: self.leader_endpoint().unwrap_or_default(),
            };
        }

        // Idempotency: a retried `(client_id, seq)` that the learner has
        // already applied returns its prior commit slot without re-running
        // Paxos (exactly-once writes, idempotent retry). Checked before
        // window admission / coalescing so duplicates never consume a
        // window permit or enter a batch.
        if let (Some(cid), Some(s)) = (client_id, seq) {
            if let Some(cached_slot) = replica.learner.dedup_lookup(cid, s) {
                debug!(
                    group_id = self.group_id,
                    client_id = cid,
                    seq = s,
                    slot = cached_slot,
                    "dedup hit; returning cached commit without re-proposing"
                );
                return ProposeResult::Chosen { slot: cached_slot };
            }
        }

        let tag = dedup_tag(client_id, seq);

        // R45: event-driven coalescing. When `coalesce_max_keys > 0` and
        // self-weak is set, the first op starts a round immediately (no
        // timer) and ops arriving during the round join the next batch.
        let coalesce_on = self.config.paxos.coalesce_max_keys > 0 && self.self_weak.get().is_some();
        if coalesce_on {
            self.coalesce_enqueue(payload, tag).await
        } else {
            let tags: Vec<DedupTag> = tag.into_iter().collect();
            // `Bytes::from(Vec<u8>)` reuses the allocation (no copy) and
            // gives cheap `Clone` for the slot-retry loop and Accept fanout.
            self.propose_inner(bytes::Bytes::from(payload), &tags).await
        }
    }

    /// Drive one Paxos proposal (single- or multi-key) through to a chosen
    /// slot. Holds one inflight permit for the whole round, allocates one
    /// slot, and records every `dedup_tags` entry against the chosen slot
    /// on the local learner. The leadership gate is re-checked here so a
    /// step-down between coalescer batch collection and flush surfaces as
    /// `NotLeader` instead of racing into Paxos with stale identity.
    async fn propose_inner(&self, payload: bytes::Bytes, dedup_tags: &[DedupTag]) -> ProposeResult {
        let e2e_start = std::time::Instant::now();
        let result = self.propose_inner_impl(payload, dedup_tags).await;
        if let Some(h) = self.write_handles.get() {
            h.propose_e2e
                .observe(e2e_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    async fn propose_inner_impl(&self, payload: bytes::Bytes, dedup_tags: &[DedupTag]) -> ProposeResult {
        let replica = &self.local_replica;

        // Re-check the leadership gate (see `propose`).
        let role_is_leader = replica.role() == crate::cluster::local_replica::PxLocalReplicaRole::Leader;
        let current_term = replica.current_term_snapshot();
        let proposing_term = self.proposing_term.load(Ordering::Acquire);
        if !(role_is_leader && current_term == proposing_term) {
            return ProposeResult::NotLeader {
                leader_hint: self.leader_endpoint().unwrap_or_default(),
            };
        }

        // Sliding-window admission: cap concurrent in-flight proposals. The
        // permit is held for the whole proposal (released on drop at every
        // return path below). Depending on the admission policy, a full
        // window either fails fast with `Busy` (Reject) or blocks until a
        // permit is freed (Queue).
        let Some(_window_permit) = self.inflight.acquire_permit().await else {
            warn!(
                group_id = self.group_id,
                window = self.inflight.total_permits(),
                "inflight window full; rejecting proposal as Busy"
            );
            return ProposeResult::Busy;
        };

        let group_id = self.group_id;
        // Voting-only quorum (`self.quorum`/`cached_quorum`), *not*
        // `valid_replica_count + 1` -- the latter counts non-voting
        // catch-up members too and would inflate the threshold (and,
        // combined with unfiltered ack-counting, could also let
        // non-voting acks satisfy it -- see `run_accept_phase`'s
        // `remote.voting` guard).
        let quorum = self.quorum();
        let mut slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let mut last_error = String::new();

        trace!(
            group_id,
            my_id = self.local_replica.id,
            dedup_tags = dedup_tags.len(),
            peer_count = self.valid_replica_count,
            quorum,
            "start paxos proposal"
        );

        'slot_retry: for _slot_attempt in 0..PaxosConfig::DEFAULT.max_slot_retries {
            let base_entry = self.base_entry(slot, payload.clone());
            let mut force_prepare = self.config.force_classic; // Classic: always prepare; Leader: Phase-2 only
            let mut min_round = 0u64;

            for attempt in 0..PaxosConfig::DEFAULT.max_paxos_retries {
                let mut entry = base_entry.clone();
                let mut adopted_foreign_value = false;
                debug!(
                    group_id,
                    slot, attempt, force_prepare, min_round, "start paxos attempt"
                );

                if force_prepare {
                    match self
                        .run_prepare_phase(replica, slot, payload.clone(), quorum, min_round)
                        .await
                    {
                        PrepareAttempt::Proceed {
                            entry: prepared_entry,
                            foreign_value,
                        } => {
                            entry = prepared_entry;
                            adopted_foreign_value = foreign_value;
                        }
                        PrepareAttempt::Retry {
                            next_min_round,
                            error,
                        } => {
                            warn!(
                                group_id,
                                slot,
                                attempt,
                                next_min_round,
                                error = error.keyword(),
                                "prepare retry requested"
                            );
                            last_error = error.keyword().to_string();
                            min_round = next_min_round;
                            sleep(Self::retry_backoff(attempt)).await;
                            continue;
                        }
                        PrepareAttempt::Fail { error } => {
                            error!(group_id, slot, attempt, error = error.keyword(), "prepare failed");
                            if let PxPaxosError::TermStale { current_term } = &error {
                                warn!(
                                    group_id,
                                    slot, current_term, "stepping down: peer term observed during prepare"
                                );
                                replica.become_follower(*current_term);
                                return ProposeResult::NotLeader {
                                    leader_hint: self.leader_endpoint().unwrap_or_default(),
                                };
                            }
                            if let PxPaxosError::MembershipEpochMismatch { responder_epoch } = &error {
                                let adopted = self.adopt_membership_epoch(*responder_epoch);
                                warn!(
                                    group_id,
                                    slot,
                                    attempt,
                                    responder_epoch,
                                    adopted_epoch = adopted,
                                    "prepare epoch mismatch; adopted responder epoch, retrying same slot"
                                );
                                last_error = error.keyword().to_string();
                                sleep(Self::retry_backoff(attempt)).await;
                                continue;
                            }
                            last_error = error.keyword().to_string();
                            break;
                        }
                    }
                } else if min_round > entry.ballot.round {
                    entry.ballot.round = min_round;
                }

                match self.run_accept_phase(replica, &entry, dedup_tags, quorum).await {
                    AcceptAttempt::Chosen => {
                        // R17: when async_engine_apply is enabled, spawn
                        // the engine apply as a background task and return
                        // Chosen immediately. The fan_out_chosen_notice
                        // fires immediately too (non-blocking mpsc enqueue).
                        if self.config.async_engine_apply {
                            replica.spawn_learn_chosen(entry.clone(), dedup_tags);
                        } else {
                            replica.learn_chosen(&entry, dedup_tags).await;
                        }
                        self.fan_out_chosen_notice(&entry, group_id);
                        trace!(
                            group_id,
                            slot = entry.slot,
                            round = entry.ballot.round,
                            leader_id = entry.ballot.leader_id,
                            "paxos entry chosen and learned locally"
                        );

                        if adopted_foreign_value || entry.payload != payload {
                            last_error = PxPaxosError::ForeignValueChosen { slot }.keyword().to_string();
                            warn!(
                                group_id,
                                slot,
                                error = last_error,
                                "foreign value chosen; retrying client value on next slot"
                            );
                            slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
                            continue 'slot_retry;
                        }

                        return ProposeResult::Chosen { slot };
                    }
                    AcceptAttempt::Retry {
                        next_min_round,
                        error,
                    } => {
                        warn!(
                            group_id,
                            slot,
                            attempt,
                            next_min_round,
                            error = error.keyword(),
                            "accept retry requested; running prepare with higher ballot"
                        );
                        last_error = error.keyword().to_string();
                        min_round = next_min_round;
                        force_prepare = true;
                        sleep(Self::retry_backoff(attempt)).await;
                    }
                    AcceptAttempt::Fail { error } => {
                        error!(group_id, slot, attempt, error = error.keyword(), "accept failed");
                        if let PxPaxosError::TermStale { current_term } = &error {
                            warn!(
                                group_id,
                                slot, current_term, "stepping down: peer term observed during accept"
                            );
                            replica.become_follower(*current_term);
                            return ProposeResult::NotLeader {
                                leader_hint: self.leader_endpoint().unwrap_or_default(),
                            };
                        }
                        if let PxPaxosError::MembershipEpochMismatch { responder_epoch } = &error {
                            let adopted = self.adopt_membership_epoch(*responder_epoch);
                            warn!(
                                group_id,
                                slot,
                                attempt,
                                responder_epoch,
                                adopted_epoch = adopted,
                                "accept epoch mismatch; adopted responder epoch, retrying same slot"
                            );
                            last_error = error.keyword().to_string();
                            sleep(Self::retry_backoff(attempt)).await;
                            continue;
                        }
                        last_error = error.keyword().to_string();
                        break;
                    }
                }
            }

            warn!(
                group_id,
                slot, last_error, "slot proposal failed; retrying on next slot"
            );
            slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        }

        error!(
            group_id,
            last_error,
            max_paxos_retries = PaxosConfig::DEFAULT.max_paxos_retries,
            max_slot_retries = PaxosConfig::DEFAULT.max_slot_retries,
            "paxos proposal exhausted retry budget"
        );
        ProposeResult::Err(if last_error.is_empty() {
            "paxos retry exhausted".to_string()
        } else {
            format!(
                "{} (after {} paxos retries, {} slot retries)",
                last_error,
                PaxosConfig::DEFAULT.max_paxos_retries,
                PaxosConfig::DEFAULT.max_slot_retries
            )
        })
    }

    // ── R45 coalescer ─────────────────────────────────────

    /// Current time in microseconds since `UNIX_EPOCH`. Coarse — used only
    /// for watchdog inactivity detection.
    fn now_micros() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64)
    }

    /// Record coalescer activity (enqueue or round completion). The
    /// watchdog uses this to detect stuck batches.
    fn coalesce_touch_activity(&self) {
        self.coalesce_last_activity_us
            .store(Self::now_micros(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Start the single long-running watchdog task if not already running.
    /// Called lazily on first enqueue. The watchdog sleeps for
    /// `WATCHDOG_US`, then checks if there's been no activity for that
    /// duration. If so, it flushes any stuck non-empty batch.
    fn coalesce_start_watchdog(&self) {
        self.coalesce_watchdog_handle.get_or_init(|| {
            let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
                return tokio::spawn(async {});
            };
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_micros(WATCHDOG_US)).await;
                    let last = group
                        .coalesce_last_activity_us
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let now = Self::now_micros();
                    if now.saturating_sub(last) < WATCHDOG_US {
                        continue;
                    }
                    // No activity for WATCHDOG_US — check for stuck batch.
                    let batch = group.coalescer.lock().take();
                    let Some(mut batch) = batch else {
                        continue;
                    };
                    if let Some(timer) = batch.timer.take() {
                        timer.abort();
                    }
                    if batch.op_count > 0 {
                        tracing::warn!(
                            group_id = group.group_id,
                            op_count = batch.op_count,
                            "coalescer: watchdog flushing stuck batch"
                        );
                        group.coalesce_touch_activity();
                        group.coalesce_flush_batch(batch);
                    }
                    // Empty batch — just drop it (go idle).
                }
            })
        });
    }

    /// Flush a pending batch as a Paxos round. Opens a fresh pending
    /// batch for ops arriving during this round.
    fn coalesce_flush_batch(&self, batch: PendingBatch) {
        let mut payload = Vec::with_capacity(2 + batch.op_bodies.len());
        payload.extend_from_slice(&batch.op_count.to_le_bytes());
        payload.extend_from_slice(&batch.op_bodies);
        let payload = bytes::Bytes::from(payload);
        let tags = batch.tags;
        let waiters = batch.waiters;
        // Open a fresh pending batch for the next round.
        let new_batch = PendingBatch::default();
        let mut guard = self.coalescer.lock();
        if guard.is_none() {
            *guard = Some(new_batch);
        }
        let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
            return;
        };
        tokio::spawn(async move {
            #[cfg(feature = "test-util")]
            group.coalesce_await_round_gate().await;
            let result = group.propose_inner(payload, &tags).await;
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
            group.coalesce_drain_after_round();
        });
    }

    /// Enqueue one single-key op into the coalescer.
    ///
    /// - Idle (no batch): start a 1-op round immediately. Open a pending
    ///   batch for concurrent ops.
    /// - Batch exists: append to the pending batch. If the batch fills to
    ///   `max_keys`, flush it as a concurrent round.
    ///
    /// A single activity-based watchdog task runs in the background — it
    /// only fires if there's no coalescer activity for `WATCHDOG_US`.
    #[allow(clippy::type_complexity)]
    async fn coalesce_enqueue(&self, payload: Vec<u8>, tag: Option<DedupTag>) -> ProposeResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let op_body: &[u8] = payload.get(2..).unwrap_or(&[]);
        let max_keys = self.coalesce_max_keys.load(std::sync::atomic::Ordering::Relaxed);
        self.coalesce_touch_activity();
        self.coalesce_start_watchdog();

        // The locked section returns:
        //   Some((payload, tags, waiters)) → start a round now
        //   None → joined a batch, just await the oneshot
        let start_round: Option<(
            Vec<u8>,
            Vec<DedupTag>,
            Vec<tokio::sync::oneshot::Sender<ProposeResult>>,
        )> = {
            let mut guard = self.coalescer.lock();
            match &mut *guard {
                None => {
                    // Idle: start a 1-op round immediately.
                    let batch = PendingBatch::default();
                    *guard = Some(batch);
                    let mut round_payload = Vec::with_capacity(2 + op_body.len());
                    round_payload.extend_from_slice(&1u16.to_le_bytes());
                    round_payload.extend_from_slice(op_body);
                    let round_tags: Vec<DedupTag> = tag.into_iter().collect();
                    Some((round_payload, round_tags, vec![tx]))
                }
                Some(batch) => {
                    batch.op_bodies.extend_from_slice(op_body);
                    batch.op_count = batch.op_count.saturating_add(1);
                    if let Some(t) = tag {
                        batch.tags.push(t);
                    }
                    batch.waiters.push(tx);
                    if batch.op_count >= max_keys {
                        // Batch full: flush as a concurrent round.
                        let taken = std::mem::take(batch);
                        *guard = Some(PendingBatch::default());
                        let mut p = Vec::with_capacity(2 + taken.op_bodies.len());
                        p.extend_from_slice(&taken.op_count.to_le_bytes());
                        p.extend_from_slice(&taken.op_bodies);
                        if let Some(t) = taken.timer {
                            t.abort();
                        }
                        Some((p, taken.tags, taken.waiters))
                    } else {
                        None
                    }
                }
            }
        };

        // If None, we joined a batch — just await the result.
        let Some((payload, round_tags, round_waiters)) = start_round else {
            return match rx.await {
                Ok(result) => result,
                Err(_) => ProposeResult::Err("coalescer round dropped".to_string()),
            };
        };

        // Start the round. Spawn so the caller is not pinned to the paxos
        // round and can return via the oneshot.
        let payload = bytes::Bytes::from(payload);
        let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
            return ProposeResult::Err("group dropped".to_string());
        };
        tokio::spawn(async move {
            #[cfg(feature = "test-util")]
            group.coalesce_await_round_gate().await;
            let result = group.propose_inner(payload, &round_tags).await;
            for waiter in round_waiters {
                let _ = waiter.send(result.clone());
            }
            group.coalesce_drain_after_round();
        });

        match rx.await {
            Ok(result) => result,
            Err(_) => ProposeResult::Err("coalescer round dropped".to_string()),
        }
    }

    /// Called after a coalesced round completes. Drains the pending
    /// batch (ops that accumulated during the round). If non-empty,
    /// flushes it as the next round immediately. If empty, goes idle
    /// (coalescer → `None`) so the next op starts a 1-op round — the
    /// zero-latency-floor behavior at low load.
    ///
    /// R45b drain threshold: if the in-flight slot-task count
    /// (`occupied`) is at or above `coalesce_drain_threshold`, skip the
    /// drain — the `max_keys` overflow path handles high load with full
    /// batches, and draining here would fragment the batch (many
    /// slot-tasks racing to take one shared batch). The permit is
    /// already released before this call, so the last finisher always
    /// sees a count below threshold and takes the batch.
    fn coalesce_drain_after_round(&self) {
        self.coalesce_touch_activity();
        let threshold = self.config.paxos.coalesce_drain_threshold;
        if threshold > 0 && self.inflight.occupied() >= u64::try_from(threshold).unwrap_or(u64::MAX) {
            return;
        }
        let batch = self.coalescer.lock().take();
        let Some(mut batch) = batch else {
            return;
        };
        if let Some(timer) = batch.timer.take() {
            timer.abort();
        }
        if batch.op_count == 0 {
            // No ops arrived during the round — go idle.
            return;
        }
        // Flush the accumulated batch as the next round immediately.
        self.coalesce_flush_batch(batch);
    }

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

    fn base_entry(&self, slot: u64, payload: bytes::Bytes) -> PxLogEntry {
        PxLogEntry {
            slot,
            ballot: PxBallot::new(0, self.local_replica.id),
            term: self.local_replica.current_term_snapshot(),
            payload,
        }
    }

    fn consider_accepted(adopted: &mut Option<PxLogEntry>, candidate: PxLogEntry) {
        let should_replace = adopted
            .as_ref()
            .map_or(true, |current| candidate.ballot > current.ballot);
        if should_replace {
            *adopted = Some(candidate);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_prepare_phase(
        &self,
        replica: &PxLocalReplica,
        slot: u64,
        payload: bytes::Bytes,
        quorum: usize,
        min_round: u64,
    ) -> PrepareAttempt {
        let phase_start = std::time::Instant::now();
        let result = self
            .run_prepare_phase_impl(replica, slot, payload, quorum, min_round)
            .await;
        if let Some(h) = self.write_handles.get() {
            h.prepare_phase
                .observe(phase_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    async fn run_prepare_phase_impl(
        &self,
        replica: &PxLocalReplica,
        slot: u64,
        payload: bytes::Bytes,
        quorum: usize,
        min_round: u64,
    ) -> PrepareAttempt {
        use futures::stream::StreamExt;
        let mut max_round = min_round;
        if let Some(b) = replica.promised_at(slot).await {
            max_round = max_round.max(b.round);
        }

        let ballot = PxBallot {
            round: max_round + 1,
            leader_id: self.local_replica.id,
        };
        let group_id = self.group_id;
        debug!(
            group_id,
            slot,
            round = ballot.round,
            peer_count = self.valid_replica_count,
            quorum,
            "run prepare phase"
        );
        let mut entry = self.base_entry(slot, payload.clone());
        entry.ballot = ballot;
        let term = entry.term;

        let mut fold = ReplyFold::new();

        // E1: quorum short-circuit. When self_weak is set (all add_group
        // groups), fan out via FuturesUnordered with 'static futures
        // (capturing Arc<PxGroup>) so the proposer returns on quorum +
        // local-folded and detaches the remaining replies for side effects
        // (late TermStale → step down, late EpochMismatch → adopt epoch).
        // Fallback to join_all for groups without self_weak (unit-test
        // single-voter groups — no remotes, trivial).
        if let Some(group) = self.self_weak.get().and_then(Weak::upgrade) {
            let membership_epoch = self.membership_epoch();
            let mut futs: FuturesUnordered<
                Pin<Box<dyn Future<Output = TaggedPrepareReply> + Send + 'static>>,
            > = FuturesUnordered::new();
            // Local prepare (W6: counted before Proceed is returned).
            {
                let group = group.clone();
                futs.push(Box::pin(async move {
                    let r = <PxLocalReplica as ReplicaHandler>::on_prepare(
                        &group.local_replica,
                        slot,
                        ballot,
                        term,
                        group_id,
                    )
                    .await;
                    TaggedPrepareReply::Local(r)
                }));
            }
            // Remote prepares.
            for (idx, remote) in group.remote_replicas.iter().enumerate() {
                if let RemoteReplicaKind::Real(remote) = remote {
                    let voting = remote.voting;
                    let remote_id = remote.node_id;
                    let endpoint = remote.endpoint.clone();
                    let group = group.clone();
                    futs.push(Box::pin(async move {
                        let reply = match group.remote_replicas.get(idx) {
                            Some(RemoteReplicaKind::Real(r)) => {
                                r.send_prepare(slot, ballot, term, group_id, membership_epoch)
                                    .await
                            }
                            _ => Err(PxReplicaError::Internal(
                                "prepare: remote vanished mid-fanout".to_string(),
                            )),
                        };
                        TaggedPrepareReply::Remote {
                            voting,
                            remote_id,
                            endpoint,
                            reply,
                        }
                    }));
                }
            }
            while let Some(tagged) = futs.next().await {
                match tagged {
                    TaggedPrepareReply::Local(result) => match result {
                        Ok(reply) => fold.fold_prepare_local(replica.voting(), reply),
                        Err(error) => {
                            error!(
                                group_id,
                                slot,
                                replica_id = replica.id,
                                error = %error,
                                "local prepare handler failed"
                            );
                            fold.local_folded = true;
                        }
                    },
                    TaggedPrepareReply::Remote {
                        voting,
                        remote_id,
                        endpoint,
                        reply,
                        ..
                    } => {
                        let ctx = RemoteFoldCtx {
                            group_id,
                            slot,
                            remote_id,
                            proposer_epoch: membership_epoch,
                            phase: "prepare",
                        };
                        match reply {
                            Ok(reply) => fold.fold_prepare_remote(voting, &ctx, reply),
                            Err(error) => {
                                error!(
                                    group_id,
                                    slot,
                                    remote_id,
                                    endpoint = %endpoint,
                                    error = %error,
                                    "prepare rpc failed"
                                );
                            }
                        }
                    }
                }
                // E1 short-circuit: quorum + local folded + no disqualifying
                // TermStale (prepare checks TermStale before quorum, matching
                // the existing failure-decision order). Detach remaining
                // replies for side effects, then break to the Proceed path.
                if fold.accepted >= quorum && fold.local_folded && fold.highest_seen_term.is_none() {
                    let cancel = group.tenure_cancel.clone();
                    let group_drain = group.clone();
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                biased;
                                () = cancel.cancelled() => return,
                                Some(tagged) = futs.next() =>
                                    prepare_drain_side_effect(&group_drain, slot, tagged),
                                else => return,
                            }
                        }
                    });
                    break;
                }
            }
        } else {
            // Fallback: join_all (groups without self_weak — no remotes).
            let prepare_futs: Vec<_> = self
                .remote_replicas
                .iter()
                .filter_map(|remote| {
                    if let RemoteReplicaKind::Real(remote) = remote {
                        Some(remote.send_prepare(slot, ballot, term, group_id, self.membership_epoch()))
                    } else {
                        None
                    }
                })
                .collect();

            let (local_result, prepare_results) = tokio::join!(
                <PxLocalReplica as ReplicaHandler>::on_prepare(replica, slot, ballot, term, group_id),
                join_all(prepare_futs),
            );

            match local_result {
                Ok(reply) => fold.fold_prepare_local(replica.voting(), reply),
                Err(error) => {
                    error!(
                        group_id,
                        slot,
                        replica_id = replica.id,
                        error = %error,
                        "local prepare handler failed"
                    );
                    fold.local_folded = true;
                }
            }

            for (remote, result) in self
                .remote_replicas
                .iter()
                .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
                .zip(prepare_results)
            {
                let RemoteReplicaKind::Real(remote) = remote else {
                    continue;
                };
                let ctx = RemoteFoldCtx {
                    group_id,
                    slot,
                    remote_id: remote.node_id,
                    proposer_epoch: self.membership_epoch(),
                    phase: "prepare",
                };
                match result {
                    Ok(reply) => fold.fold_prepare_remote(remote.voting, &ctx, reply),
                    Err(error) => {
                        error!(
                            group_id,
                            slot,
                            remote_id = remote.node_id,
                            endpoint = remote.endpoint,
                            error = %error,
                            "prepare rpc failed"
                        );
                    }
                }
            }
        }

        let ReplyFold {
            accepted: promised,
            highest_rejected_round,
            highest_seen_term,
            epoch_mismatch,
            adopted,
            local_folded: _,
        } = fold;

        if let Some(new_term) = highest_seen_term {
            // A peer's `current_term > term`. The proposer is a stale leader;
            // bubble up `TermStale` so the group-level propose loop steps down.
            return PrepareAttempt::Fail {
                error: PxPaxosError::TermStale {
                    current_term: new_term,
                },
            };
        }

        if promised < quorum {
            if let Some(round) = highest_rejected_round {
                let error = PxPaxosError::PrepareRejected {
                    promised: PxBallot::new(round, 0),
                };
                let next_min_round = match error.retry_action() {
                    PxRetryAction::RetrySameSlot {
                        min_round: Some(round),
                        ..
                    } => round,
                    _ => round,
                };
                return PrepareAttempt::Retry {
                    next_min_round,
                    error,
                };
            }
            if let Some(responder_epoch) = epoch_mismatch {
                return PrepareAttempt::Fail {
                    error: PxPaxosError::MembershipEpochMismatch { responder_epoch },
                };
            }
            return PrepareAttempt::Fail {
                error: PxPaxosError::QuorumUnavailable {
                    phase: PxPaxosPhase::Prepare,
                },
            };
        }

        let mut foreign_value = false;
        if let Some(prev) = adopted {
            foreign_value = prev.payload != payload;
            if foreign_value {
                warn!(
                    group_id,
                    slot,
                    adopted_round = prev.ballot.round,
                    adopted_leader_id = prev.ballot.leader_id,
                    "prepare adopted foreign value"
                );
            }
            entry.payload = prev.payload;
        }
        PrepareAttempt::Proceed { entry, foreign_value }
    }

    pub(crate) async fn run_accept_phase(
        &self,
        replica: &PxLocalReplica,
        entry: &PxLogEntry,
        dedup_tags: &[DedupTag],
        quorum: usize,
    ) -> AcceptAttempt {
        let phase_start = std::time::Instant::now();
        let result = self
            .run_accept_phase_impl(replica, entry, dedup_tags, quorum)
            .await;
        if let Some(h) = self.write_handles.get() {
            h.accept_phase
                .observe(phase_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
        }
        result
    }

    async fn run_accept_phase_impl(
        &self,
        replica: &PxLocalReplica,
        entry: &PxLogEntry,
        dedup_tags: &[DedupTag],
        quorum: usize,
    ) -> AcceptAttempt {
        use futures::stream::StreamExt;
        let quorum_rpc_start = std::time::Instant::now();
        let mut fold = ReplyFold::new();
        let group_id = self.group_id;
        trace!(
            group_id,
            slot = entry.slot,
            round = entry.ballot.round,
            peer_count = self.valid_replica_count,
            quorum,
            "run accept phase"
        );

        // E1: quorum short-circuit. When self_weak is set, fan out via
        // FuturesUnordered with 'static futures (capturing Arc<PxGroup>) so
        // the proposer returns on quorum + local-folded and detaches the
        // remaining replies for side effects. Fallback to join_all for
        // groups without self_weak (unit-test single-voter groups).
        if let Some(group) = self.self_weak.get().and_then(Weak::upgrade) {
            let membership_epoch = self.membership_epoch();
            let entry_owned = entry.clone();
            let dedup_tags_owned = dedup_tags.to_vec();
            let slot = entry.slot;

            if self.config.wal_early_ack && self.cached_quorum > 1 {
                // R16b: split path — CAS only, persist deferred.
                let mut futs = build_accept_remote_futs(
                    &group,
                    &entry_owned,
                    &dedup_tags_owned,
                    group_id,
                    membership_epoch,
                );
                // Local CAS (infallible → wrapped in Ok to normalize).
                {
                    let group = group.clone();
                    let entry = entry_owned.clone();
                    futs.push(Box::pin(async move {
                        let r = group.local_replica.on_accept_inner(&entry).await;
                        TaggedAcceptReply::Local(Ok(r))
                    }));
                }
                let mut local_accepted = false;
                while let Some(tagged) = futs.next().await {
                    match tagged {
                        TaggedAcceptReply::Local(Ok(reply)) => {
                            local_accepted = matches!(reply, PxAcceptReply::Accepted { .. });
                            fold.fold_accept_local(replica.voting(), &reply);
                        }
                        TaggedAcceptReply::Local(Err(_)) => {
                            unreachable!("on_accept_inner is infallible")
                        }
                        TaggedAcceptReply::Remote {
                            voting,
                            remote_id,
                            endpoint,
                            reply,
                            ..
                        } => {
                            let ctx = RemoteFoldCtx {
                                group_id,
                                slot,
                                remote_id,
                                proposer_epoch: membership_epoch,
                                phase: "accept",
                            };
                            match reply {
                                Ok(reply) => fold.fold_accept_remote(voting, &ctx, &reply),
                                Err(error) => {
                                    error!(
                                        group_id, slot, remote_id, endpoint = %endpoint,
                                        error = %error, "accept rpc failed"
                                    );
                                }
                            }
                        }
                    }
                    // R16b short-circuit: local CAS + quorum.
                    if local_accepted && fold.accepted >= quorum {
                        let cancel = group.tenure_cancel.clone();
                        let group_drain = group.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    () = cancel.cancelled() => return,
                                    Some(tagged) = futs.next() =>
                                        accept_drain_side_effect(&group_drain, slot, tagged),
                                    else => return,
                                }
                            }
                        });
                        if let Some(h) = self.write_handles.get() {
                            h.accept_quorum_rpc.observe(
                                quorum_rpc_start
                                    .elapsed()
                                    .as_nanos()
                                    .try_into()
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        replica.spawn_accept_persist(entry.clone());
                        return AcceptAttempt::Chosen;
                    }
                }
            } else {
                // R16a: default path — local on_accept (CAS + WAL persist).
                let mut futs = build_accept_remote_futs(
                    &group,
                    &entry_owned,
                    &dedup_tags_owned,
                    group_id,
                    membership_epoch,
                );
                {
                    let group = group.clone();
                    let entry = entry_owned.clone();
                    futs.push(Box::pin(async move {
                        let r = <PxLocalReplica as ReplicaHandler>::on_accept(
                            &group.local_replica,
                            &entry,
                            group_id,
                        )
                        .await;
                        TaggedAcceptReply::Local(r)
                    }));
                }
                while let Some(tagged) = futs.next().await {
                    match tagged {
                        TaggedAcceptReply::Local(result) => match result {
                            Ok(reply) => fold.fold_accept_local(replica.voting(), &reply),
                            Err(error) => {
                                error!(
                                    group_id, slot, replica_id = replica.id,
                                    error = %error, "local accept handler failed"
                                );
                                fold.local_folded = true;
                            }
                        },
                        TaggedAcceptReply::Remote {
                            voting,
                            remote_id,
                            endpoint,
                            reply,
                            ..
                        } => {
                            let ctx = RemoteFoldCtx {
                                group_id,
                                slot,
                                remote_id,
                                proposer_epoch: membership_epoch,
                                phase: "accept",
                            };
                            match reply {
                                Ok(reply) => fold.fold_accept_remote(voting, &ctx, &reply),
                                Err(error) => {
                                    error!(
                                        group_id, slot, remote_id, endpoint = %endpoint,
                                        error = %error, "accept rpc failed"
                                    );
                                }
                            }
                        }
                    }
                    // R16a short-circuit: quorum + local folded (W6).
                    if fold.accepted >= quorum && fold.local_folded {
                        let cancel = group.tenure_cancel.clone();
                        let group_drain = group.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    () = cancel.cancelled() => return,
                                    Some(tagged) = futs.next() =>
                                        accept_drain_side_effect(&group_drain, slot, tagged),
                                    else => return,
                                }
                            }
                        });
                        if let Some(h) = self.write_handles.get() {
                            h.accept_quorum_rpc.observe(
                                quorum_rpc_start
                                    .elapsed()
                                    .as_nanos()
                                    .try_into()
                                    .unwrap_or(u64::MAX),
                            );
                        }
                        break;
                    }
                }
            }
        } else {
            // Fallback: join_all (groups without self_weak — no remotes).
            let accept_futs: Vec<_> = self
                .remote_replicas
                .iter()
                .filter_map(|remote| {
                    if let RemoteReplicaKind::Real(remote) = remote {
                        Some(remote.send_accept(entry, dedup_tags, group_id, self.membership_epoch()))
                    } else {
                        None
                    }
                })
                .collect();

            if self.config.wal_early_ack && self.cached_quorum > 1 {
                let (local_result, accept_results) =
                    tokio::join!(replica.on_accept_inner(entry), join_all(accept_futs),);

                let local_accepted = matches!(&local_result, PxAcceptReply::Accepted { .. });
                fold.fold_accept_local(replica.voting(), &local_result);

                for (remote, result) in self
                    .remote_replicas
                    .iter()
                    .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
                    .zip(accept_results)
                {
                    let RemoteReplicaKind::Real(remote) = remote else {
                        continue;
                    };
                    let ctx = RemoteFoldCtx {
                        group_id,
                        slot: entry.slot,
                        remote_id: remote.node_id,
                        proposer_epoch: self.membership_epoch(),
                        phase: "accept",
                    };
                    match result {
                        Ok(reply) => fold.fold_accept_remote(remote.voting, &ctx, &reply),
                        Err(error) => {
                            error!(
                                group_id, slot = entry.slot, remote_id = remote.node_id,
                                endpoint = remote.endpoint, error = %error, "accept rpc failed"
                            );
                        }
                    }
                }

                if local_accepted && fold.accepted >= quorum {
                    replica.spawn_accept_persist(entry.clone());
                    return AcceptAttempt::Chosen;
                }
            } else {
                let (local_result, accept_results) = tokio::join!(
                    <PxLocalReplica as ReplicaHandler>::on_accept(replica, entry, group_id),
                    join_all(accept_futs),
                );

                match local_result {
                    Ok(reply) => fold.fold_accept_local(replica.voting(), &reply),
                    Err(error) => {
                        error!(
                            group_id, slot = entry.slot, replica_id = replica.id,
                            error = %error, "local accept handler failed"
                        );
                        fold.local_folded = true;
                    }
                }

                for (remote, result) in self
                    .remote_replicas
                    .iter()
                    .filter(|r| matches!(r, RemoteReplicaKind::Real(_)))
                    .zip(accept_results)
                {
                    let RemoteReplicaKind::Real(remote) = remote else {
                        continue;
                    };
                    let ctx = RemoteFoldCtx {
                        group_id,
                        slot: entry.slot,
                        remote_id: remote.node_id,
                        proposer_epoch: self.membership_epoch(),
                        phase: "accept",
                    };
                    match result {
                        Ok(reply) => fold.fold_accept_remote(remote.voting, &ctx, &reply),
                        Err(error) => {
                            error!(
                                group_id, slot = entry.slot, remote_id = remote.node_id,
                                endpoint = remote.endpoint, error = %error, "accept rpc failed"
                            );
                        }
                    }
                }
            }
        }

        let ReplyFold {
            accepted,
            highest_rejected_round,
            highest_seen_term,
            epoch_mismatch,
            adopted: _,
            local_folded: _,
        } = fold;

        if accepted >= quorum {
            return AcceptAttempt::Chosen;
        }

        if let Some(new_term) = highest_seen_term {
            return AcceptAttempt::Fail {
                error: PxPaxosError::TermStale {
                    current_term: new_term,
                },
            };
        }

        if let Some(round) = highest_rejected_round {
            let error = PxPaxosError::AcceptRejected {
                promised: PxBallot::new(round, 0),
            };
            let next_min_round = match error.retry_action() {
                PxRetryAction::RetrySameSlot {
                    min_round: Some(round),
                    ..
                } => round,
                _ => round + 1,
            };
            return AcceptAttempt::Retry {
                next_min_round,
                error,
            };
        }
        if let Some(responder_epoch) = epoch_mismatch {
            return AcceptAttempt::Fail {
                error: PxPaxosError::MembershipEpochMismatch { responder_epoch },
            };
        }
        AcceptAttempt::Fail {
            error: PxPaxosError::QuorumUnavailable {
                phase: PxPaxosPhase::Accept,
            },
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

    fn recompute_quorum(&mut self) {
        let voting_count = self.remote_replicas.iter().filter(|r| r.voting()).count()
            + u32::from(self.local_replica.voting()) as usize;
        self.cached_quorum = (voting_count / 2) + 1;
    }

    fn retry_backoff(attempt: usize) -> Duration {
        let factor = 1u64 << attempt.min(6);
        let base = PaxosConfig::DEFAULT.retry_base_backoff_ms.saturating_mul(factor);
        // E4: ±50% jitter to decorrelate retry storms across replicas.
        // `jitter_mult` is in `[500, 1500]` (milli-units), so
        // `base * jitter_mult / 1000` gives `[base/2, base*3/2]`.
        let jitter_mult = retry_jitter_multiplier();
        Duration::from_millis(base.saturating_mul(jitter_mult) / 1000)
    }
}

thread_local! {
    static RETRY_JITTER_RNG: std::cell::RefCell<XorShift64> =
        std::cell::RefCell::new(XorShift64::new(seed_retry_jitter()));
}

fn seed_retry_jitter() -> u64 {
    // Seed from node id + monotonic nanos so two replicas starting
    // simultaneously get different sequences.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn retry_jitter_multiplier() -> u64 {
    RETRY_JITTER_RNG.with(|rng| {
        let r = rng.borrow_mut().next_u64() % 1001;
        500 + r
    })
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
    fn endpoint(&self) -> Option<&str> {
        match self {
            Self::Real(r) => Some(r.endpoint.as_str()),
            Self::Placeholder => None,
        }
    }

    fn voting(&self) -> bool {
        match self {
            Self::Real(r) => r.voting,
            Self::Placeholder => false,
        }
    }

    fn as_real(&self) -> Option<&PxRemoteReplica> {
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

/// Test-only hooks (compiled under the `test-util` feature). These expose
/// crate-internal mechanisms — the proposer admission semaphore, a single
/// repair step, and peer-applied injection — to integration tests under
/// `tests/` without permanently widening the production public API.
#[cfg(feature = "test-util")]
impl PxGroup {
    /// Acquire all inflight admission permits across all queues so a
    /// test can exhaust the window and observe `ProposeResult::Busy`
    /// (Reject mode) or blocking (Queue mode). Returns permits that
    /// release on drop.
    pub fn try_acquire_all_inflight_permits(&self) -> Vec<tokio::sync::SemaphorePermit<'_>> {
        self.inflight.try_acquire_all()
    }

    /// Borrow the inflight semaphore for tests that need direct access.
    #[must_use]
    pub fn inflight_semaphore(&self) -> &tokio::sync::Semaphore {
        &self.inflight.semaphore
    }

    /// Run one background-repair step, returning the slot that was filled
    /// (`Some`) or `None` when there was no gap to repair / repair did not
    /// choose. Wraps the internal [`Self::repair_once`].
    pub async fn repair_once_for_tests(&self) -> Option<u64> {
        match self.repair_once().await {
            RepairOutcome::Filled { slot } => Some(slot),
            RepairOutcome::NoGap | RepairOutcome::NotLeader | RepairOutcome::Failed => None,
        }
    }

    /// Inject a peer's reported `contiguous_applied` watermark, normally driven
    /// by the leader heartbeat round, so a test can exercise group-safe-slot
    /// computation deterministically. Wraps the internal [`Self::note_peer_applied`].
    pub fn note_peer_applied_for_tests(&self, peer_id: PxNodeId, applied: SlotIndex) {
        self.note_peer_applied(peer_id, applied);
    }

    /// Inject a peer's reported `durable_snapshot_slot` watermark, normally
    /// driven by the leader heartbeat round, so a test can exercise
    /// group-snapshot-slot computation deterministically. Wraps the
    /// internal [`Self::note_peer_durable`].
    pub fn note_peer_durable_for_tests(&self, peer_id: PxNodeId, durable: SlotIndex) {
        self.note_peer_durable(peer_id, durable);
    }

    /// Clear all peer-applied/-durable tracking and reset the published
    /// `group_safe_slot`/`group_snapshot_slot` to `0`, so a test can
    /// exercise the new-leader-tenure reset deterministically without
    /// driving a real election. Wraps the internal
    /// [`Self::reset_safe_slot_tracking`].
    pub fn reset_safe_slot_tracking_for_tests(&self) {
        self.reset_safe_slot_tracking();
    }

    /// Run one [`crate::cluster::group_maintenance`] pass synchronously,
    /// without spawning/waiting on the periodic loop's timer, so a test can
    /// exercise engine-snapshot / GC-watermark / WAL-GC wiring
    /// deterministically.
    pub async fn run_maintenance_pass_for_tests(&self) {
        crate::cluster::group_maintenance::run_pass(self).await;
    }

    /// Install a one-shot gate that holds the next `ReadIndex` heartbeat
    /// round open until `release` is consumed. The test keeps the
    /// `oneshot::Sender` and sends `()` once the batch of concurrent
    /// reads has been fired, so the round leader blocks long enough for
    /// the other reads to enqueue on the pending-barrier queue. Consumed
    /// by the first round that runs after this call.
    pub fn set_readindex_round_gate_for_tests(&self, release: tokio::sync::oneshot::Receiver<()>) {
        *self.readindex_round_gate.lock() = Some(release);
    }

    /// Whether a `ReadIndex` heartbeat round is currently in flight (i.e.
    /// a pending-barrier batch exists). Used by tests to wait until the
    /// round leader has registered its batch before firing the waiters.
    #[must_use]
    pub fn has_pending_read_barrier_for_tests(&self) -> bool {
        self.pending_read_barrier.lock().is_some()
    }

    /// Number of waiters currently queued on the in-flight `ReadIndex`
    /// round. Used by tests to confirm all concurrent reads have batched
    /// onto one round before releasing the gate.
    #[must_use]
    pub fn pending_read_barrier_waiters_for_tests(&self) -> usize {
        self.pending_read_barrier
            .lock()
            .as_ref()
            .map_or(0, |p| p.waiters.len())
    }

    /// Install a one-shot gate that holds the next coalescer round open
    /// until the test releases it, so concurrent ops deterministically
    /// join the pending batch. The test keeps the `oneshot::Sender` and
    /// sends `()` once the batch of concurrent ops has been fired.
    pub fn set_coalesce_round_gate_for_tests(&self, release: tokio::sync::oneshot::Receiver<()>) {
        *self.coalesce_round_gate.lock() = Some(release);
    }

    /// Whether a coalescer round is in flight (pending batch exists).
    #[must_use]
    pub fn has_coalesce_pending_for_tests(&self) -> bool {
        self.coalescer.lock().is_some()
    }

    /// Number of ops in the current pending batch.
    #[must_use]
    pub fn coalesce_pending_count_for_tests(&self) -> u16 {
        self.coalescer.lock().as_ref().map_or(0, |b| b.op_count)
    }

    /// Set the `max_keys` for testing.
    pub fn set_coalesce_max_keys_for_tests(&self, max_keys: u16) {
        self.coalesce_max_keys
            .store(max_keys, std::sync::atomic::Ordering::Relaxed);
    }
}

impl PxGroup {
    /// Test-only: await the coalesce round gate if set. Consumed by the
    /// first round that runs after the gate is installed.
    #[cfg(feature = "test-util")]
    async fn coalesce_await_round_gate(&self) {
        let gate = self.coalesce_round_gate.lock().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
    }
}

/// Build a dedup tag from the client-supplied `(client_id, seq)` options.
/// `None` when either is absent or `client_id == 0` (the no-dedup sentinel
/// matching `PxLearner::record_dedup_tags`).
fn dedup_tag(client_id: Option<u64>, seq: Option<u64>) -> Option<DedupTag> {
    match (client_id, seq) {
        (Some(cid), Some(s)) if cid != 0 => Some(DedupTag {
            client_id: cid,
            seq: s,
        }),
        _ => None,
    }
}

/// Inflight proposal admission gate. Owns a single semaphore of
/// `max_inflight` permits and supports both fail-fast (Reject) and
/// blocking (Queue) admission policies.
pub(crate) struct InflightAdmission {
    semaphore: tokio::sync::Semaphore,
    window: usize,
    policy: AdmissionPolicy,
    /// Cumulative count of proposals that entered the queue (did not
    /// get a fast-path permit).
    total_enqueued: AtomicU64,
    /// Cumulative wait time in microseconds.
    total_wait_us: AtomicU64,
    /// Current number of proposals waiting on `acquire().await`.
    waiting: AtomicU64,
}

impl InflightAdmission {
    fn new(max_inflight: usize, policy: AdmissionPolicy) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(max_inflight),
            window: max_inflight,
            policy,
            total_enqueued: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
            waiting: AtomicU64::new(0),
        }
    }

    /// Total permits.
    fn total_permits(&self) -> usize {
        self.window
    }

    /// Currently occupied permits.
    fn occupied(&self) -> u64 {
        let avail = self.semaphore.available_permits();
        u64::try_from(self.window.saturating_sub(avail)).unwrap_or(0)
    }

    /// Acquire a permit. Returns `None` if Reject mode and the window is
    /// full. In Queue mode, blocks until a permit is available.
    async fn acquire_permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        // Fast path: try to acquire without blocking.
        if let Ok(permit) = self.semaphore.try_acquire() {
            return Some(permit);
        }
        // Slow path depends on policy.
        match self.policy {
            AdmissionPolicy::Reject => None,
            AdmissionPolicy::Queue => {
                self.total_enqueued.fetch_add(1, Ordering::Relaxed);
                self.waiting.fetch_add(1, Ordering::Relaxed);
                let t0 = std::time::Instant::now();
                let permit = self.semaphore.acquire().await.expect("inflight semaphore closed");
                let wait_us = t0.elapsed().as_micros();
                self.waiting.fetch_sub(1, Ordering::Relaxed);
                self.total_wait_us
                    .fetch_add(u64::try_from(wait_us).unwrap_or(u64::MAX), Ordering::Relaxed);
                Some(permit)
            }
        }
    }

    /// Try to acquire all permits (test helper).
    #[cfg(feature = "test-util")]
    fn try_acquire_all(&self) -> Vec<tokio::sync::SemaphorePermit<'_>> {
        let mut held = Vec::new();
        while let Ok(p) = self.semaphore.try_acquire() {
            held.push(p);
        }
        held
    }
}

impl std::fmt::Debug for InflightAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InflightAdmission")
            .field("window", &self.window)
            .field("policy", &self.policy)
            .field("occupied", &self.occupied())
            .field("waiting", &self.waiting.load(Ordering::Relaxed))
            .finish()
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

pub(crate) enum PrepareAttempt {
    Proceed {
        entry: PxLogEntry,
        foreign_value: bool,
    },
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}

pub(crate) enum AcceptAttempt {
    Chosen,
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}

/// Logging context for a remote reply fold. The per-remote `EpochMismatch`
/// arm emits a `warn!` with peer attribution, so the fold method needs the
/// fields that vary per remote.
struct RemoteFoldCtx {
    group_id: u64,
    slot: u64,
    remote_id: PxNodeId,
    proposer_epoch: u64,
    /// "prepare" or "accept" — selects the warn message text.
    phase: &'static str,
}

/// Accumulator for the prepare/accept reply fold. Replaces the triplicated
/// inline `match` over `PxPrepareReply` / `PxAcceptReply` in
/// `run_prepare_phase` and `run_accept_phase` (both R16a and R16b paths).
///
/// `accepted` counts promises (prepare) or accepts (accept) from voting
/// replicas only. `local_folded` is set once the local reply has been
/// observed (Ok or Err) so the E1 quorum short-circuit can gate
/// `Chosen`/`Proceed` on the local reply being counted first (W6).
struct ReplyFold {
    accepted: usize,
    highest_rejected_round: Option<u64>,
    highest_seen_term: Option<u64>,
    epoch_mismatch: Option<u64>,
    adopted: Option<PxLogEntry>,
    local_folded: bool,
}

impl ReplyFold {
    fn new() -> Self {
        Self {
            accepted: 0,
            highest_rejected_round: None,
            highest_seen_term: None,
            epoch_mismatch: None,
            adopted: None,
            local_folded: false,
        }
    }

    fn note_rejected(&mut self, current_promised: PxBallot) {
        let candidate = current_promised.round;
        self.highest_rejected_round = Some(
            self.highest_rejected_round
                .map_or(candidate, |r| r.max(candidate)),
        );
    }

    fn note_term_stale(&mut self, new_term: u64) {
        self.highest_seen_term = Some(self.highest_seen_term.map_or(new_term, |t| t.max(new_term)));
    }

    /// Fold the local prepare reply. `EpochMismatch` is unreachable on the
    /// local acceptor.
    fn fold_prepare_local(&mut self, voting: bool, reply: PxPrepareReply) {
        match reply {
            PxPrepareReply::Promised { accepted, .. } => {
                if voting {
                    self.accepted += 1;
                }
                if let Some(prev) = accepted {
                    PxGroup::consider_accepted(&mut self.adopted, prev);
                }
            }
            PxPrepareReply::Rejected { current_promised, .. } => {
                self.note_rejected(current_promised);
            }
            PxPrepareReply::TermStale { new_term, .. } => {
                self.note_term_stale(new_term);
            }
            PxPrepareReply::EpochMismatch { .. } => {
                unreachable!("local on_prepare does not produce EpochMismatch")
            }
        }
        self.local_folded = true;
    }

    /// Fold a remote prepare reply. `EpochMismatch` is real and logged with
    /// peer attribution.
    fn fold_prepare_remote(&mut self, voting: bool, ctx: &RemoteFoldCtx, reply: PxPrepareReply) {
        match reply {
            PxPrepareReply::Promised { accepted, .. } => {
                if voting {
                    self.accepted += 1;
                }
                if let Some(prev) = accepted {
                    PxGroup::consider_accepted(&mut self.adopted, prev);
                }
            }
            PxPrepareReply::Rejected { current_promised, .. } => {
                self.note_rejected(current_promised);
            }
            PxPrepareReply::TermStale { new_term, .. } => {
                self.note_term_stale(new_term);
            }
            PxPrepareReply::EpochMismatch { responder_epoch } => {
                warn!(
                    group_id = ctx.group_id,
                    slot = ctx.slot,
                    remote_id = ctx.remote_id,
                    proposer_epoch = ctx.proposer_epoch,
                    responder_epoch,
                    "{} rejected by membership-epoch fence",
                    ctx.phase
                );
                self.epoch_mismatch = Some(responder_epoch);
            }
        }
    }

    /// Fold the local accept reply (R16a `Ok` arm and R16b infallible path
    /// share this — the reply-arm logic is identical). `EpochMismatch` is
    /// unreachable on the local acceptor.
    fn fold_accept_local(&mut self, voting: bool, reply: &PxAcceptReply) {
        match reply {
            PxAcceptReply::Accepted { .. } => {
                if voting {
                    self.accepted += 1;
                }
            }
            PxAcceptReply::Rejected { current_promised, .. } => {
                self.note_rejected(*current_promised);
            }
            PxAcceptReply::TermStale { new_term, .. } => {
                self.note_term_stale(*new_term);
            }
            PxAcceptReply::EpochMismatch { .. } => {
                unreachable!("local on_accept does not produce EpochMismatch")
            }
        }
        self.local_folded = true;
    }

    /// Fold a remote accept reply. `EpochMismatch` is real and logged with
    /// peer attribution.
    fn fold_accept_remote(&mut self, voting: bool, ctx: &RemoteFoldCtx, reply: &PxAcceptReply) {
        match reply {
            PxAcceptReply::Accepted { .. } => {
                if voting {
                    self.accepted += 1;
                }
            }
            PxAcceptReply::Rejected { current_promised, .. } => {
                self.note_rejected(*current_promised);
            }
            PxAcceptReply::TermStale { new_term, .. } => {
                self.note_term_stale(*new_term);
            }
            PxAcceptReply::EpochMismatch { responder_epoch } => {
                warn!(
                    group_id = ctx.group_id,
                    slot = ctx.slot,
                    remote_id = ctx.remote_id,
                    proposer_epoch = ctx.proposer_epoch,
                    responder_epoch,
                    "{} rejected by membership-epoch fence",
                    ctx.phase
                );
                self.epoch_mismatch = Some(*responder_epoch);
            }
        }
    }
}

/// Tagged reply from the prepare-phase fan-out. The local future and each
/// remote future produce one of these so a single `FuturesUnordered` can
/// fold them in arrival order (E1 quorum short-circuit). `remote_id` /
/// `endpoint` are captured at future-build time so the fold loop and the
/// detached drain do not need to re-look-up the remote (which may vanish
/// if the group is reconfigured mid-proposal).
enum TaggedPrepareReply {
    Local(Result<PxPrepareReply, PxReplicaError>),
    Remote {
        voting: bool,
        remote_id: PxNodeId,
        endpoint: String,
        reply: Result<PxPrepareReply, PxReplicaError>,
    },
}

/// Tagged reply from the accept-phase fan-out. The R16b local path produces
/// an infallible `PxAcceptReply` (wrapped in `Ok` to normalize); R16a local
/// and all remotes produce `Result<PxAcceptReply, PxReplicaError>`.
enum TaggedAcceptReply {
    Local(Result<PxAcceptReply, PxReplicaError>),
    Remote {
        voting: bool,
        remote_id: PxNodeId,
        endpoint: String,
        reply: Result<PxAcceptReply, PxReplicaError>,
    },
}

/// Apply a late prepare reply observed by the detached drain task (after the
/// proposer already short-circuited on quorum). Only `TermStale` and
/// `EpochMismatch` carry side effects the proposer must not lose; accepted /
/// rejected / error replies are no-ops here (the slot is already proceeding).
fn prepare_drain_side_effect(group: &Arc<PxGroup>, slot: u64, tagged: TaggedPrepareReply) {
    let group_id = group.group_id;
    let (remote_id, reply) = match tagged {
        TaggedPrepareReply::Local(_) => return,
        TaggedPrepareReply::Remote { remote_id, reply, .. } => (remote_id, reply),
    };
    match reply {
        Ok(PxPrepareReply::TermStale { new_term, .. }) => {
            warn!(
                group_id,
                slot, remote_id, new_term, "late prepare TermStale in drain; stepping down"
            );
            group.local_replica.become_follower(new_term);
        }
        Ok(PxPrepareReply::EpochMismatch { responder_epoch }) => {
            let adopted = group.adopt_membership_epoch(responder_epoch);
            warn!(
                group_id,
                slot,
                remote_id,
                responder_epoch,
                adopted_epoch = adopted,
                "late prepare EpochMismatch in drain; adopted responder epoch"
            );
        }
        _ => {}
    }
}

/// Apply a late accept reply observed by the detached drain task. Mirrors
/// [`prepare_drain_side_effect`] for the accept reply type.
fn accept_drain_side_effect(group: &Arc<PxGroup>, slot: u64, tagged: TaggedAcceptReply) {
    let group_id = group.group_id;
    let (remote_id, reply) = match tagged {
        TaggedAcceptReply::Local(_) => return,
        TaggedAcceptReply::Remote { remote_id, reply, .. } => (remote_id, reply),
    };
    match reply {
        Ok(PxAcceptReply::TermStale { new_term, .. }) => {
            warn!(
                group_id,
                slot, remote_id, new_term, "late accept TermStale in drain; stepping down"
            );
            group.local_replica.become_follower(new_term);
        }
        Ok(PxAcceptReply::EpochMismatch { responder_epoch }) => {
            let adopted = group.adopt_membership_epoch(responder_epoch);
            warn!(
                group_id,
                slot,
                remote_id,
                responder_epoch,
                adopted_epoch = adopted,
                "late accept EpochMismatch in drain; adopted responder epoch"
            );
        }
        _ => {}
    }
}

/// Build the `FuturesUnordered` of remote accept RPC futures for the E1
/// short-circuit path. Each future captures `Arc<PxGroup>` (so it is
/// `'static`) and resolves to a `TaggedAcceptReply::Remote`. Shared by the
/// R16b and R16a accept paths (only the local future differs).
fn build_accept_remote_futs(
    group: &Arc<PxGroup>,
    entry: &PxLogEntry,
    dedup_tags: &[DedupTag],
    group_id: u64,
    membership_epoch: u64,
) -> FuturesUnordered<Pin<Box<dyn Future<Output = TaggedAcceptReply> + Send + 'static>>> {
    let futs: FuturesUnordered<Pin<Box<dyn Future<Output = TaggedAcceptReply> + Send + 'static>>> =
        FuturesUnordered::new();
    for (idx, remote) in group.remote_replicas.iter().enumerate() {
        if let RemoteReplicaKind::Real(remote) = remote {
            let voting = remote.voting;
            let remote_id = remote.node_id;
            let endpoint = remote.endpoint.clone();
            let group = group.clone();
            let entry = entry.clone();
            let dedup_tags = dedup_tags.to_owned();
            futs.push(Box::pin(async move {
                let reply = match group.remote_replicas.get(idx) {
                    Some(RemoteReplicaKind::Real(r)) => {
                        r.send_accept(&entry, &dedup_tags, group_id, membership_epoch)
                            .await
                    }
                    _ => Err(PxReplicaError::Internal(
                        "accept: remote vanished mid-fanout".to_string(),
                    )),
                };
                TaggedAcceptReply::Remote {
                    voting,
                    remote_id,
                    endpoint,
                    reply,
                }
            }));
        }
    }
    futs
}

/// R65: Follower-side `FetchGap` driver. Periodically checks the local
/// replica's `gap_slots` set and sends `FetchGap` to the leader for each
/// gap. On reply, overwrites the stale/missing value and wakes the apply
/// loop. Bounded by `MAX_INFLIGHT_FETCHGAP`.
async fn run_fetchgap_driver(group: Weak<PxGroup>, group_id: PxGroupId, cancel: CancellationToken) {
    let tick = Duration::from_millis(10);
    loop {
        if cancel.is_cancelled() {
            return;
        }
        if sleep_or_cancel(&cancel, tick).await {
            return;
        }
        let Some(group) = group.upgrade() else {
            return;
        };
        // R65: update replication gauges periodically.
        group.local_replica().update_replication_gauges();
        // Only followers need FetchGap — the leader is the source of
        // truth and has no gaps.
        if group.local_replica().is_leader() {
            continue;
        }
        let leader_id = group.leader_id();
        if leader_id == 0 {
            continue;
        }
        let Some(remote) = group.get_remote_replica(leader_id) else {
            continue;
        };
        // R65: snapshot fallback — if gap count exceeds the threshold,
        // skip FetchGap and log a warning. A full automatic snapshot
        // install for running replicas is a follow-up; for now the
        // follower will eventually catch up via ChosenNotice + heartbeat.
        let gap_count = group.local_replica().gap_slots.lock().len();
        let threshold = group.config.election.catchup_snapshot_threshold;
        if gap_count > threshold as usize {
            tracing::warn!(
                group_id,
                gap_count,
                threshold,
                "gap count exceeds snapshot threshold; FetchGap skipped (snapshot fallback not yet automatic)"
            );
            continue;
        }
        // Clone the learner stream Arc so the spawned task can send
        // FetchGap without holding a reference to the group.
        let stream = remote.learner_stream().clone();
        let replica = group.local_replica();
        let term = replica.current_term_snapshot();
        let gaps = replica.drain_gaps_for_fetchgap();
        if gaps.is_empty() {
            continue;
        }
        replica.incr_fetchgap_sent(gaps.len() as u64);
        // Clone the Arc fields needed by the spawned task.
        let fetchgap_inflight = replica.fetchgap_inflight.clone();
        let gap_slots = replica.gap_slots.clone();
        let learner = replica.learner.clone();
        let acceptor = replica.acceptor.clone();
        let apply_notify = replica.apply_notify.clone();
        let fetchgap_received = replica
            .replication_handles
            .get()
            .map(|h| h.fetchgap_received.clone());
        let fetchgap_failed = replica
            .replication_handles
            .get()
            .map(|h| h.fetchgap_failed.clone());
        // Spawn a task per gap to send FetchGap concurrently.
        for slot in gaps {
            let stream_clone = stream.clone();
            let fetchgap_inflight_clone = fetchgap_inflight.clone();
            let gap_slots_clone = gap_slots.clone();
            let learner_clone = learner.clone();
            let acceptor_clone = acceptor.clone();
            let apply_notify_clone = apply_notify.clone();
            let fetchgap_received_clone = fetchgap_received.clone();
            let fetchgap_failed_clone = fetchgap_failed.clone();
            tokio::spawn(async move {
                let req = crate::rpc::FetchGapRequest {
                    version: 1,
                    group_id,
                    slot,
                    term,
                    leader_id,
                };
                let result = stream_clone.send_fetch_gap(req).await;
                fetchgap_inflight_clone.fetch_sub(1, Ordering::AcqRel);
                match result {
                    Ok(resp) => {
                        if let Some(c) = &fetchgap_received_clone {
                            c.inc();
                        }
                        debug!(
                            group_id,
                            slot,
                            term = resp.term,
                            ballot_round = resp.ballot_round,
                            leader_id = resp.leader_id,
                            payload_len = resp.payload.len(),
                            "FetchGap reply received"
                        );
                        let entry = crate::paxos::roles::PxLogEntry {
                            slot: resp.slot,
                            ballot: crate::paxos::roles::PxBallot::new(resp.ballot_round, resp.leader_id),
                            term: resp.term,
                            payload: bytes::Bytes::from(resp.payload),
                        };
                        acceptor_clone.accept(&entry).await;
                        learner_clone.update_chosen_frontier(resp.slot, resp.term);
                        apply_notify_clone.notify_one();
                    }
                    Err(err) => {
                        if let Some(c) = &fetchgap_failed_clone {
                            c.inc();
                        }
                        // Re-record the gap so the next tick retries.
                        gap_slots_clone.lock().insert(slot);
                        debug!(
                            group_id,
                            slot,
                            error = %err,
                            "FetchGap failed (will retry next tick)"
                        );
                    }
                }
            });
        }
        if sleep_or_cancel(&cancel, tick).await {
            return;
        }
    }
}

/// Returns `true` if cancelled during sleep, `false` if sleep completed.
async fn sleep_or_cancel(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(dur) => false,
    }
}
