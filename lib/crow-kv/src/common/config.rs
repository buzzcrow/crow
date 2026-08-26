// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Static configuration profiles for the Paxos retry budget, server
//! lifecycle, and per-group leader-election / heartbeat / lease tunables.
//!
//! All values here are compile-time `const`s exposed via `DEFAULT`
//! constants and `for_tests` constructors; runtime overrides happen at
//! the call sites (`crow-kv-server` CLI, testkit harness) before the
//! group is wrapped in an `Arc`.

use std::path::{Path, PathBuf};

use crow_common::config::BaseConfig;

use crate::wal::pipeline_backend::WalBlockAlignment;
use crate::wal::record::WalRecordFormat;

/// Admission policy for inflight proposals when the window is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum AdmissionPolicy {
    /// Fail fast with `ProposeResult::Busy`.
    Reject,
    /// Block the caller on `acquire().await` until a permit is freed.
    /// Default policy — eliminates client-side reject-retry storms.
    #[default]
    Queue,
}

impl AdmissionPolicy {
    /// Parse from a CLI string.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "reject" => Some(Self::Reject),
            "queue" => Some(Self::Queue),
            _ => None,
        }
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::Queue => "queue",
        }
    }
}

/// Paxos retry configuration (all fields static — bind at group
/// creation).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PaxosConfig {
    /// static: max paxos retries.
    pub max_paxos_retries: usize,
    /// static: max slot retries.
    pub max_slot_retries: usize,
    /// static: retry base backoff in milliseconds.
    pub retry_base_backoff_ms: u64,
    /// Maximum number of in-flight (allocated-but-not-yet-chosen) proposals
    /// static: maximum number of in-flight (allocated-but-not-yet-chosen)
    /// proposals the leader admits concurrently. A proposal that cannot
    /// acquire a window permit fails fast with `PxPaxosError::Busy`
    /// (retryable) rather than blocking, so the leader never stalls
    /// behind a saturated pipeline (parallel-slot window / performance
    /// targets).
    pub max_inflight_proposals: usize,
    /// static: admission policy when all permits are occupied: `Reject`
    /// (fail fast with `Busy`) or `Queue` (block until a permit is
    /// freed). Default `Reject`.
    pub inflight_admission: AdmissionPolicy,
    /// static: R45 max ops per coalesced batch. `0` disables coalescing
    /// (one proposal per key). Default 0 (opt-in).
    pub coalesce_max_keys: usize,
    /// static: R45b drain threshold — skip draining the pending batch
    /// in `coalesce_drain_after_round` when the in-flight slot-task
    /// count (`occupied`) is at or above this value. Lets the
    /// `max_keys` overflow path handle high load (full batches) while
    /// the drain maintains concurrency at low-moderate load. Library
    /// default `1`; the `crow-kv-server` CLI derives `max_inflight / 4`
    /// when `--coalesce-drain-threshold` is omitted. `0` = always
    /// drain (disables the heuristic).
    pub coalesce_drain_threshold: usize,
}

impl PaxosConfig {
    pub const DEFAULT: Self = Self {
        max_paxos_retries: 3,
        max_slot_retries: 3,
        retry_base_backoff_ms: 5,
        max_inflight_proposals: 32,
        inflight_admission: AdmissionPolicy::Queue,
        coalesce_max_keys: 0,
        coalesce_drain_threshold: 1,
    };
}

impl Default for PaxosConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Server-level configuration (all fields static — bind at startup).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ServerConfig {
    /// static: per-layer graceful-shutdown timeout in milliseconds.
    /// Shutdowns that take longer almost always indicate a stuck task and are
    /// better force-cleaned than waited on.
    pub shutdown_timeout_ms: u64,
    /// static: byte budget for scan responses: caps the total key+value
    /// bytes in one unary `KvScanResponse` so every page is provably
    /// bounded regardless of value sizes. The engine always returns at
    /// least one entry even if it alone exceeds the budget (so the
    /// client makes progress). Default 3.5 MiB leaves ~0.5 MiB for
    /// proto framing under the default 4 MiB limit
    /// `max_decoding_message_size`; tune down for low-latency
    /// interactive scans or up for bulk-export workloads (stay below
    /// the RPC frame ceiling). Post-R32 (custom Rust RPC) the ceiling
    /// may change — only this default's constraint value needs
    /// revisiting, not the knob itself.
    pub scan_byte_budget: usize,
}

impl ServerConfig {
    pub const DEFAULT: Self = Self {
        shutdown_timeout_ms: 10_000,
        scan_byte_budget: 3 * 1024 * 1024 + 512 * 1024, // 3.5 MiB
    };
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// WAL configuration for a single consensus group.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct WalConfig {
    /// static: directories to distribute WAL segments across.
    pub wal_disks: Vec<PathBuf>,
    /// static: target segment size before rotation (bytes). Default 64 MiB.
    pub wal_segment_size: u64,
    /// static: durable flush batch size trigger (bytes). Default 64 KiB.
    pub wal_flush_batch_bytes: usize,
    /// dynamic: safety-net timer that wakes the idle writer every
    /// `watchdog` ms to drain any queued records in case of a missed
    /// wake ("just in case for bugs"). Default 100 ms.
    pub wal_flush_watchdog_ms: u64,
    /// dynamic: disk-pressure watermark for eager GC. Default 80%.
    pub wal_disk_high_watermark_pct: u8,
    /// dynamic: forensics retention grace period (seconds). Default 3600
    /// (1 hour).
    pub wal_min_retention_secs: u64,
    /// dynamic: GC scan cadence (seconds). Default 30.
    pub gc_tick_secs: u64,
    /// static: whether the WAL backend uses block-aligned I/O (SSD/NVMe
    /// under `O_DIRECT`). When `false` (default), the file backend is
    /// used with byte-addressable media (RAM/SCM/PMEM). When `true`, a
    /// block pipeline is selected and `wal_io_unit_bytes` controls the
    /// alignment unit.
    pub wal_aligned: bool,
    /// static: SSD/NVMe I/O unit size in bytes. Only used when
    /// `wal_aligned` is `true`. Common values: 512 (logical sector),
    /// 4096 (default 4 KiB), 8192, 16384, 65536. Must be a power of
    /// two.
    pub wal_io_unit_bytes: usize,
    /// static: record encoding format. `Auto` selects binary frames
    /// (zero-copy) on all backends.
    pub wal_record_format: WalRecordFormat,
    /// static: skip the durable `fdatasync` on every write batch.
    /// Records are still written to the segment file, but the flush is
    /// not durable. Unsafe for production — only for benchmark
    /// path-overhead isolation (R10). Default `false`.
    pub wal_skip_fsync: bool,
}

impl WalConfig {
    #[must_use]
    pub fn with_root(wal_root: impl Into<PathBuf>) -> Self {
        Self {
            wal_disks: vec![wal_root.into()],
            ..Self::default()
        }
    }

    pub fn set_root(&mut self, wal_root: impl Into<PathBuf>) {
        self.wal_disks = vec![wal_root.into()];
    }

    /// Construct the `WalBlockAlignment` implied by this config.
    /// Returns `Unaligned` when `wal_aligned` is false, otherwise
    /// `Aligned { io_unit_bytes: wal_io_unit_bytes }`.
    #[must_use]
    pub(crate) fn alignment(&self) -> WalBlockAlignment {
        if self.wal_aligned {
            WalBlockAlignment::Aligned {
                io_unit_bytes: self.wal_io_unit_bytes,
            }
        } else {
            WalBlockAlignment::Unaligned
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_disks: vec![PathBuf::from("waldata")],
            wal_segment_size: 64 * 1024 * 1024,
            wal_flush_batch_bytes: 64 * 1024,
            wal_flush_watchdog_ms: 100,
            wal_disk_high_watermark_pct: 80,
            wal_min_retention_secs: 3600,
            gc_tick_secs: 30,
            wal_aligned: false,
            wal_io_unit_bytes: WalBlockAlignment::DEFAULT_IO_UNIT_BYTES,
            wal_record_format: WalRecordFormat::Auto,
            wal_skip_fsync: false,
        }
    }
}

/// Leader-election / heartbeat / lease tunables (all fields static —
/// bind at group creation; per group).
///
/// All time values are milliseconds, stored as `u64`. The election driver
/// converts to `Duration` at the consumption site.
///
/// Defaults target a single-datacenter deployment with NTP-disciplined
/// clocks. Rationale and cross-DC / WAN override profile documented
/// in the leader-election sub-design.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PxElectionConfig {
    /// Whether `PreVote` rounds protect against partition-rejoin disruption.
    pub prevote_enabled: bool,
    /// Leader's heartbeat tick interval.
    pub heartbeat_interval_ms: u64,
    /// Lower bound of the follower's randomized election deadline.
    pub election_min_ms: u64,
    /// Upper bound of the follower's randomized election deadline.
    pub election_max_ms: u64,
    /// Election-side lease duration; also the follower's `vote_lockout_until`
    /// extension on every heartbeat.
    pub lease_duration_ms: u64,
    /// Maximum admissible clock skew across the cluster. Subtracted from the
    /// leader's `lease_read_until` to remain safe under skew.
    pub max_clock_skew_ms: u64,
    /// Slots scanned per bulk-Phase-1 batch (new-leader open-prefix repair).
    pub bulk_prepare_window: u64,
    /// R65: gap count threshold for snapshot fallback. When a follower's
    /// gap count exceeds this, `FetchGap` is skipped and a warning is logged
    /// (full automatic snapshot install for running replicas is a
    /// follow-up). Default matches `bulk_prepare_window` (1024).
    pub catchup_snapshot_threshold: u64,
    /// Test-only override: when `true`, the election driver task is not
    /// spawned. Used by `testkit::cluster::start_cluster` to keep legacy M1/M2
    /// tests deterministic (pinned leader via `set_leader_id`).
    pub election_driver_disabled: bool,
    /// Bounded capacity of the per-peer `PxLearnerStream` outbound mpsc.
    /// Full mpsc surfaces as `PxPaxosError::Busy` on the proposer side
    /// (already classified `FailRetryable`).
    ///
    /// Derived as `max_inflight_proposals * LEARNER_WINDOW_MULTIPLIER` (4×) so
    /// that the learner channel always has headroom over the proposer
    /// admission gate. Only `max_inflight_proposals` needs to be tuned; this
    /// field follows automatically.
    pub learner_stream_window_frames: usize,
    /// Tick interval for the per-group engine-durability + WAL-GC
    /// maintenance loop (follow-up; see
    /// `cluster::group_maintenance`). Previously hardcoded as
    /// `group_maintenance::DEFAULT_MAINTENANCE_TICK`; now a normal
    /// per-group tunable like the other fields here.
    pub maintenance_tick_ms: u64,
    /// Minimum slot advance since the last durable snapshot before
    /// `persist_snapshot()` is called again. `flush()` still runs every
    /// tick; this only gates the expensive disk-write path.
    pub snapshot_slot_threshold: u64,
    /// Maximum wall-clock time since the last durable snapshot before
    /// `persist_snapshot()` is called again, in milliseconds. Ensures a
    /// low-write-rate replica still checkpoints periodically.
    pub snapshot_time_threshold_ms: u64,
    /// Per-RPC deadline for unary `prepare`, the bidi `accept`
    /// learner-stream call, and the unary `heartbeat` RPC, in
    /// milliseconds. On expiry the caller gets a retryable
    /// `PxReplicaError` and the pending-map entry (bidi path) is removed
    /// so it cannot leak. Paired with h2 keepalive on the connect-time
    /// `Endpoint` so a hung peer (accepts connection but never replies)
    /// is detected within the deadline rather than stalling the proposer
    /// indefinitely.
    pub learner_stream_rpc_timeout_ms: u64,
}

impl PxElectionConfig {
    /// Multiplier applied to `PaxosConfig::max_inflight_proposals` to derive
    /// `learner_stream_window_frames`. Gives the learner channel 4×
    /// headroom over the proposer admission gate.
    pub(crate) const LEARNER_WINDOW_MULTIPLIER: usize = 4;

    /// Production / single-DC default.
    ///
    /// Heartbeat 150 ms / election 1–2 s / lease 3 s. Follows etcd's
    /// production defaults (100 ms heartbeat, 1 s election) with a slightly
    /// conservative heartbeat for disk-fsync jitter. Lease ≥ `election_max`
    /// + `clock_skew` (2000 + 500 = 2500) ensures leader-lease safety.
    ///
    /// `learner_stream_window_frames` is derived as
    /// `PaxosConfig::DEFAULT.max_inflight_proposals * LEARNER_WINDOW_MULTIPLIER`
    /// (= 32 × 4 = 128).
    pub const DEFAULT: Self = Self {
        prevote_enabled: true,
        heartbeat_interval_ms: 150,
        election_min_ms: 1000,
        election_max_ms: 2000,
        lease_duration_ms: 3000,
        max_clock_skew_ms: 500,
        bulk_prepare_window: 1024,
        catchup_snapshot_threshold: 1024,
        election_driver_disabled: false,
        learner_stream_window_frames: PaxosConfig::DEFAULT.max_inflight_proposals
            * Self::LEARNER_WINDOW_MULTIPLIER,
        // Maintenance loop tick: flush L0→L1 + GC watermark check every
        // tick; durable snapshot gated by thresholds above.
        maintenance_tick_ms: 10_000,
        snapshot_slot_threshold: 100_000,
        snapshot_time_threshold_ms: 600_000,
        learner_stream_rpc_timeout_ms: 2000,
    };

    /// Aggressive timings for `#[tokio::test(start_paused = true)]` suites.
    ///
    /// Heartbeat 5 ms / election 30–60 ms / lease 25 ms. Not exposed on the
    /// `crow-kv-server` CLI.
    #[must_use]
    pub const fn for_tests() -> Self {
        Self {
            prevote_enabled: true,
            heartbeat_interval_ms: 5,
            election_min_ms: 30,
            election_max_ms: 60,
            lease_duration_ms: 25,
            max_clock_skew_ms: 1,
            bulk_prepare_window: 1024,
            catchup_snapshot_threshold: 1024,
            election_driver_disabled: false,
            learner_stream_window_frames: PaxosConfig::DEFAULT.max_inflight_proposals
                * Self::LEARNER_WINDOW_MULTIPLIER,
            maintenance_tick_ms: 500,
            snapshot_slot_threshold: 1000,
            snapshot_time_threshold_ms: 1_000,
            learner_stream_rpc_timeout_ms: 500,
        }
    }

    /// E2E / Playwright + benchmark profile: fast election but stable
    /// under real scheduling jitter.
    ///
    /// Election 300–600 ms / heartbeat 100 ms / lease 800 ms. Matches the
    /// Raft paper's 150–300 ms suggestion with a 2× margin for localhost
    /// parallel-test load. Lease ≥ `election_max` + `clock_skew` (600 + 100
    /// = 700) ensures leader-lease safety. `learner_stream_window_frames`
    /// = 32 × 4 = 128. Maintenance tick 3 s, snapshot time threshold 9 s
    /// so a 15 s bench triggers exactly one time-threshold snapshot
    /// (at the third tick, t≈9 s) while exercising more flush/GC passes.
    /// Slot threshold 1,000,000 avoids slot-triggered snapshots firing
    /// on every tick at high throughput.
    #[must_use]
    pub const fn for_e2e() -> Self {
        Self {
            prevote_enabled: true,
            heartbeat_interval_ms: 100,
            election_min_ms: 300,
            election_max_ms: 600,
            lease_duration_ms: 800,
            max_clock_skew_ms: 100,
            bulk_prepare_window: 1024,
            catchup_snapshot_threshold: 1024,
            election_driver_disabled: false,
            learner_stream_window_frames: PaxosConfig::DEFAULT.max_inflight_proposals
                * Self::LEARNER_WINDOW_MULTIPLIER,
            maintenance_tick_ms: 3_000,
            snapshot_slot_threshold: 1_000_000,
            snapshot_time_threshold_ms: 9_000,
            learner_stream_rpc_timeout_ms: 1000,
        }
    }

    /// Derive the learner stream window for a given max-inflight-proposals
    /// count. Call this when customizing `max_inflight_proposals` at runtime
    /// so the learner channel stays in sync.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn learner_window_for(max_inflight_proposals: usize) -> usize {
        max_inflight_proposals * Self::LEARNER_WINDOW_MULTIPLIER
    }
}

impl Default for PxElectionConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Unified configuration for a `CrowKV` cluster node.
///
/// Merges all sub-configs (`ServerConfig`, `PaxosConfig`,
/// `PxElectionConfig`, `WalConfig`) and the former `PxGroup` internal
/// flags (`force_classic`, `wal_early_ack`, `async_engine_apply`) into
/// one struct with `serde` derives for TOML file loading. Runtime
/// paths and backends are `#[serde(skip)]` — set from CLI args after
/// loading.
///
/// Usage: `crow_common::config::load_from_file::<CrowKVConfig>(path)`
/// or `CrowKVConfig::default()`, then CLI args override individual
/// fields, then pass `&CrowKVConfig` to `create_group_with_wal`.
///
/// All top-level fields are static (bind at startup / group creation).
/// Dynamic fields are inside `WalConfig` (GC tunables).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct CrowKVConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub paxos: PaxosConfig,
    #[serde(default)]
    pub election: PxElectionConfig,
    #[serde(default)]
    pub wal: WalConfig,
    /// static: force classic paxos path (no pipelining).
    #[serde(default)]
    pub force_classic: bool,
    /// static: early WAL ack timing.
    #[serde(default)]
    pub wal_early_ack: bool,
    /// static: async engine apply (R35).
    #[serde(default)]
    pub async_engine_apply: bool,
    /// static: WAL root directory. `#[serde(skip)]` — set from
    /// `--wal-root` CLI.
    #[serde(skip)]
    pub wal_root: PathBuf,
    /// static: group config root directory. `#[serde(skip)]` — set
    /// from CLI.
    #[serde(skip)]
    pub config_root: PathBuf,
    /// static: crowtree data root directory. `#[serde(skip)]` — set
    /// from CLI.
    #[serde(skip)]
    pub data_root: PathBuf,
    /// static: WAL I/O backend label. `#[serde(skip)]` — set from
    /// `--wal-backend`.
    #[serde(skip)]
    pub wal_backend: String,
    /// static: crowtree storage backend label. `#[serde(skip)]` — set
    /// from `--kv-backend`.
    #[serde(skip)]
    pub crowtree_backend: String,
    /// static: skip durable fdatasync. `#[serde(skip)]` — set from
    /// `--no-fsync`.
    #[serde(skip)]
    pub wal_skip_fsync: bool,
    /// static: log directory. `#[serde(skip)]` — set from CLI.
    #[serde(skip)]
    pub log_dir: String,
    /// static: node root directory (the `--root` CLI arg). The four
    /// path fields above are derived from it via `apply_root`.
    /// `#[serde(skip)]` — set from CLI. Used to populate the
    /// kv-server keep-alive's `data_root` field in group 0.
    #[serde(skip)]
    pub node_root: Option<PathBuf>,
}

impl BaseConfig for CrowKVConfig {
    fn validate(&self) -> Result<(), String> {
        if self.server.shutdown_timeout_ms == 0 {
            return Err("server.shutdown_timeout_ms must be > 0".to_string());
        }
        if self.server.scan_byte_budget == 0 {
            return Err("server.scan_byte_budget must be > 0".to_string());
        }
        if self.paxos.max_inflight_proposals == 0 {
            return Err("paxos.max_inflight_proposals must be > 0".to_string());
        }
        if self.election.election_min_ms >= self.election.election_max_ms {
            return Err(format!(
                "election.election_min_ms ({}) must be < election_max_ms ({})",
                self.election.election_min_ms, self.election.election_max_ms,
            ));
        }
        Ok(())
    }

    fn fill_skip_defaults(&mut self) {
        if self.wal_root.as_os_str().is_empty() {
            self.wal_root = PathBuf::from("waldata");
        }
        if self.config_root.as_os_str().is_empty() {
            self.config_root = PathBuf::from("conf");
        }
        if self.data_root.as_os_str().is_empty() {
            self.data_root = PathBuf::from("ctdata");
        }
        if self.wal_backend.is_empty() {
            self.wal_backend = "file".to_string();
        }
        if self.crowtree_backend.is_empty() {
            self.crowtree_backend = "file".to_string();
        }
        if self.log_dir.is_empty() {
            self.log_dir = "log".to_string();
        }
    }
}

impl Default for CrowKVConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::DEFAULT,
            paxos: PaxosConfig::DEFAULT,
            election: PxElectionConfig::DEFAULT,
            wal: WalConfig::default(),
            force_classic: false,
            wal_early_ack: true,
            // R35: async engine apply on by default — the Linearizable
            // read path's apply fence (`PxLearner::await_applied`) preserves
            // read-your-writes, so moving `learn_chosen` off the write
            // critical path is safe. Test profiles (`for_tests`) and the
            // `PxGroup::new` test path opt back out for determinism.
            async_engine_apply: true,
            wal_root: PathBuf::from("waldata"),
            config_root: PathBuf::from("conf"),
            data_root: PathBuf::from("ctdata"),
            wal_backend: "file".to_string(),
            crowtree_backend: "file".to_string(),
            wal_skip_fsync: false,
            log_dir: "log".to_string(),
            node_root: None,
        }
    }
}

impl CrowKVConfig {
    /// Load from a TOML file. Delegates to
    /// `crow_common::config::load_from_file` (fills skip defaults +
    /// validates). Kept for backward compatibility with existing
    /// call sites.
    ///
    /// # Errors
    /// Returns `Err(message)` if the file cannot be read, parsed, or
    /// fails validation.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, String> {
        crow_common::config::load_from_file::<Self>(path)
    }

    /// Test profile — fast election timings, `wal_early_ack` off.
    #[must_use]
    pub fn for_tests() -> Self {
        Self {
            election: PxElectionConfig::for_tests(),
            // Keep tests synchronous/deterministic — R17's spawned apply
            // introduces timing nondeterminism. Tests that exercise R17 opt
            // in via `set_async_engine_apply(true)`.
            async_engine_apply: false,
            ..Self::default()
        }
    }

    /// E2E / benchmark profile — stable under real scheduling jitter.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn for_e2e() -> Self {
        Self {
            election: PxElectionConfig::for_e2e(),
            ..Self::default()
        }
    }

    /// Max in-flight proposals (convenience accessor for
    /// `paxos.max_inflight_proposals`).
    #[must_use]
    pub fn max_inflight(&self) -> usize {
        self.paxos.max_inflight_proposals
    }

    /// Admission policy (convenience accessor for
    /// `paxos.inflight_admission`).
    #[must_use]
    pub fn inflight_admission(&self) -> AdmissionPolicy {
        self.paxos.inflight_admission
    }

    /// Derive the four runtime paths from a node root directory using
    /// the fixed on-disk layout: `wal_root = root/waldata`,
    /// `config_root = root/conf`, `data_root = root/ctdata`,
    /// `log_dir = root/log`. Also records `root` in `node_root` so the
    /// keep-alive loop can publish it to group 0.
    pub fn apply_root(&mut self, root: &Path) {
        self.wal_root = root.join("waldata");
        self.config_root = root.join("conf");
        self.data_root = root.join("ctdata");
        self.log_dir = root.join("log").to_string_lossy().into_owned();
        self.node_root = Some(root.to_path_buf());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crow_kv_config_default_round_trip() {
        let config = CrowKVConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        let restored: CrowKVConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(restored.max_inflight(), config.max_inflight());
        assert_eq!(restored.wal_early_ack, config.wal_early_ack);
        assert_eq!(restored.election.election_min_ms, config.election.election_min_ms);
    }

    #[test]
    fn crow_kv_config_partial_toml_uses_defaults() {
        let toml_str = "wal_early_ack = true";
        let config: CrowKVConfig = toml::from_str(toml_str).unwrap();
        assert!(config.wal_early_ack);
        assert_eq!(
            config.paxos.max_inflight_proposals,
            PaxosConfig::DEFAULT.max_inflight_proposals
        );
    }

    /// The tracked `app/crow-kv-server/conf/crow_kv_server_config.toml`
    /// must parse without error — guards against stale/mismatched
    /// template edits.
    #[test]
    fn tracked_kv_server_config_file_loads() {
        let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let config_path = manifest_dir
            .join("..")
            .join("..")
            .join("app")
            .join("crow-kv-server")
            .join("conf")
            .join("crow_kv_server_config.toml");
        if !config_path.exists() {
            // Running from a published crate (no workspace layout).
            return;
        }
        let config = CrowKVConfig::load_from_file(&config_path).expect("load tracked config");
        assert_eq!(config.server.shutdown_timeout_ms, 10_000);
        assert!(config.wal_early_ack);
        assert!(config.async_engine_apply);
    }
}
