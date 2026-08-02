// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Static configuration profiles for the Paxos retry budget, server
//! lifecycle, and per-group leader-election / heartbeat / lease tunables.
//!
//! All values here are compile-time `const`s exposed via `DEFAULT`
//! constants and `for_tests` constructors; runtime overrides happen at
//! the call sites (`crowkv-server` CLI, testkit harness) before the
//! group is wrapped in an `Arc`.

use std::path::PathBuf;

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
    pub fn parse(s: &str) -> Option<Self> {
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

/// Paxos retry configuration (static, global).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct PaxosConfig {
    pub max_paxos_retries: usize,
    pub max_slot_retries: usize,
    pub retry_base_backoff_ms: u64,
    /// Maximum number of in-flight (allocated-but-not-yet-chosen) proposals
    /// the leader admits concurrently. A proposal that cannot acquire a window
    /// permit fails fast with `PxPaxosError::Busy` (retryable) rather than
    /// blocking, so the leader never stalls behind a saturated pipeline
    /// (parallel-slot window / performance targets).
    pub max_inflight_proposals: usize,
    /// Number of admission queues per group. Each queue gets
    /// `ceil(max_inflight_proposals / inflight_queues)` permits. Default 1.
    pub inflight_queues: usize,
    /// Admission policy when all permits are occupied: `Reject` (fail fast
    /// with `Busy`) or `Queue` (block until a permit is freed). Default
    /// `Reject`.
    pub inflight_admission: AdmissionPolicy,
    /// R45 max ops per coalesced batch. Capped at 255 (the payload op-count
    /// byte). `0` disables coalescing (one proposal per key). Default 0
    /// (opt-in).
    pub coalesce_max_keys: usize,
    /// R45 timer-mode window in microseconds. When the coalescer switches
    /// to timer mode (high load), batches flush after this interval or
    /// when `coalesce_max_keys` is reached, whichever comes first.
    /// `0` = event-only mode (never switch to timer mode; watchdog still
    /// active at fixed 1000ms). Default 0.
    pub coalesce_window_us: u64,
}

impl PaxosConfig {
    pub const DEFAULT: Self = Self {
        max_paxos_retries: 3,
        max_slot_retries: 3,
        retry_base_backoff_ms: 5,
        max_inflight_proposals: 32,
        inflight_queues: 1,
        inflight_admission: AdmissionPolicy::Queue,
        coalesce_max_keys: 0,
        coalesce_window_us: 0,
    };
}

impl Default for PaxosConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Server-level configuration (static, global).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ServerConfig {
    /// Per-layer graceful-shutdown timeout in milliseconds.
    /// Shutdowns that take longer almost always indicate a stuck task and are
    /// better force-cleaned than waited on.
    pub shutdown_timeout_ms: u64,
}

impl ServerConfig {
    pub const DEFAULT: Self = Self {
        shutdown_timeout_ms: 10_000,
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
    /// Directories to distribute WAL segments across.
    pub wal_disks: Vec<PathBuf>,
    /// Target segment size before rotation (bytes). Default 64 MiB.
    pub wal_segment_size: u64,
    /// Durable flush batch size trigger (bytes). Default 64 KiB.
    pub wal_flush_batch_bytes: usize,
    /// Safety-net timer that wakes the idle writer every `watchdog` ms to
    /// drain any queued records in case of a missed wake ("just in case for
    /// bugs"). Default 100 ms.
    pub wal_flush_watchdog_ms: u64,
    /// Disk-pressure watermark for eager GC. Default 80%.
    pub wal_disk_high_watermark_pct: u8,
    /// Forensics retention grace period (seconds). Default 3600 (1 hour).
    pub wal_min_retention_secs: u64,
    /// GC scan cadence (seconds). Default 30.
    pub gc_tick_secs: u64,
    /// Whether the WAL backend uses block-aligned I/O (SSD/NVMe under
    /// `O_DIRECT`). When `false` (default), the file backend is used with
    /// byte-addressable media (RAM/SCM/PMEM). When `true`, a block pipeline
    /// is selected and `wal_io_unit_bytes` controls the alignment unit.
    pub wal_aligned: bool,
    /// SSD/NVMe I/O unit size in bytes. Only used when `wal_aligned` is `true`.
    /// Common values: 512 (logical sector), 4096 (default 4 KiB), 8192,
    /// 16384, 65536. Must be a power of two.
    pub wal_io_unit_bytes: usize,
    /// Record encoding format. `Auto` selects binary frames (zero-copy) on all
    /// backends.
    pub wal_record_format: WalRecordFormat,
    /// Skip the durable `fdatasync` on every write batch. Records are still
    /// written to the segment file, but the flush is not durable. Unsafe for
    /// production — only for benchmark path-overhead isolation (R10). Default
    /// `false`.
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
    pub fn alignment(&self) -> WalBlockAlignment {
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

/// Leader-election / heartbeat / lease tunables (per group).
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
}

impl PxElectionConfig {
    /// Multiplier applied to `PaxosConfig::max_inflight_proposals` to derive
    /// `learner_stream_window_frames`. Gives the learner channel 4×
    /// headroom over the proposer admission gate.
    pub const LEARNER_WINDOW_MULTIPLIER: usize = 4;

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
        election_driver_disabled: false,
        learner_stream_window_frames: PaxosConfig::DEFAULT.max_inflight_proposals
            * Self::LEARNER_WINDOW_MULTIPLIER,
        // Maintenance loop tick: flush L0→L1 + GC watermark check every
        // tick; durable snapshot gated by thresholds above.
        maintenance_tick_ms: 10_000,
        snapshot_slot_threshold: 100_000,
        snapshot_time_threshold_ms: 600_000,
    };

    /// Aggressive timings for `#[tokio::test(start_paused = true)]` suites.
    ///
    /// Heartbeat 5 ms / election 30–60 ms / lease 25 ms. Not exposed on the
    /// `crowkv-server` CLI.
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
            election_driver_disabled: false,
            learner_stream_window_frames: PaxosConfig::DEFAULT.max_inflight_proposals
                * Self::LEARNER_WINDOW_MULTIPLIER,
            maintenance_tick_ms: 500,
            snapshot_slot_threshold: 1000,
            snapshot_time_threshold_ms: 1_000,
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
            election_driver_disabled: false,
            learner_stream_window_frames: PaxosConfig::DEFAULT.max_inflight_proposals
                * Self::LEARNER_WINDOW_MULTIPLIER,
            maintenance_tick_ms: 3_000,
            snapshot_slot_threshold: 1_000_000,
            snapshot_time_threshold_ms: 9_000,
        }
    }

    /// Derive the learner stream window for a given max-inflight-proposals
    /// count. Call this when customizing `max_inflight_proposals` at runtime
    /// so the learner channel stays in sync.
    #[must_use]
    pub const fn learner_window_for(max_inflight_proposals: usize) -> usize {
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
/// one struct with `serde` derives for JSON file loading. Runtime
/// paths and backends are `#[serde(skip)]` — set from CLI args after
/// loading.
///
/// Usage: `CrowKVConfig::load_from_file(path)` or
/// `CrowKVConfig::default()`, then CLI args override individual fields,
/// then pass `&CrowKVConfig` to `create_group_with_wal`.
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
    #[serde(default)]
    pub force_classic: bool,
    #[serde(default)]
    pub wal_early_ack: bool,
    #[serde(default)]
    pub async_engine_apply: bool,
    /// WAL root directory. `#[serde(skip)]` — set from `--wal-root` CLI.
    #[serde(skip)]
    pub wal_root: PathBuf,
    /// Group config root directory. `#[serde(skip)]` — set from CLI.
    #[serde(skip)]
    pub config_root: PathBuf,
    /// Crowtree data root directory. `#[serde(skip)]` — set from CLI.
    #[serde(skip)]
    pub data_root: PathBuf,
    /// WAL I/O backend label. `#[serde(skip)]` — set from `--wal-backend`.
    #[serde(skip)]
    pub wal_backend: String,
    /// Crowtree storage backend label. `#[serde(skip)]` — set from
    /// `--kv-backend`.
    #[serde(skip)]
    pub crowtree_backend: String,
    /// Skip durable fdatasync. `#[serde(skip)]` — set from `--no-fsync`.
    #[serde(skip)]
    pub wal_skip_fsync: bool,
    /// Log directory. `#[serde(skip)]` — set from CLI.
    #[serde(skip)]
    pub log_dir: String,
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
        }
    }
}

impl CrowKVConfig {
    /// Load from a JSON file. Missing fields use sub-struct defaults.
    ///
    /// # Errors
    /// Returns the `serde_json::Error` on parse failure.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, serde_json::Error> {
        let file = std::fs::File::open(path).map_err(serde_json::Error::io)?;
        let mut config: Self = serde_json::from_reader(file)?;
        // Fill runtime defaults if the file didn't set them (they're
        // serde-skip so they're always default-initialized by
        // deserialization).
        if config.wal_root.as_os_str().is_empty() {
            config.wal_root = PathBuf::from("waldata");
        }
        if config.config_root.as_os_str().is_empty() {
            config.config_root = PathBuf::from("conf");
        }
        if config.data_root.as_os_str().is_empty() {
            config.data_root = PathBuf::from("ctdata");
        }
        if config.wal_backend.is_empty() {
            config.wal_backend = "file".to_string();
        }
        if config.crowtree_backend.is_empty() {
            config.crowtree_backend = "file".to_string();
        }
        if config.log_dir.is_empty() {
            config.log_dir = "log".to_string();
        }
        Ok(config)
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
    pub fn for_e2e() -> Self {
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

    /// Number of admission queues (convenience accessor for
    /// `paxos.inflight_queues`).
    #[must_use]
    pub fn inflight_queues(&self) -> usize {
        self.paxos.inflight_queues
    }

    /// Admission policy (convenience accessor for
    /// `paxos.inflight_admission`).
    #[must_use]
    pub fn inflight_admission(&self) -> AdmissionPolicy {
        self.paxos.inflight_admission
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crowkv_config_default_round_trip() {
        let config = CrowKVConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let restored: CrowKVConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.max_inflight(), config.max_inflight());
        assert_eq!(restored.wal_early_ack, config.wal_early_ack);
        assert_eq!(restored.election.election_min_ms, config.election.election_min_ms);
    }

    #[test]
    fn crowkv_config_partial_json_uses_defaults() {
        let json = r#"{"wal_early_ack": true}"#;
        let config: CrowKVConfig = serde_json::from_str(json).unwrap();
        assert!(config.wal_early_ack);
        assert_eq!(
            config.paxos.max_inflight_proposals,
            PaxosConfig::DEFAULT.max_inflight_proposals
        );
    }
}
