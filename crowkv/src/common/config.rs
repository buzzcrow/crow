//! Static configuration profiles for the Paxos retry budget, server
//! lifecycle, and per-group leader-election / heartbeat / lease tunables.
//!
//! All values here are compile-time `const`s exposed via `DEFAULT`
//! constants and `for_tests()` constructors; runtime overrides happen at
//! the call sites (`crowkv-server` CLI, testkit harness) before the
//! group is wrapped in an `Arc`.

use std::path::PathBuf;

use crate::wal::pipeline_backend::WalBlockAlignment;
use crate::wal::record::WalRecordFormat;

/// Paxos retry configuration (static, global).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaxosConfig {
    pub max_paxos_retries: usize,
    pub max_slot_retries: usize,
    pub retry_base_backoff_ms: u64,
    /// Maximum number of in-flight (allocated-but-not-yet-chosen) proposals
    /// the leader admits concurrently. A proposal that cannot acquire a window
    /// permit fails fast with `PxPaxosError::Busy` (retryable) rather than
    /// blocking, so the leader never stalls behind a saturated pipeline
    /// (`requirement.md` §7.3 / §12.1).
    pub proposer_window: usize,
}

impl PaxosConfig {
    pub const DEFAULT: Self = Self {
        max_paxos_retries: 3,
        max_slot_retries: 3,
        retry_base_backoff_ms: 5,
        proposer_window: 16,
    };
}

/// Server-level configuration (static, global).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// WAL configuration for a single consensus group.
#[derive(Clone, Debug)]
pub struct WalConfig {
    /// Directories to distribute WAL segments across.
    pub wal_disks: Vec<PathBuf>,
    /// Target segment size before rotation (bytes). Default 64 MiB.
    pub wal_segment_size: u64,
    /// Durable flush batch size trigger (bytes). Default 64 KiB.
    pub wal_flush_batch_bytes: usize,
    /// Optional durable flush coalescing budget (microseconds). Default 0.
    pub wal_flush_coalesce_us: u64,
    /// Watchdog timer for stuck durable flush batches. Default 100 ms.
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
            wal_disks: vec![PathBuf::from("wal")],
            wal_segment_size: 64 * 1024 * 1024,
            wal_flush_batch_bytes: 64 * 1024,
            wal_flush_coalesce_us: 0,
            wal_flush_watchdog_ms: 100,
            wal_disk_high_watermark_pct: 80,
            wal_min_retention_secs: 3600,
            gc_tick_secs: 30,
            wal_aligned: false,
            wal_io_unit_bytes: WalBlockAlignment::DEFAULT_IO_UNIT_BYTES,
            wal_record_format: WalRecordFormat::Auto,
        }
    }
}

/// Leader-election / heartbeat / lease tunables (per group).
///
/// All time values are milliseconds, stored as `u64`. The election driver
/// converts to `Duration` at the consumption site.
///
/// Defaults target a single-datacenter deployment with NTP-disciplined
/// clocks. See `doc/design/design-leader-election.md` §10 ("Tunables and
/// defaults") for the rationale and the cross-DC / WAN override profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    /// Bounded capacity of the per-peer `PxPeerStream` outbound mpsc.
    /// Full mpsc surfaces as `PxPaxosError::Busy` on the proposer side
    /// (already classified `FailRetryable`).
    pub peer_stream_window_frames: usize,
}

impl PxElectionConfig {
    /// Production / single-DC default. See `doc/design/design-leader-election.md` §10.
    pub const DEFAULT: Self = Self {
        prevote_enabled: true,
        heartbeat_interval_ms: 500,
        election_min_ms: 4000,
        election_max_ms: 8000,
        lease_duration_ms: 4500,
        max_clock_skew_ms: 500,
        bulk_prepare_window: 1024,
        election_driver_disabled: false,
        peer_stream_window_frames: 64,
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
            peer_stream_window_frames: 64,
        }
    }
}
