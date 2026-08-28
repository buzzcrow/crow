// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]

//! Hand-written Rust types replacing the prost-generated `crow_kv.rpc`
//! KV client types. API-compatible with the former proto-generated structs.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Implement `From<Enum> for i32` and `TryFrom<i32> for Enum`.
macro_rules! impl_enum_conversions {
    ($enum:ident, $($variant:ident = $value:expr),+ $(,)?) => {
        impl From<$enum> for i32 {
            fn from(v: $enum) -> Self {
                v as i32
            }
        }

        impl std::convert::TryFrom<i32> for $enum {
            type Error = ();

            fn try_from(v: i32) -> Result<Self, Self::Error> {
                Ok(match v {
                    $($value => $enum::$variant,)+
                    _ => return Err(()),
                })
            }
        }
    };
}

// ── Enums ───────────────────────────────────────────────────────

/// Structured error code for KV read/write responses. proto3
/// default 0 = NONE keeps the change wire-compatible: old servers that
/// don't set the field produce NONE, and the client falls back to the
/// `error` string for NONE. New servers set the code alongside the
/// existing `error` string so the client can switch on the code without
/// fragile string matching.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(i32)]
pub enum KvErrorCode {
    #[default]
    KvErrorNone = 0,
    KvErrorNotLeader = 1,
    KvErrorUnavailable = 2,
    KvErrorInternal = 3,
    /// JournalScan asked for slots already GC'd below the WAL trim point.
    /// The caller falls back to a full-scan rebuild (diskdb strategy 1).
    KvErrorJournalScanGcGap = 4,
}
impl_enum_conversions!(
    KvErrorCode,
    KvErrorNone = 0,
    KvErrorNotLeader = 1,
    KvErrorUnavailable = 2,
    KvErrorInternal = 3,
    KvErrorJournalScanGcGap = 4
);

/// Point-read consistency mode. Applies to both KvGetRequest and (with the
/// same numeric values) KvScanRequest.
///   0 LINEARIZABLE — reflect every committed write; served by the leader
///                    behind a lease fast-path or ReadIndex quorum check,
///                    followers transparently forward.
///   1 MIN_SLOT     — client carries `min_slot`; replica serves locally if
///                    its applied frontier >= min_slot, otherwise redirects
///                    to the leader. min_slot = 0 accepts any staleness;
///                    the write watermark gives read-your-writes; the last
///                    known safe_slot gives bounded-stale.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(i32)]
pub enum ReadMode {
    #[default]
    Linearizable = 0,
    MinSlot = 1,
}
impl_enum_conversions!(ReadMode, Linearizable = 0, MinSlot = 1);

// ── Request/response types ──────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvSetRequest {
    pub version: u32,
    pub key: Bytes,
    pub value: Bytes,
    /// client sequence for idempotency
    pub seq: u64,
    /// 0 = no expiration
    pub ttl_ms: u64,
    pub client_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    pub group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvGetRequest {
    pub version: u32,
    pub key: Bytes,
    pub request_id: u64,
    pub request_create_ms: u64,
    pub group_id: u64,
    pub read_mode: i32,
    /// For MIN_SLOT: the minimum applied slot the replica must have reached
    /// before serving locally. 0 = accept any staleness. Ignored by
    /// LINEARIZABLE.
    pub min_slot: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvDeleteRequest {
    pub version: u32,
    pub key: Bytes,
    pub seq: u64,
    pub client_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    pub group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvBatchItem {
    pub key: Bytes,
    pub value: Bytes,
    pub is_delete: bool,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvBatchWriteRequest {
    pub version: u32,
    pub items: Vec<KvBatchItem>,
    pub seq: u64,
    pub client_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    pub group_id: u64,
}

/// Unified mutation response.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvResponse {
    pub version: u32,
    pub ok: bool,
    /// logical version / LSN
    pub revision: u64,
    /// empty if ok
    pub error: String,
    /// true when delete targets a missing key
    pub not_found: bool,
    /// leader endpoint when this node is not the leader
    pub not_leader_hint: String,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// value for get operations
    pub value: Bytes,
    /// Slot the read was served at (the replica's applied frontier for reads).
    pub read_slot: u64,
    /// Group safe-slot known to the serving replica (min applied across voting
    /// members). Meaningful for BOUNDED_STALE reads.
    pub safe_slot: u64,
    /// Structured error code. Default 0 = NONE (old server).
    pub error_code: i32,
}

impl KvResponse {
    /// Wire-format version emitted by every response. Bump only when
    /// the schema gains a backward-incompatible field.
    pub const VERSION: u32 = 1;

    /// Successful proposal commit at `revision` (Paxos slot). Used by
    /// `kv_put` / `kv_delete` / `kv_batch_write`.
    #[must_use]
    pub fn ok_chosen(revision: u64, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: true,
            revision,
            error: String::new(),
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value: Bytes::new(),
            read_slot: 0,
            safe_slot: 0,
            error_code: KvErrorCode::KvErrorNone as i32,
        }
    }

    /// Attach the slot a read was served at and the serving replica's group
    /// safe-slot. Chainable on `ok_value` / `not_found`.
    #[must_use]
    pub fn with_read_slots(mut self, read_slot: u64, safe_slot: u64) -> Self {
        self.read_slot = read_slot;
        self.safe_slot = safe_slot;
        self
    }

    /// Successful read returning `value`. Used by `kv_get` hits.
    #[must_use]
    pub fn ok_value(value: Bytes, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: true,
            revision: 0,
            error: String::new(),
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value,
            read_slot: 0,
            safe_slot: 0,
            error_code: KvErrorCode::KvErrorNone as i32,
        }
    }

    /// Successful read returning `value` with the per-key write slot as
    /// `revision`. Used by `kv_get` hits when the engine reports the slot
    /// at which the key was last written.
    #[must_use]
    pub fn ok_value_with_revision(
        value: Bytes,
        revision: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> Self {
        Self {
            version: Self::VERSION,
            ok: true,
            revision,
            error: String::new(),
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value,
            read_slot: 0,
            safe_slot: 0,
            error_code: KvErrorCode::KvErrorNone as i32,
        }
    }

    /// Read miss — key absent in the local learner store.
    #[must_use]
    pub fn not_found(request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: false,
            revision: 0,
            error: String::new(),
            not_found: true,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value: Bytes::new(),
            read_slot: 0,
            safe_slot: 0,
            error_code: KvErrorCode::KvErrorNone as i32,
        }
    }

    /// Write rejected because the local replica is not the leader. The
    /// `hint` carries the known leader's endpoint when available.
    #[must_use]
    pub fn not_leader(hint: String, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: false,
            revision: 0,
            error: "not leader".to_string(),
            not_found: false,
            not_leader_hint: hint,
            request_id,
            request_create_ms,
            value: Bytes::new(),
            read_slot: 0,
            safe_slot: 0,
            error_code: KvErrorCode::KvErrorNotLeader as i32,
        }
    }

    /// Generic error path (proposal failure other than `NotLeader`).
    #[must_use]
    pub fn err(msg: String, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: false,
            revision: 0,
            error: msg,
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value: Bytes::new(),
            read_slot: 0,
            safe_slot: 0,
            error_code: KvErrorCode::KvErrorInternal as i32,
        }
    }
}

/// Prefix scan over the learner store. V1: local-replica read like
/// `KvGet`, no Paxos round-trip; results may include uncommitted-on-this-
/// replica writes lagging behind the leader. The handler iterates the
/// `DashMap` keys, filters by `prefix`, sorts, then truncates to
/// `limit`. `limit == 0` means "no limit".
#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvScanRequest {
    pub version: u32,
    pub prefix: Bytes,
    pub limit: u32,
    pub request_id: u64,
    pub request_create_ms: u64,
    pub group_id: u64,
    pub read_mode: i32,
    /// Exclusive lower bound for pagination. Empty = start from the
    /// beginning. Only keys strictly greater than `start_after` (and
    /// matching `prefix`) are returned.
    pub start_after: Bytes,
    /// For MIN_SLOT: same semantics as KvGetRequest.min_slot.
    pub min_slot: u64,
    /// Exclusive upper bound. Empty = unbounded. Only keys strictly less
    /// than `end_key` (and matching `prefix`) are returned.
    pub end_key: Bytes,
    /// Skip value materialization: items carry empty `value` fields. The engine
    /// skips overflow-chain assembly and value copy; the byte budget accounts for
    /// key bytes only, so a page fits more entries. Default false = today's behavior.
    pub keys_only: bool,
    /// Count matching live keys and ship zero items; the response `count` field
    /// carries the count. The store counts via a keys_only engine pass with no
    /// byte budget (one pass). Default false.
    pub count_only: bool,
    /// Absolute per-scan deadline in unix-ms (0 = no deadline, preserves today's
    /// behavior). The engine merge loop checks periodically and breaks early
    /// with truncated = true when exceeded; the client pagination loop checks
    /// before each page and stops with timed_out = true.
    pub deadline_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvScanItem {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvScanResponse {
    pub version: u32,
    pub ok: bool,
    pub error: String,
    /// If `truncated` is true the caller hit `limit` and should re-scan
    /// with a longer `prefix` or higher `limit` to see the rest.
    pub truncated: bool,
    pub items: Vec<KvScanItem>,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// Slot the scan was served at (the serving replica's applied frontier).
    pub read_slot: u64,
    /// Leader endpoint when this node is not the leader (MinSlot scan on a
    /// lagging follower). Empty for other failure shapes.
    pub not_leader_hint: String,
    /// Structured error code. Default 0 = NONE (old server).
    pub error_code: i32,
    /// Matched-key count for `count_only` scans (zero items shipped). 0 otherwise.
    pub count: u64,
    /// Deadline fired mid-scan: the result is partial (truncated = true, entries
    /// fetched before the deadline). Default false.
    pub timed_out: bool,
}

// ── Snapshot API (R59) ──────────────────────────────────────────

/// Snapshot versioning API (R59): pin a point-in-time-consistent L1 view
/// for backup/analytics. CreateSnapshot flushes L0 -> L1 and pins the
/// durable view at last_applied_slot; SnapshotScan iterates the frozen
/// vector with prefix/pagination; ReleaseSnapshot drops the pin.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct CreateSnapshotRequest {
    pub group_id: u64,
    pub read_mode: i32,
    pub min_slot: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CreateSnapshotResponse {
    pub ok: bool,
    pub error: String,
    pub snapshot_handle: u64,
    pub at_slot: u64,
    pub error_code: i32,
    pub not_leader_hint: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct ListSnapshotsRequest {
    pub group_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub snapshot_handle: u64,
    pub at_slot: u64,
    pub lease_remaining_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ListSnapshotsResponse {
    pub ok: bool,
    pub error: String,
    pub snapshots: Vec<SnapshotInfo>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct SnapshotScanRequest {
    pub snapshot_handle: u64,
    pub prefix: Bytes,
    pub start_after: Bytes,
    pub limit: u32,
    pub group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct SnapshotScanResponse {
    pub ok: bool,
    pub error: String,
    pub truncated: bool,
    pub items: Vec<KvScanItem>,
    pub error_code: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct ReleaseSnapshotRequest {
    pub snapshot_handle: u64,
    pub group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct ReleaseSnapshotResponse {
    pub ok: bool,
    pub error: String,
}

// ── Journal scan ────────────────────────────────────────────────

/// Slot-ordered scan over the chosen log. Returns individual KV ops
/// (Put / Delete) in commit (slot) order within [min_slot, max_slot],
/// filtered by key prefix. Used by diskdb strategy 2 (journal scan
/// replay) — the existing `Scan` RPC returns key order, not slot order,
/// so it cannot drive a correct replay. Pagination via `limit` +
/// `last_op_slot`: the caller sends `min_slot = last_op_slot + 1` for
/// the next page.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvJournalScanRequest {
    pub version: u32,
    pub group_id: u64,
    /// inclusive lower bound
    pub min_slot: u64,
    /// inclusive upper bound; 0 = MAX (current applied)
    pub max_slot: u64,
    /// only ops whose key starts with this prefix
    pub key_prefix: Bytes,
    /// max ops per response page; 0 = unlimited
    pub limit: u32,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// LINEARIZABLE or MIN_SLOT (min_slot doubles as the read-freshness
    /// floor for MIN_SLOT)
    pub read_mode: i32,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvJournalOp {
    pub key: Bytes,
    /// empty for Delete
    pub value: Bytes,
    pub is_delete: bool,
    /// the slot at which this op was committed
    pub slot: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct KvJournalScanResponse {
    pub version: u32,
    pub ok: bool,
    pub error: String,
    pub ops: Vec<KvJournalOp>,
    /// hit `limit`; more ops remain
    pub truncated: bool,
    /// slot of the last op returned (for pagination)
    pub last_op_slot: u64,
    /// the applied frontier when the scan ran
    pub read_slot: u64,
    pub error_code: i32,
    pub not_leader_hint: String,
    pub request_id: u64,
    pub request_create_ms: u64,
}

// ── Watch/Notify ────────────────────────────────────────────────
//
// Client-to-leader watch subscription. The client opens a WatchNotify
// bidi stream to a node, sends WatchSubscribe frames for each
// (group_id, prefix) it wants to watch, and receives WatchNotify
// frames when watched keys change. If the node is not the leader of
// the subscribed group_id, it returns a WatchNotifyError with
// not_leader_hint and the client reconnects to the leader.

#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchSubscribe {
    pub version: u32,
    pub group_id: u64,
    /// watch all keys with this byte prefix
    pub prefix: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchUnsubscribe {
    pub group_id: u64,
    pub prefix: Vec<u8>,
}

/// Pushed from leader to watcher when a watched key changes. Carries
/// the changed keys and their latest values so the watcher can act
/// without a re-read. `values[i]` is the value for `keys[i]`; empty
/// bytes for a Delete (tombstone). slot = the apply slot of the
/// triggering write.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchNotify {
    pub group_id: u64,
    /// which watched prefix matched
    pub prefix: Vec<u8>,
    /// changed keys (deduplicated, coalesced)
    pub keys: Vec<Vec<u8>>,
    pub slot: u64,
    /// latest value per key (empty = Delete)
    pub values: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchNotifyError {
    pub group_id: u64,
    /// empty for non-leader errors
    pub not_leader_hint: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchNotifyRequest {
    pub frame: Option<watch_notify_request::Frame>,
}

/// Nested oneof module for `WatchNotifyRequest.frame`.
pub mod watch_notify_request {
    #[derive(Clone, PartialEq, Debug)]
    pub enum Frame {
        Subscribe(super::WatchSubscribe),
        Unsubscribe(super::WatchUnsubscribe),
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct WatchNotifyResponse {
    pub frame: Option<watch_notify_response::Frame>,
}

/// Nested oneof module for `WatchNotifyResponse.frame`.
pub mod watch_notify_response {
    #[derive(Clone, PartialEq, Debug)]
    pub enum Frame {
        Notify(super::WatchNotify),
        Error(super::WatchNotifyError),
    }
}
