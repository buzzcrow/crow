// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! [`KvStore`] trait — the crowdb-rpc-facing surface of a single
//! `PxKvStore` instance. Methods mirror the wire protocol (`KvGet`,
//! `KvPut`, `KvDelete`, `KvBatchWrite`, `KvScan`) and return the
//! corresponding response message. The trait is implemented by
//! [`crate::cluster::px_kv_store::PxKvStore`]; it exists so the crowdb-rpc
//! handler layer in `crate::rpc` can depend on the trait rather than the
//! concrete store, easing mocking and future store implementations.

use crate::rpc::{
    CreateSnapshotResponse, KvBatchItem, KvJournalScanResponse, KvResponse, KvScanResponse,
    ListSnapshotsResponse, ReleaseSnapshotResponse, SnapshotScanResponse,
};

#[allow(async_fn_in_trait)]
pub trait KvStore {
    /// Point read. `read_mode` is the proto `ReadMode` discriminant
    /// (`0` = linearizable, `1` = `min_slot`). `min_slot` is the minimum
    /// applied slot for `MinSlot` mode (ignored by `Linearizable`).
    /// See `px_kv_store` for per-mode routing.
    #[allow(clippy::too_many_arguments)]
    async fn kv_get(
        &self,
        group_id: u64,
        key: &[u8],
        read_mode: i32,
        min_slot: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse;

    #[allow(clippy::too_many_arguments)]
    async fn kv_put(
        &self,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse;

    async fn kv_delete(
        &self,
        group_id: u64,
        key: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse;

    async fn kv_batch_write(
        &self,
        group_id: u64,
        items: Vec<KvBatchItem>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse;

    /// Prefix-scan the learner store, returning at most `limit` items
    /// (`limit == 0` means "no limit"). `read_mode` selects the consistency
    /// discipline as in [`Self::kv_get`]: linearizable scans run behind the
    /// leader read barrier, `min_slot` scans serve from local applied state
    /// when the frontier has caught up. The response sets `truncated = true`
    /// when `limit` was reached. `keys_only` returns items with empty values
    /// (no value materialization). `count_only` ships zero items and sets the
    /// response `count` to the number of matching live keys (counted via a
    /// single `keys_only` pass with no byte budget).
    #[allow(clippy::too_many_arguments)]
    async fn kv_scan(
        &self,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: i32,
        min_slot: u64,
        keys_only: bool,
        count_only: bool,
        deadline_ms: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvScanResponse;

    /// Pin a point-in-time-consistent L1 view: flush L0 → L1, then
    /// `snapshot_view()`. Returns a server-side snapshot handle (opaque
    /// u64 id) and the `at_slot` the snapshot covers. The handle is held
    /// by a per-group registry with a lease/expiry to reap abandoned
    /// snapshots.
    async fn kv_create_snapshot(
        &self,
        group_id: u64,
        read_mode: i32,
        min_slot: u64,
    ) -> CreateSnapshotResponse;

    /// List active snapshot handles for a group with their `at_slot` and
    /// remaining lease.
    async fn kv_list_snapshots(&self, group_id: u64) -> ListSnapshotsResponse;

    /// Iterate a pinned snapshot with `prefix`, `start_after`, `limit`.
    /// Same pagination contract as `kv_scan` (`truncated` + `start_after`),
    /// but against the frozen vector instead of live data.
    async fn kv_snapshot_scan(
        &self,
        group_id: u64,
        snapshot_handle: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> SnapshotScanResponse;

    /// Drop a snapshot handle, releasing the pinned view. The next GC
    /// sweep can reclaim the pages.
    async fn kv_release_snapshot(&self, group_id: u64, snapshot_handle: u64) -> ReleaseSnapshotResponse;

    /// Slot-ordered scan over the chosen log: returns individual KV ops
    /// (Put / Delete) in commit (slot) order within `[min_slot,
    /// max_slot]`, filtered by `key_prefix`. Used by diskdb strategy 2
    /// (journal scan replay) — `kv_scan` returns key order, not slot
    /// order, so it cannot drive a correct replay. `limit` caps the
    /// ops per response page; the response carries `truncated` +
    /// `last_op_slot` for stateless pagination (caller sends
    /// `min_slot = last_op_slot + 1` for the next page). `max_slot =
    /// 0` means "up to the current applied frontier". Read-mode
    /// routing mirrors `kv_scan`: linearizable runs the leader barrier;
    /// `min_slot` serves locally once the frontier has caught up.
    #[allow(clippy::too_many_arguments)]
    async fn kv_journal_scan(
        &self,
        group_id: u64,
        min_slot: u64,
        max_slot: u64,
        key_prefix: &[u8],
        limit: u32,
        read_mode: i32,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvJournalScanResponse;
}
