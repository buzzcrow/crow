//! [`KvStore`] trait — the gRPC-facing surface of a single
//! `PxKvStore` instance. Methods mirror the wire protocol (`KvGet`,
//! `KvPut`, `KvDelete`, `KvBatchWrite`, `KvScan`) and return the
//! corresponding response message. The trait is implemented by
//! [`crate::cluster::px_kv_store::PxKvStore`]; it exists so the gRPC
//! handler layer in `crate::rpc` can depend on the trait rather than the
//! concrete store, easing mocking and future store implementations.

use crate::rpc::{KvBatchItem, KvResponse, KvScanResponse};

#[allow(async_fn_in_trait)]
pub trait KvStore {
    /// Point read. `read_mode` is the proto `ReadMode` discriminant
    /// (`0` = linearizable). `client_slot` is the read-your-writes target
    /// slot (ignored by other modes). See `px_kv_store` for per-mode routing.
    #[allow(clippy::too_many_arguments)]
    async fn kv_get(
        &self,
        group_id: u64,
        key: &[u8],
        read_mode: i32,
        client_slot: u64,
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
    /// leader read barrier, stale modes serve from local applied state. The
    /// response sets `truncated = true` when `limit` was reached.
    async fn kv_scan(
        &self,
        group_id: u64,
        prefix: &[u8],
        limit: u32,
        read_mode: i32,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvScanResponse;
}
