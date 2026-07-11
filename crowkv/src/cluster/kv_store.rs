use crate::rpc::{KvBatchItem, KvResponse, KvScanResponse};

#[allow(async_fn_in_trait)]
pub trait KvStore {
    async fn kv_get(&self, group_id: u64, key: Vec<u8>, request_id: u64, request_create_ms: u64) -> KvResponse;

    #[allow(clippy::too_many_arguments)]
    async fn kv_put(&self, group_id: u64, key: Vec<u8>, value: Vec<u8>, client_id: u64, seq: u64, request_id: u64, request_create_ms: u64) -> KvResponse;

    async fn kv_delete(&self, group_id: u64, key: Vec<u8>, client_id: u64, seq: u64, request_id: u64, request_create_ms: u64) -> KvResponse;

    async fn kv_batch_write(&self, group_id: u64, items: Vec<KvBatchItem>, client_id: u64, seq: u64, request_id: u64, request_create_ms: u64) -> KvResponse;

    /// Prefix-scan the learner store, returning at most `limit` items
    /// (`limit == 0` means "no limit"). V1 is a local-replica read with
    /// the same staleness window as `kv_get`; the response sets
    /// `truncated = true` when `limit` was reached.
    async fn kv_scan(&self, group_id: u64, prefix: Vec<u8>, limit: u32, request_id: u64, request_create_ms: u64) -> KvScanResponse;
}
