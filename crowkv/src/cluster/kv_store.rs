use crate::rpc::{KvBatchItem, KvResponse};

pub trait KvStore {
    async fn kv_get(
        &self,
        group_id: u64,
        key: Vec<u8>,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse;

    async fn kv_put(
        &self,
        group_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse;

    async fn kv_delete(
        &self,
        group_id: u64,
        key: Vec<u8>,
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
}
