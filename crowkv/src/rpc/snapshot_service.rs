//! Tonic `SnapshotService` implementation (plan-tree #20 new-member
//! snapshot install, `design-crowtree-snapshot-gc.md` §6).
//!
//! Serves this replica's local `KVEngine::snapshot_export` as a chunked
//! stream: a single leading `SnapshotHeader` frame (carrying the term of
//! the log entry chosen at the snapshot's `at_slot`, needed by the caller
//! to seed a fresh replica's learner frontier -- `at_slot` itself is
//! embedded in the exported byte stream), followed by `data` chunks.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::snapshot_service_server::SnapshotService;
use crate::rpc::{snapshot_stream_item, SnapshotHeader, SnapshotRequest, SnapshotStreamItem};

/// Chunk size for streamed `data` frames. Matches crowtree's own portable
/// snapshot export default chunk size (`kSnapshotChunkBytes`,
/// `crowtree/include/crowtree/snapshot_io.h`) so re-chunking here rarely
/// splits an already-chunk-aligned stream any further.
const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct PxSnapshotService {
    store: Arc<PxKvStore>,
}

impl PxSnapshotService {
    pub fn new(store: Arc<PxKvStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl SnapshotService for PxSnapshotService {
    type StreamSnapshotStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotStreamItem, Status>> + Send + 'static>>;

    async fn stream_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        let req = request.into_inner();
        let group = self
            .store
            .get_group(req.group_id)
            .ok_or_else(|| Status::not_found("px group not found"))?;
        let replica = group.local_replica();

        let (at_slot, bytes) = replica
            .learner
            .engine()
            .snapshot_export()
            .map_err(|e| Status::failed_precondition(format!("snapshot export failed: {e}")))?;

        let term_at_slot = replica.accepted_at(at_slot).await.map_or_else(
            || {
                debug!(
                    store_id = self.store.store_id,
                    group_id = req.group_id,
                    at_slot,
                    "snapshot export: no local accepted entry at at_slot; defaulting term_at_slot to 0"
                );
                0
            },
            |entry| entry.term,
        );

        debug!(
            store_id = self.store.store_id,
            group_id = req.group_id,
            at_slot,
            term_at_slot,
            stream_bytes = bytes.len(),
            "serving snapshot export"
        );

        let mut items = vec![SnapshotStreamItem {
            payload: Some(snapshot_stream_item::Payload::Header(SnapshotHeader {
                term_at_slot,
                membership_epoch: group.membership_epoch(),
            })),
        }];
        items.extend(
            bytes
                .chunks(SNAPSHOT_STREAM_CHUNK_BYTES)
                .map(|chunk| SnapshotStreamItem {
                    payload: Some(snapshot_stream_item::Payload::Data(chunk.to_vec())),
                }),
        );

        let out_stream = tokio_stream::iter(items.into_iter().map(Result::<_, Status>::Ok));
        Ok(Response::new(Box::pin(out_stream) as Self::StreamSnapshotStream))
    }
}
