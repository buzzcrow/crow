// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowKV` gRPC services and client library.
//!
//! Minimal consensus RPC service (`Prepare`/`Promise`/`Accept`/`Accepted`)
//! lands in P1 M2 with wire protocol and service definitions.
//!
//! Full service set (`PxService`, `VoteService`, `SnapshotService`,
//! client library with topology cache, retry, `NotLeaderHint` handling)
//! lands in P4.
//! Key work: consensus RPC, service definitions, client library, topology cache.

// Generated from src/rpc/proto/pxos.proto and src/rpc/proto/kv.proto by
// tonic-build in build.rs. Wire types, PxService and KvService client/server
// traits.
#[allow(
    clippy::wildcard_imports,
    clippy::let_unit_value,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::items_after_statements,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    missing_docs
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/crow_kv.rpc.rs"));
}

pub use generated::*;

mod kv_response;
pub(crate) mod kv_rpc_service;
pub(crate) mod kv_service;
pub(crate) mod px_rpc_service;
pub(crate) mod px_rpc_transport;
pub(crate) mod px_service;
pub(crate) mod snapshot_service;
#[allow(unused_imports)]
pub(crate) use kv_rpc_service::{KvClientRpcForwarder, KvRpcService};
#[allow(unused_imports)]
pub(crate) use kv_service::KvStoreService;
#[allow(unused_imports)]
pub(crate) use px_rpc_service::PxRpcService;
#[allow(unused_imports)]
pub use px_rpc_transport::PxRpcTransport;
pub(crate) use px_service::PxReplicaService;
pub(crate) use snapshot_service::PxSnapshotService;

// Re-export the flatbuffer KV client request/response types so the
// server handler (`kv_rpc_service.rs`) can reference them via
// `crate::rpc::FB<Type>` without a direct `crow_protocol` import at
// every call site. These are the types from `kv_client_fb` (R117).
#[allow(unused_imports)]
pub(crate) use crow_protocol::kv_client_fb::{
    FBBytes, FBBytesArgs, FBCreateSnapshotRequest, FBCreateSnapshotRequestArgs, FBCreateSnapshotResponse,
    FBCreateSnapshotResponseArgs, FBKvBatchItem, FBKvBatchItemArgs, FBKvBatchWriteRequest,
    FBKvBatchWriteRequestArgs, FBKvClientRetCode, FBKvDeleteRequest, FBKvDeleteRequestArgs, FBKvGetRequest,
    FBKvGetRequestArgs, FBKvJournalOp, FBKvJournalOpArgs, FBKvJournalScanRequest, FBKvJournalScanRequestArgs,
    FBKvJournalScanResponse, FBKvJournalScanResponseArgs, FBKvResponse, FBKvResponseArgs, FBKvScanItem,
    FBKvScanItemArgs, FBKvScanRequest, FBKvScanRequestArgs, FBKvScanResponse, FBKvScanResponseArgs,
    FBKvSetRequest, FBKvSetRequestArgs, FBListSnapshotsRequest, FBListSnapshotsRequestArgs,
    FBListSnapshotsResponse, FBListSnapshotsResponseArgs, FBReadMode, FBReleaseSnapshotRequest,
    FBReleaseSnapshotRequestArgs, FBReleaseSnapshotResponse, FBReleaseSnapshotResponseArgs, FBSnapshotInfo,
    FBSnapshotInfoArgs, FBSnapshotScanRequest, FBSnapshotScanRequestArgs, FBSnapshotScanResponse,
    FBSnapshotScanResponseArgs, FBWatchNotify, FBWatchNotifyArgs, FBWatchNotifyError, FBWatchNotifyErrorArgs,
    FBWatchSubscribe, FBWatchSubscribeArgs, FBWatchUnsubscribe, FBWatchUnsubscribeArgs,
};
