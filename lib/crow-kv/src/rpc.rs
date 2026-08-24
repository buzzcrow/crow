// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `CrowKV` RPC services and client library.
//!
//! Consensus RPC service (`Prepare`/`Promise`/`Accept`/`Accepted`) and
//! the full service set (`PxService`, `SnapshotService`, client library
//! with topology cache, retry, `NotLeaderHint` handling).
//!
//! The wire types (message structs + enums) are defined in
//! `crow-protocol` (`kv_consensus::rpc` for Paxos messages,
//! `kv_client::rpc` for KV client messages) and re-exported here so
//! existing `use crate::rpc::*` imports continue to work. The transport
//! is crow-rpc (flatbuffer over TCP); the former gRPC server and
//! client modules have been removed.

pub use crow_protocol::kv_client::rpc::*;
pub use crow_protocol::kv_consensus::rpc::*;

pub(crate) mod kv_rpc_service;
pub(crate) mod px_rpc_service;
pub(crate) mod px_rpc_transport;
#[allow(unused_imports)]
pub(crate) use kv_rpc_service::{KvClientRpcForwarder, KvRpcService};
#[allow(unused_imports)]
pub(crate) use px_rpc_service::PxRpcService;
pub use px_rpc_transport::PxRpcTransport;

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
