// Copyright 2026-present buzzcrow <buzzcrow@126.com>

//! chunkdb service layer — tonic gRPC + crow-rpc handler sets.

pub mod chunkdb_rpc_service;
pub mod chunkdb_service;

pub use chunkdb_rpc_service::ChunkdbRpcService;
pub use chunkdb_service::ChunkdbService;
