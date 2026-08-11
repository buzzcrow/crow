// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common type aliases that complement the proto types in
//! `common_type.proto`.
//!
//! All identifier types are defined here (once) and used consistently
//! across all crates (console config, kv-client, kv-server, diskdb,
//! chunkdb). The simple integer IDs are type aliases (`pub type X =
//! u64;`) for documentation and API clarity, not newtypes. The
//! composite IDs (`DiskId`, `ChunkId`) are proto structs in
//! `common_type.proto`.

/// Rack identifier (integer, assigned by the cluster).
pub type RackId = u64;

/// Node identifier (integer, assigned by the cluster).
pub type NodeId = u64;

/// Disk-group identifier (integer, globally unique).
pub type DiskGroupId = u64;

/// Store identifier (integer, assigned by the cluster).
pub type StoreId = u64;

/// Group identifier (integer, assigned within a store).
pub type GroupId = u64;

/// Replica identifier (integer, assigned within a group).
pub type ReplicaId = u64;

/// Service instance identifier (diskdb instance, kv-server instance).
pub type InstanceId = u64;
