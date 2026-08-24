// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Default port allocation for CROW services.
//!
//! Each service type has a **base port** — the start port for that
//! service type. When multiple instances of the same service type run
//! on one node, each instance picks `base + instance * stride`. Port
//! ranges are non-overlapping across service types so different
//! services never collide on the same node.
//!
//! ## Port ranges
//!
//! - `9910`–`9919` — crow-kv-server HTTP management API (stride 1)
//! - `9920`–`9929` — crow-web HTTP service (stride 1)
//! - `9931`–`9940` — crow-diskdb crow-rpc listener (R115 migration,
//!   stride 1; instance `i` uses `9931 + i`)
//! - `9941`–`9960` — crow-diskdb gRPC + HTTP management (paired,
//!   stride 2; instance `i` uses gRPC `9941 + 2i`, HTTP `9942 + 2i`)
//! - `9971`–`9990` — crow-chunkdb gRPC + HTTP management (paired,
//!   stride 2; instance `i` uses gRPC `9971 + 2i`, HTTP `9972 + 2i`)
//! - `28001`–`28200` — crow-kv-server gRPC `PxKvStore` listener pool
//!   (stride 1)
//! - `28101`–`28300` — crow-kv-server crow-rpc consensus listener (R32
//!   migration, stride 1; inter-KV-server only. R117 adds a separate
//!   client-facing port)
//! - `28201`–`28400` — crow-kv-server crow-rpc client-facing listener
//!   (R117 migration, stride 1; client-to-server only. Separate from
//!   the consensus port so the two surfaces evolve independently)
//!
//! Future service types (diskio, …) should pick a base
//! outside these ranges and document it here.

/// crow-kv-server HTTP management API — base port.
pub const KV_SERVER_MGMT_BASE: u16 = 9910;

/// crow-kv-server gRPC `PxKvStore` listener — base port (port pool).
pub const KV_SERVER_GRPC_BASE: u16 = 28001;

/// crow-kv-server crow-rpc consensus listener — base port (R32
/// migration). Separate from the gRPC port so both servers run
/// simultaneously during the mixed-rollout window. Inter-KV-server
/// only (replica-to-replica Paxos). R117 adds a separate client-facing
/// port. Stride 1 (one port per instance).
pub const KV_RPC_BASE: u16 = 28101;

/// crow-kv-server crow-rpc client-facing listener — base port (R117
/// migration). Separate from the consensus port so the two surfaces
/// evolve independently. Stride 1 (one port per instance). Derived
/// from the gRPC port via `KV_CLIENT_RPC_BASE - KV_SERVER_GRPC_BASE
/// = 200` (parallel to R32's `KV_RPC_BASE - KV_SERVER_GRPC_BASE =
/// 100`).
pub const KV_CLIENT_RPC_BASE: u16 = 28201;

/// crow-diskdb gRPC listener — base port.
pub const DISKDB_GRPC_BASE: u16 = 9941;

/// crow-diskdb HTTP management API — base port.
pub const DISKDB_HTTP_BASE: u16 = 9942;

/// crow-diskdb crow-rpc listener — base port (R115 migration). Separate
/// from the gRPC port so both servers run simultaneously during the
/// mixed-rollout window. Stride 1 (one port per instance).
pub const DISKDB_RPC_BASE: u16 = 9931;

/// crow-chunkdb gRPC listener — base port.
pub const CHUNKDB_GRPC_BASE: u16 = 9971;

/// crow-chunkdb HTTP management API — base port.
pub const CHUNKDB_HTTP_BASE: u16 = 9972;

/// crow-chunkdb crow-rpc listener — base port (R116 migration).
/// Separate from the gRPC port so both servers run simultaneously
/// during the mixed-rollout window. Stride 1 (one port per instance).
pub const CHUNKDB_RPC_BASE: u16 = 9961;

/// crow-web HTTP service — base port.
pub const WEB_BASE: u16 = 9920;

/// CROW service type for default port allocation.
///
/// Use [`ServicePort::port`] to compute the listen port for the
/// `instance`-th instance of a service type on a given node (0-based).
/// The base constants (e.g. [`KV_SERVER_MGMT_BASE`]) are re-exported
/// for contexts that need a plain `const` value (clap `default_value_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServicePort {
    /// crow-kv-server HTTP management API.
    KvServerMgmt,
    /// crow-kv-server gRPC — `PxKvStore` listener (port pool).
    KvServerGrpc,
    /// crow-kv-server crow-rpc consensus listener (R32 migration).
    KvServerRpc,
    /// crow-kv-server crow-rpc client-facing listener (R117 migration).
    KvServerClientRpc,
    /// crow-diskdb gRPC listener.
    DiskdbGrpc,
    /// crow-diskdb HTTP management API.
    DiskdbHttp,
    /// crow-chunkdb gRPC listener.
    ChunkdbGrpc,
    /// crow-chunkdb HTTP management API.
    ChunkdbHttp,
    /// crow-chunkdb crow-rpc listener (R116 migration).
    ChunkdbRpc,
    /// crow-web HTTP service.
    Web,
}

impl ServicePort {
    /// Base (start) port for this service type.
    #[must_use]
    pub const fn base(self) -> u16 {
        match self {
            Self::KvServerMgmt => KV_SERVER_MGMT_BASE,
            Self::KvServerGrpc => KV_SERVER_GRPC_BASE,
            Self::KvServerRpc => KV_RPC_BASE,
            Self::KvServerClientRpc => KV_CLIENT_RPC_BASE,
            Self::DiskdbGrpc => DISKDB_GRPC_BASE,
            Self::DiskdbHttp => DISKDB_HTTP_BASE,
            Self::ChunkdbGrpc => CHUNKDB_GRPC_BASE,
            Self::ChunkdbHttp => CHUNKDB_HTTP_BASE,
            Self::ChunkdbRpc => CHUNKDB_RPC_BASE,
            Self::Web => WEB_BASE,
        }
    }

    /// Port stride between consecutive instances of the same service
    /// type on one node.
    #[must_use]
    pub const fn stride(self) -> u16 {
        match self {
            Self::KvServerMgmt
            | Self::KvServerGrpc
            | Self::KvServerRpc
            | Self::KvServerClientRpc
            | Self::Web
            | Self::ChunkdbRpc => 1,
            // diskdb and chunkdb use paired ports (gRPC + HTTP); each
            // instance consumes two consecutive ports.
            Self::DiskdbGrpc | Self::DiskdbHttp | Self::ChunkdbGrpc | Self::ChunkdbHttp => 2,
        }
    }

    /// Port for the `instance`-th instance of this service type on a
    /// node (0-based).
    #[must_use]
    pub const fn port(self, instance: u16) -> u16 {
        self.base() + instance * self.stride()
    }
}
