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
//! - `9941`–`9960` — crow-diskdb gRPC + HTTP management (paired,
//!   stride 2; instance `i` uses gRPC `9941 + 2i`, HTTP `9942 + 2i`)
//! - `28001`–`28200` — crow-kv-server gRPC `PxKvStore` listener pool
//!   (stride 1)
//!
//! Future service types (chunkdb, diskio, …) should pick a base
//! outside these ranges and document it here.

/// crow-kv-server HTTP management API — base port.
pub const KV_SERVER_MGMT_BASE: u16 = 9910;

/// crow-kv-server gRPC `PxKvStore` listener — base port (port pool).
pub const KV_SERVER_GRPC_BASE: u16 = 28001;

/// crow-diskdb gRPC listener — base port.
pub const DISKDB_GRPC_BASE: u16 = 9941;

/// crow-diskdb HTTP management API — base port.
pub const DISKDB_HTTP_BASE: u16 = 9942;

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
    /// crow-diskdb gRPC listener.
    DiskdbGrpc,
    /// crow-diskdb HTTP management API.
    DiskdbHttp,
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
            Self::DiskdbGrpc => DISKDB_GRPC_BASE,
            Self::DiskdbHttp => DISKDB_HTTP_BASE,
            Self::Web => WEB_BASE,
        }
    }

    /// Port stride between consecutive instances of the same service
    /// type on one node.
    #[must_use]
    pub const fn stride(self) -> u16 {
        match self {
            Self::KvServerMgmt | Self::KvServerGrpc | Self::Web => 1,
            // diskdb uses paired ports (gRPC + HTTP); each instance
            // consumes two consecutive ports.
            Self::DiskdbGrpc | Self::DiskdbHttp => 2,
        }
    }

    /// Port for the `instance`-th instance of this service type on a
    /// node (0-based).
    #[must_use]
    pub const fn port(self, instance: u16) -> u16 {
        self.base() + instance * self.stride()
    }
}
