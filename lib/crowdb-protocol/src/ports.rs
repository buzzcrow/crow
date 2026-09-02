// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Default port allocation for CROWDB services.
//!
//! Each service type gets a **1000-port block** with a shared prefix
//! (same kind of server = same leading digits), all ports **>10000**.
//! Within a block, each listener kind gets a 100-port sub-range. All
//! services use **stride 1** — no paired-port logic. Port 0 is never
//! used (rejected everywhere by CLI parse and the port allocator).
//!
//! ## Port map
//!
//! - `10000`–`10999` — crowdb-kv-server (prefix 10)
//!   - `10000`–`10099` — HTTP management API (stride 1)
//!   - `10100`–`10199` — main `PxKvStore` listener (hosts both
//!     consensus and client crowdb-rpc handlers; stride 1)
//!   - `10200`–`10999` — spare
//! - `11000`–`11999` — crowdb-diskdb (prefix 11)
//!   - `11000`–`11099` — main listener (stride 1)
//!   - `11100`–`11199` — HTTP management API (stride 1; independent
//!     of listen — no paired-port invariant)
//!   - `11200`–`11299` — crowdb-rpc listener (stride 1)
//!   - `11300`–`11999` — spare
//! - `12000`–`12999` — crowdb-chunkdb (prefix 12)
//!   - `12100`–`12199` — HTTP management API (stride 1)
//!   - `12200`–`12299` — crowdb-rpc listener (stride 1)
//!   - `12300`–`12999` — spare
//! - `13000`–`13999` — crowdb-diskio (prefix 13)
//!   - `13000`–`13099` — crowdb-rpc listener (stride 1)
//!   - `13100`–`13999` — spare
//! - `14000`–`14999` — crowdb-web (prefix 14)
//!   - `14000`–`14099` — HTTP service (stride 1)
//!   - `14100`–`14999` — spare
//!
//! The group-0 kv-server mgmt port (`10000`) is the famous bootstrap
//! discovery port — any client can contact group-0 to read the service
//! registry and learn all living services' IP + port.
//!
//! Future service types should pick a base outside these ranges (next
//! free prefix: 15xxx) and document it here.

/// crowdb-kv-server HTTP management API — base port. Also the famous
/// group-0 bootstrap discovery port.
pub const KV_SERVER_MGMT_BASE: u16 = 10000;

/// crowdb-kv-server main `PxKvStore` listener — base port (port pool).
/// Hosts both consensus and client crowdb-rpc handlers on the same
/// listener (RPC port collapse — no separate consensus/client ports).
pub const KV_SERVER_LISTEN_BASE: u16 = 10100;

/// crowdb-diskdb main listener — base port.
pub const DISKDB_LISTEN_BASE: u16 = 11000;

/// crowdb-diskdb HTTP management API — base port. Independent of
/// `DISKDB_LISTEN_BASE` (no paired-port invariant).
pub const DISKDB_HTTP_BASE: u16 = 11100;

/// crowdb-diskdb crowdb-rpc listener — base port.
pub const DISKDB_RPC_BASE: u16 = 11200;

/// crowdb-chunkdb main listener — base port. Vestigial: the chunkdb
/// server binds `rpc_listen_addr` and `http_listen_addr` only; this
/// range is reserved for future use.
pub const CHUNKDB_LISTEN_BASE: u16 = 12000;

/// crowdb-chunkdb HTTP management API — base port.
pub const CHUNKDB_HTTP_BASE: u16 = 12100;

/// crowdb-chunkdb crowdb-rpc listener — base port.
pub const CHUNKDB_RPC_BASE: u16 = 12200;

/// crowdb-diskio crowdb-rpc listener — base port.
pub const DISKIO_RPC_BASE: u16 = 13000;

/// crowdb-web HTTP service — base port.
pub const WEB_BASE: u16 = 14000;

/// CROWDB service type for default port allocation.
///
/// Use [`ServicePort::port`] to compute the listen port for the
/// `instance`-th instance of a service type on a given node (0-based).
/// The base constants (e.g. [`KV_SERVER_MGMT_BASE`]) are re-exported
/// for contexts that need a plain `const` value (clap `default_value_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServicePort {
    /// crowdb-kv-server HTTP management API.
    KvServerMgmt,
    /// crowdb-kv-server main `PxKvStore` listener (port pool; hosts
    /// both consensus and client crowdb-rpc).
    KvServerListen,
    /// crowdb-diskdb main listener.
    DiskdbListen,
    /// crowdb-diskdb HTTP management API.
    DiskdbHttp,
    /// crowdb-diskdb crowdb-rpc listener.
    DiskdbRpc,
    /// crowdb-chunkdb main listener (vestigial — range reserved).
    ChunkdbListen,
    /// crowdb-chunkdb HTTP management API.
    ChunkdbHttp,
    /// crowdb-chunkdb crowdb-rpc listener.
    ChunkdbRpc,
    /// crowdb-diskio crowdb-rpc listener.
    DiskioRpc,
    /// crowdb-web HTTP service.
    Web,
}

impl ServicePort {
    /// Base (start) port for this service type.
    #[must_use]
    pub const fn base(self) -> u16 {
        match self {
            Self::KvServerMgmt => KV_SERVER_MGMT_BASE,
            Self::KvServerListen => KV_SERVER_LISTEN_BASE,
            Self::DiskdbListen => DISKDB_LISTEN_BASE,
            Self::DiskdbHttp => DISKDB_HTTP_BASE,
            Self::DiskdbRpc => DISKDB_RPC_BASE,
            Self::ChunkdbListen => CHUNKDB_LISTEN_BASE,
            Self::ChunkdbHttp => CHUNKDB_HTTP_BASE,
            Self::ChunkdbRpc => CHUNKDB_RPC_BASE,
            Self::DiskioRpc => DISKIO_RPC_BASE,
            Self::Web => WEB_BASE,
        }
    }

    /// Port stride between consecutive instances of the same service
    /// type on one node. All services use stride 1.
    #[must_use]
    pub const fn stride(self) -> u16 {
        let _ = self;
        1
    }

    /// Port for the `instance`-th instance of this service type on a
    /// node (0-based).
    #[must_use]
    pub const fn port(self, instance: u16) -> u16 {
        self.base() + instance * self.stride()
    }

    /// Sub-range size (number of ports) for this service type's
    /// listener kind. Each listener kind gets 100 ports.
    #[must_use]
    pub const fn range_size(self) -> u16 {
        let _ = self;
        100
    }
}
