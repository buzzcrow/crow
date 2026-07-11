//! `CrowKV` gRPC services and client library.
//!
//! Minimal consensus RPC service (`Prepare`/`Promise`/`Accept`/`Accepted`)
//! lands in P1 M2 per `doc/plan/plan-consensus.md` §1 M2 and
//! `doc/design/design-rpc.md` §2.
//!
//! Full service set (`PxService`, `VoteService`, `SnapshotService`,
//! client library with topology cache, retry, `NotLeaderHint` handling)
//! lands in P4 per `doc/plan/plan-rpc.md`.

// Generated from src/rpc/proto/pxos.proto and src/rpc/proto/kv.proto by
// tonic-build in build.rs. Wire types, PxService and KvService client/server
// traits.
#[allow(clippy::wildcard_imports, clippy::let_unit_value, missing_docs)]
#[rustfmt::skip]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/crowkv.rpc.rs"));
}

pub use generated::*;

pub(crate) mod px_service;
pub(crate) mod kv_service;
pub use px_service::PxReplicaService;
pub use kv_service::KvStoreService;
