//! `CrowKV` gRPC services and client library.
//!
//! Minimal consensus RPC service (`Prepare`/`Promise`/`Accept`/`Accepted`)
//! lands in P1 M2 per `doc/plan/plan-consensus.md` §1 M2 and
//! `doc/design/design-rpc.md` §2.
//!
//! Full service set (`PeerService`, `VoteService`, `SnapshotService`,
//! client library with topology cache, retry, `NotLeaderHint` handling)
//! lands in P4 per `doc/plan/plan-rpc.md`.

// Generated from src/rpc/proto/classic_paxos.proto by tonic-build in build.rs.
// Wire types and PeerService client/server traits.
#[allow(clippy::wildcard_imports, clippy::let_unit_value, missing_docs)]
#[rustfmt::skip]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/crowkv.rpc.rs"));
}

pub use generated::*;

pub mod service;
