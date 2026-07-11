//! Constructor helpers for the prost-generated [`KvResponse`].
//!
//! Centralizes the otherwise-repeated `KvResponse { version: 1, ok, …,
//! request_id, request_create_ms }` initialization shapes so adding a
//! new proto field is a one-line change rather than a four-call-site
//! audit. See `crowkv/src/cluster/px_kv_store.rs` for the original
//! expanded construction.

use super::KvResponse;

impl KvResponse {
    /// Wire-format version emitted by every response. Bump only when
    /// the protobuf schema gains a backward-incompatible field.
    pub const VERSION: u32 = 1;

    /// Successful proposal commit at `revision` (Paxos slot). Used by
    /// `kv_put` / `kv_delete` / `kv_batch_write`.
    #[must_use]
    pub fn ok_chosen(revision: u64, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: true,
            revision,
            error: String::new(),
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value: Vec::new(),
        }
    }

    /// Successful read returning `value`. Used by `kv_get` hits.
    #[must_use]
    pub fn ok_value(value: Vec<u8>, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: true,
            revision: 0,
            error: String::new(),
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value,
        }
    }

    /// Read miss — key absent in the local learner store.
    #[must_use]
    pub fn not_found(request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: false,
            revision: 0,
            error: String::new(),
            not_found: true,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value: Vec::new(),
        }
    }

    /// Write rejected because the local replica is not the leader. The
    /// `hint` carries the known leader's gRPC endpoint when available.
    #[must_use]
    pub fn not_leader(hint: String, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: false,
            revision: 0,
            error: "not leader".to_string(),
            not_found: false,
            not_leader_hint: hint,
            request_id,
            request_create_ms,
            value: Vec::new(),
        }
    }

    /// Generic error path (proposal failure other than `NotLeader`).
    #[must_use]
    pub fn err(msg: String, request_id: u64, request_create_ms: u64) -> Self {
        Self {
            version: Self::VERSION,
            ok: false,
            revision: 0,
            error: msg,
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
            value: Vec::new(),
        }
    }
}
