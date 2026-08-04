// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Workload kinds and per-operation dispatch for the bench engine.
//!
//! Key work: parse `WorkloadKind` from CLI strings, classify each
//! issued op into an `OpKind` for stats bucketing, generate keys/values
//! deterministically from a worker-local rng so independent runs are
//! comparable.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// CLI-facing workload selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadKind {
    Read,
    Write,
    /// Reserved: maps to scan/list. Until the server implements prefix
    /// scan, the runner returns an "unsupported" `OpKind::List` error
    /// for every op and the bench finishes with `error_rate = 1.0`. CLI
    /// surfaces a helpful message.
    List,
    /// Default 50/50 read/write.
    Mix,
}

/// `MinSlot` read `min_slot` resolution policy for read benches.
/// `Auto` passes `None` so the client auto-attaches its write watermark
/// (the production default); `Zero` forces `Some(0)` (max staleness,
/// pure local-serve); `Fixed(n)` forces `Some(n)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MinSlotPolicy {
    Auto,
    Zero,
    Fixed(u64),
}

impl MinSlotPolicy {
    /// Parse `"auto" | "zero" | "<n>"`. Case-insensitive for the named
    /// variants; a numeric string is parsed as `Fixed(n)`.
    ///
    /// # Errors
    /// Returns the unrecognized input as an `Err` so the CLI can echo
    /// the failing token.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "zero" => Ok(Self::Zero),
            other => match other.parse::<u64>() {
                Ok(n) => Ok(Self::Fixed(n)),
                Err(_) => Err(s.to_string()),
            },
        }
    }

    /// Resolve to the `Option<u64>` the client `get` API expects.
    #[must_use]
    pub fn to_min_slot(self) -> Option<u64> {
        match self {
            Self::Auto => None,
            Self::Zero => Some(0),
            Self::Fixed(n) => Some(n),
        }
    }
}

/// splitmix64 — a portable `u64 -> u64` mixing function (Sebastiano
/// Vigna). Used by `byte_at` so per-byte value generation is O(1) and
/// deterministic across runs without pulling in a new dependency.
#[must_use]
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = z;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic per-byte value generation: each byte of a value is
/// independently computable from `(key_id, offset)` so any single byte
/// is O(1) to verify (8 random bytes = 8 hashes, not 512). The value
/// still looks like random noise, so wire/engine costs are realistic.
/// Pre-population writes use the same formula reads verify against.
#[must_use]
#[allow(clippy::cast_possible_truncation, reason = "mod 256 is the intent")]
pub fn byte_at(key_id: u64, offset: u64) -> u8 {
    splitmix64(key_id ^ splitmix64(offset)) as u8
}

/// Deterministic per-key value generation: each byte is
/// `byte_at(key_id, offset)`. Used by pre-population writes and any
/// read bench that verifies returned bytes against the formula.
#[must_use]
pub fn value_for(key_id: u64, size: usize) -> Vec<u8> {
    (0..size as u64).map(|i| byte_at(key_id, i)).collect()
}

/// Render a key id as the canonical `k{id:020}` zero-padded ascii form
/// used by both pre-population writes and read key selection.
#[must_use]
pub fn format_key(id: u64) -> Vec<u8> {
    format!("k{id:020}").into_bytes()
}

impl WorkloadKind {
    /// Parse `"read" | "write" | "list" | "mix"`. Case-insensitive.
    ///
    /// # Errors
    /// Returns the unrecognized input as an `Err` so the CLI can echo
    /// the failing token.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            "list" | "scan" => Ok(Self::List),
            "mix" => Ok(Self::Mix),
            other => Err(other.to_string()),
        }
    }
}

/// Latency-histogram bucket per op type. The runner records into one
/// histogram per kind so the report can show separate read vs write
/// percentiles for `mix`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    Read,
    Write,
    Delete,
    List,
}

impl OpKind {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Delete => "delete",
            Self::List => "list",
        }
    }
}

/// Per-worker key/value generator. Keys are integer ids drawn from
/// `[0, key_space)` and rendered as zero-padded ascii so the workload
/// stays deterministic and replayable.
pub struct OpGen {
    rng: SmallRng,
    key_space: u64,
    /// Key range for read ops. Defaults to `key_space`; read benches
    /// with pre-population set this to the populated count so reads
    /// target written keys (returning `Found`, not `NotFound`).
    read_key_space: u64,
    value_size: usize,
}

impl OpGen {
    #[must_use]
    pub fn new(seed: u64, key_space: u64, value_size: usize) -> Self {
        let key_space = key_space.max(1);
        Self {
            rng: SmallRng::seed_from_u64(seed),
            key_space,
            read_key_space: key_space,
            value_size,
        }
    }

    /// Set the read key range to `[0, count)`. Used by read benches
    /// with pre-population so reads draw from the populated range.
    pub fn set_read_key_space(&mut self, count: u64) {
        self.read_key_space = count.max(1);
    }

    pub fn next_key(&mut self) -> Vec<u8> {
        let id: u64 = self.rng.gen_range(0..self.key_space);
        format!("k{id:020}").into_bytes()
    }

    /// Draw a read key from `[0, read_key_space)`, returning both the
    /// integer id (for spot-check verification) and the formatted key
    /// bytes (for the `get` call).
    pub fn next_read_key(&mut self) -> (u64, Vec<u8>) {
        let id: u64 = self.rng.gen_range(0..self.read_key_space);
        (id, format_key(id))
    }

    /// Fixed-size payload; same byte (`b'v'`) so the wire transfer cost
    /// is what we're really measuring. Used by write benches that do
    /// not need read-side verification.
    #[must_use]
    pub fn make_value(&self) -> Vec<u8> {
        vec![b'v'; self.value_size]
    }

    /// Spot-check `verify_bytes` random offsets of `value` against the
    /// deterministic `byte_at(key_id, offset)` formula. Returns `true`
    /// if all checked bytes match. When `value` is shorter than
    /// `verify_bytes`, all offsets are checked (clamped). When
    /// `verify_bytes == 0` or `value` is empty, returns `true` (no
    /// verification requested).
    #[must_use]
    pub fn verify_value(&mut self, key_id: u64, value: &[u8], verify_bytes: usize) -> bool {
        if verify_bytes == 0 || value.is_empty() {
            return true;
        }
        let len = value.len() as u64;
        let checks = verify_bytes.min(value.len());
        for _ in 0..checks {
            let offset = self.rng.gen_range(0..len);
            #[allow(clippy::cast_possible_truncation, reason = "offset < len <= usize::MAX")]
            if value[offset as usize] != byte_at(key_id, offset) {
                return false;
            }
        }
        true
    }

    /// Decide which op a worker issues for `Mix`, biased 50/50.
    pub fn pick_mix_kind(&mut self) -> OpKind {
        if self.rng.gen_bool(0.5) {
            OpKind::Read
        } else {
            OpKind::Write
        }
    }
}
