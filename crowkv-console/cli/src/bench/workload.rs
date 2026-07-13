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
    value_size: usize,
}

impl OpGen {
    #[must_use]
    pub fn new(seed: u64, key_space: u64, value_size: usize) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            key_space: key_space.max(1),
            value_size,
        }
    }

    pub fn next_key(&mut self) -> Vec<u8> {
        let id: u64 = self.rng.gen_range(0..self.key_space);
        format!("k{id:020}").into_bytes()
    }

    /// Fixed-size payload; same byte (`b'v'`) so the wire transfer cost
    /// is what we're really measuring.
    #[must_use]
    pub fn make_value(&self) -> Vec<u8> {
        vec![b'v'; self.value_size]
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
