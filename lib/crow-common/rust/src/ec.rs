// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Erasure-coding wrapper — Reed-Solomon GF(2^8) encode/decode.
//!
//! Backend: `reed-solomon-erasure` (pure Rust). The public API is
//! backend-agnostic; isa-l can replace it behind the same interface
//! when available (see chunkdb-gap.md GAP-2).

#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

use thiserror::Error;

/// EC error.
#[derive(Debug, Error)]
pub enum EcError {
    #[error("invalid block count: data={data_num}, code={code_num}")]
    InvalidScheme { data_num: usize, code_num: usize },
    #[error("data length {data_len} not divisible by data_num {data_num}")]
    NotDivisible { data_len: usize, data_num: usize },
    #[error("too many blocks lost: lost={lost}, code_num={code_num}")]
    TooManyLost { lost: usize, code_num: usize },
    #[error("block {index} is missing and cannot be reconstructed")]
    MissingBlock { index: usize },
    #[error("backend error: {0}")]
    Backend(String),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, EcError>;

/// EC scheme configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcScheme {
    pub data_num: usize,
    pub code_num: usize,
}

impl EcScheme {
    pub const fn new(data_num: usize, code_num: usize) -> Self {
        Self { data_num, code_num }
    }

    pub const fn total_blocks(&self) -> usize {
        self.data_num + self.code_num
    }
}

/// Encode `data` into `data_num` data shards + `code_num` parity shards.
///
/// The data is split into `data_num` equal-sized shards. If the data
/// length is not divisible by `data_num`, the last shard is zero-padded
/// (the original length is not tracked — callers must store it
/// separately if needed).
///
/// Returns a vector of `data_num + code_num` shards, each of size
/// `ceil(data.len() / data_num)`.
pub fn encode(scheme: EcScheme, data: &[u8]) -> Result<Vec<Vec<u8>>> {
    if scheme.data_num == 0 || scheme.code_num == 0 {
        return Err(EcError::InvalidScheme {
            data_num: scheme.data_num,
            code_num: scheme.code_num,
        });
    }

    let shard_size = data.len().div_ceil(scheme.data_num);
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(scheme.total_blocks());

    // Split data into data_num shards (zero-pad last shard).
    for i in 0..scheme.data_num {
        let start = i * shard_size;
        let end = (start + shard_size).min(data.len());
        let mut shard = vec![0u8; shard_size];
        if start < end {
            shard[..end - start].copy_from_slice(&data[start..end]);
        }
        shards.push(shard);
    }

    // Initialize code_num parity shards (zero-filled).
    for _ in 0..scheme.code_num {
        shards.push(vec![0u8; shard_size]);
    }

    let r: reed_solomon_erasure::ReedSolomon<reed_solomon_erasure::galois_8::Field> =
        reed_solomon_erasure::ReedSolomon::new(scheme.data_num, scheme.code_num)
            .map_err(|e| EcError::Backend(e.to_string()))?;

    // Encode: fill parity shards in-place.
    let shard_refs: Vec<&mut [u8]> = shards.iter_mut().map(Vec::as_mut_slice).collect();
    r.encode(shard_refs)
        .map_err(|e| EcError::Backend(e.to_string()))?;

    Ok(shards)
}

/// Decode and reconstruct lost blocks.
///
/// `blocks` is a vector of `data_num + code_num` entries. Each entry is
/// `Some(bytes)` if the block survived, `None` if it was lost. Up to
/// `code_num` blocks can be lost. Returns the reconstructed shards.
pub fn decode(scheme: EcScheme, blocks: Vec<Option<Vec<u8>>>) -> Result<Vec<Vec<u8>>> {
    if scheme.data_num == 0 || scheme.code_num == 0 {
        return Err(EcError::InvalidScheme {
            data_num: scheme.data_num,
            code_num: scheme.code_num,
        });
    }

    let total = scheme.total_blocks();
    if blocks.len() != total {
        return Err(EcError::Backend(format!(
            "expected {total} blocks, got {}",
            blocks.len()
        )));
    }

    let lost = blocks.iter().filter(|b| b.is_none()).count();
    if lost > scheme.code_num {
        return Err(EcError::TooManyLost {
            lost,
            code_num: scheme.code_num,
        });
    }

    let shard_size = blocks
        .iter()
        .filter_map(|b| b.as_ref())
        .map(Vec::len)
        .max()
        .unwrap_or(0);

    // Build (shard, present) tuples for the reconstruct API.
    let mut shards: Vec<(Vec<u8>, bool)> = Vec::with_capacity(total);
    for block in blocks {
        match block {
            Some(b) => {
                if b.len() < shard_size {
                    let mut padded = b;
                    padded.resize(shard_size, 0);
                    shards.push((padded, true));
                } else {
                    shards.push((b, true));
                }
            }
            None => shards.push((vec![0u8; shard_size], false)),
        }
    }

    let r: reed_solomon_erasure::ReedSolomon<reed_solomon_erasure::galois_8::Field> =
        reed_solomon_erasure::ReedSolomon::new(scheme.data_num, scheme.code_num)
            .map_err(|e| EcError::Backend(e.to_string()))?;

    r.reconstruct(&mut shards)
        .map_err(|e| EcError::Backend(e.to_string()))?;

    Ok(shards.into_iter().map(|(s, _)| s).collect())
}

/// Convenience: encode and return only the parity shards.
pub fn encode_parity(scheme: EcScheme, data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let shards = encode(scheme, data)?;
    Ok(shards[scheme.data_num..].to_vec())
}

/// Convenience: decode and return only the data shards concatenated.
pub fn decode_data(scheme: EcScheme, blocks: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>> {
    let shards = decode(scheme, blocks)?;
    let mut data = Vec::new();
    for shard in shards.iter().take(scheme.data_num) {
        data.extend_from_slice(shard);
    }
    Ok(data)
}
