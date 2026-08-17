// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Erasure-coding wrapper — Reed-Solomon GF(2^8) encode/decode.
//!
//! Backend: isa-l (FFI, AVX2/AVX512-accelerated). The public API is
//! backend-agnostic.

#![allow(clippy::must_use_candidate, clippy::missing_errors_doc)]

use thiserror::Error;

use crate::ec_isal::{isal_decode, isal_encode};

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

    // Encode parity via isa-l.
    let mut data_refs: Vec<&mut [u8]> = shards
        .iter_mut()
        .take(scheme.data_num)
        .map(Vec::as_mut_slice)
        .collect();
    let parity = isal_encode(&mut data_refs, scheme.data_num, scheme.code_num);
    shards.extend(parity);

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

    if lost == 0 {
        return Ok(blocks
            .into_iter()
            .map(std::option::Option::unwrap_or_default)
            .collect());
    }

    let reconstructed = isal_decode(blocks, scheme.data_num, scheme.code_num);
    Ok(reconstructed)
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
