// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! EC wrapper round-trip tests.

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::similar_names, clippy::doc_markdown)]

use crowdb_common::ec::{
    decode, decode_data, encode, encode_parity, encode_parity_from_shards, EcError, EcScheme,
};

/// Convert shards to Option-wrapped blocks (all present).
fn all_present(shards: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
    shards.into_iter().map(Some).collect()
}

#[test]
fn ec_round_trip_6_3_no_loss() {
    let scheme = EcScheme::new(6, 3);
    let data = vec![0xAB; 600];
    let shards = encode(scheme, &data).unwrap();
    assert_eq!(shards.len(), 9);

    let blocks = all_present(shards);
    let recovered = decode_data(scheme, blocks).unwrap();
    assert_eq!(&recovered[..600], &data[..]);
}

#[test]
fn ec_round_trip_6_3_lose_3_data_blocks() {
    let scheme = EcScheme::new(6, 3);
    let data = vec![0x42; 600];
    let shards = encode(scheme, &data).unwrap();
    let mut blocks = all_present(shards);

    // Lose 3 data blocks (indices 0, 2, 4).
    blocks[0] = None;
    blocks[2] = None;
    blocks[4] = None;

    let recovered = decode_data(scheme, blocks).unwrap();
    assert_eq!(&recovered[..600], &data[..]);
}

#[test]
fn ec_round_trip_6_3_lose_3_parity_blocks() {
    let scheme = EcScheme::new(6, 3);
    let data = vec![0x42; 600];
    let shards = encode(scheme, &data).unwrap();
    let mut blocks = all_present(shards);

    // Lose all 3 parity blocks (indices 6, 7, 8).
    blocks[6] = None;
    blocks[7] = None;
    blocks[8] = None;

    let recovered = decode_data(scheme, blocks).unwrap();
    assert_eq!(&recovered[..600], &data[..]);
}

#[test]
fn ec_round_trip_6_3_lose_mixed_blocks() {
    let scheme = EcScheme::new(6, 3);
    let data: Vec<u8> = (0..600u32).map(|i| (i % 256) as u8).collect();
    let shards = encode(scheme, &data).unwrap();
    let mut blocks = all_present(shards);

    // Lose 2 data + 1 parity.
    blocks[1] = None;
    blocks[5] = None;
    blocks[7] = None;

    let recovered = decode_data(scheme, blocks).unwrap();
    assert_eq!(&recovered[..600], &data[..]);
}

#[test]
fn ec_lose_too_many_blocks_fails() {
    let scheme = EcScheme::new(6, 3);
    let data = vec![0x42; 600];
    let shards = encode(scheme, &data).unwrap();
    let mut blocks = all_present(shards);

    // Lose 4 blocks (code_num=3, so 4 is unrecoverable).
    blocks[0] = None;
    blocks[1] = None;
    blocks[2] = None;
    blocks[6] = None;

    let result = decode(scheme, blocks);
    assert!(matches!(
        result,
        Err(EcError::TooManyLost { lost: 4, code_num: 3 })
    ));
}

#[test]
fn ec_encode_parity_only() {
    let scheme = EcScheme::new(4, 2);
    let data = vec![0x77; 400];
    let parity = encode_parity(scheme, &data).unwrap();
    assert_eq!(parity.len(), 2);
    assert!(!parity[0].is_empty());
    assert!(!parity[1].is_empty());
}

#[test]
fn ec_invalid_scheme_fails() {
    let scheme = EcScheme::new(0, 3);
    let result = encode(scheme, &[]);
    assert!(matches!(result, Err(EcError::InvalidScheme { .. })));
}

#[test]
fn ec_non_divisible_data() {
    let scheme = EcScheme::new(4, 2);
    let data = vec![0x55; 301]; // not divisible by 4
    let shards = encode(scheme, &data).unwrap();
    // Should zero-pad; shard size = ceil(301/4) = 76.
    assert_eq!(shards[0].len(), 76);

    let blocks = all_present(shards);
    let recovered = decode_data(scheme, blocks).unwrap();
    assert_eq!(&recovered[..301], &data[..]);
    // Padding bytes are zero.
    assert_eq!(&recovered[301..], &[0u8; 3]);
}

// ── encode_parity_from_shards (R94 partial-EC gate) ──────────────

/// Full strip: 4 data shards (1 MB each), 4+1 EC. Encode parity from
/// shards, then verify parity matches the contiguous-buffer encode.
/// Decode with all 5 blocks present → data reconstructs.
#[test]
fn encode_parity_from_shards_full_strip() {
    let scheme = EcScheme::new(4, 1);
    let shard_size = 1024 * 1024;
    let data: Vec<u8> = (0..(shard_size * 4) as u32).map(|i| (i % 251) as u8).collect();
    let shards: Vec<&[u8]> = (0..4)
        .map(|i| &data[i * shard_size..(i + 1) * shard_size])
        .collect();

    let parity = encode_parity_from_shards(scheme, &shards).unwrap();
    assert_eq!(parity.len(), 1);
    assert_eq!(parity[0].len(), shard_size);

    // Cross-check: contiguous encode should produce the same parity.
    let full = encode(scheme, &data).unwrap();
    assert_eq!(&parity[0], &full[4]);

    // Decode with all 5 present → data reconstructs.
    let mut blocks: Vec<Option<Vec<u8>>> = shards.iter().map(|s| Some(s.to_vec())).collect();
    blocks.extend(parity.into_iter().map(Some));
    let recovered = decode_data(scheme, blocks).unwrap();
    assert_eq!(recovered, data);
}

/// Partial strip: 2 of 4 data shards (4+1 EC). Encode parity with 2
/// zero placeholders. Verify parity is consistent: lose 1 real data
/// shard, decode with 3 data + 1 parity (1 lost ≤ code_num 1) →
/// reconstruct the lost shard exactly.
#[test]
fn encode_parity_from_shards_partial_2_of_4() {
    let scheme = EcScheme::new(4, 1);
    let shard_size = 1024 * 1024;
    let shard0: Vec<u8> = (0..shard_size as u32).map(|i| (i % 251) as u8).collect();
    let shard1: Vec<u8> = (0..shard_size as u32).map(|i| (i % 197) as u8).collect();
    let shards: Vec<&[u8]> = vec![&shard0, &shard1];

    let parity = encode_parity_from_shards(scheme, &shards).unwrap();
    assert_eq!(parity.len(), 1);
    assert_eq!(parity[0].len(), shard_size);

    // Build full 5 blocks: 2 real + 2 zero + 1 parity.
    let mut blocks: Vec<Option<Vec<u8>>> = vec![
        Some(shard0.clone()),
        Some(shard1.clone()),
        Some(vec![0u8; shard_size]),
        Some(vec![0u8; shard_size]),
        Some(parity[0].clone()),
    ];

    // Lose shard 0 → 1 lost ≤ code_num 1 → reconstruct.
    blocks[0] = None;
    let recovered = decode(scheme, blocks).unwrap();
    assert_eq!(recovered[0], shard0);
    assert_eq!(recovered[1], shard1);

    // Also verify: lose shard 1 → reconstruct.
    let mut blocks2: Vec<Option<Vec<u8>>> = vec![
        Some(shard0.clone()),
        Some(shard1.clone()),
        Some(vec![0u8; shard_size]),
        Some(vec![0u8; shard_size]),
        Some(parity[0].clone()),
    ];
    blocks2[1] = None;
    let recovered2 = decode(scheme, blocks2).unwrap();
    assert_eq!(recovered2[0], shard0);
    assert_eq!(recovered2[1], shard1);
}

/// Single short shard (1 of 4, < 1 MB). Encode parity → parity length
/// matches shard length.
#[test]
fn encode_parity_from_shards_single_short() {
    let scheme = EcScheme::new(4, 1);
    let shard: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
    let shards: Vec<&[u8]> = vec![&shard];

    let parity = encode_parity_from_shards(scheme, &shards).unwrap();
    assert_eq!(parity.len(), 1);
    assert_eq!(parity[0].len(), shard.len());

    // Decode with 1 real + 3 zero + 1 parity, lose the real shard.
    let mut blocks: Vec<Option<Vec<u8>>> = vec![
        Some(shard.clone()),
        Some(vec![0u8; shard.len()]),
        Some(vec![0u8; shard.len()]),
        Some(vec![0u8; shard.len()]),
        Some(parity[0].clone()),
    ];
    blocks[0] = None;
    let recovered = decode(scheme, blocks).unwrap();
    assert_eq!(recovered[0], shard);
}

/// Cross-check: encode_parity_from_shards with a full strip produces
/// the same parity as the contiguous `encode` path.
#[test]
fn encode_parity_from_shards_matches_encode() {
    let scheme = EcScheme::new(4, 2);
    let data: Vec<u8> = (0..400u32).map(|i| (i % 256) as u8).collect();
    let shard_size = data.len() / 4;
    let shards: Vec<&[u8]> = (0..4)
        .map(|i| &data[i * shard_size..(i + 1) * shard_size])
        .collect();

    let parity_shards = encode_parity_from_shards(scheme, &shards).unwrap();
    let full = encode(scheme, &data).unwrap();
    assert_eq!(parity_shards, &full[4..]);
}

/// Invalid inputs.
#[test]
fn encode_parity_from_shards_invalid() {
    let scheme = EcScheme::new(4, 1);

    // Empty shards.
    assert!(encode_parity_from_shards(scheme, &[]).is_err());

    // Too many shards.
    let s = vec![0u8; 4];
    let shards: Vec<&[u8]> = vec![&s, &s, &s, &s, &s];
    assert!(encode_parity_from_shards(scheme, &shards).is_err());

    // Unequal shard lengths.
    let s1 = vec![0u8; 4];
    let s2 = vec![0u8; 8];
    let shards: Vec<&[u8]> = vec![&s1, &s2];
    assert!(encode_parity_from_shards(scheme, &shards).is_err());

    // Invalid scheme.
    assert!(encode_parity_from_shards(EcScheme::new(0, 1), &[&s]).is_err());
}
