// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! EC wrapper round-trip tests.

#![allow(clippy::cast_possible_truncation)]

use crow_common::ec::{decode, decode_data, encode, encode_parity, EcError, EcScheme};

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
