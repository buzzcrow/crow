// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::needless_range_loop
)]

//! UTs for `EcWorker` — streaming EC compute.

use bytes::Bytes;
use crow_chunk_client::EcWorker;
use crow_common::ec::{decode, encode_parity_from_shards, EcScheme};

#[test]
fn ec_worker_full_strip_4_1() {
    let scheme = EcScheme::new(4, 1);
    let mut worker = EcWorker::new(scheme);

    let shard_size = 4096;
    let data: Vec<Vec<u8>> = (0..4u32)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * shard_size as u32 + j as u32) % 251) as u8)
                .collect()
        })
        .collect();

    for shard in &data {
        worker.push(&Bytes::from(shard.clone())).unwrap();
    }

    let parity = worker.finish().unwrap();
    assert_eq!(parity.len(), 1);
    assert_eq!(parity[0].len(), shard_size);

    // Compare with direct encode.
    let shard_refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let expected = encode_parity_from_shards(scheme, &shard_refs).unwrap();
    assert_eq!(parity, expected);

    // Decode round-trip.
    let mut blocks: Vec<Option<Vec<u8>>> = data.into_iter().map(Some).collect();
    blocks.extend(parity.into_iter().map(Some));
    let recovered = decode(scheme, blocks).unwrap();
    for i in 0..4u32 {
        let expected: Vec<u8> = (0..shard_size)
            .map(|j| ((i * shard_size as u32 + j as u32) % 251) as u8)
            .collect();
        assert_eq!(recovered[i as usize], expected);
    }
}

#[test]
fn ec_worker_reset_reuse() {
    let scheme = EcScheme::new(4, 1);
    let mut worker = EcWorker::new(scheme);

    // First strip — 4 different shards.
    let strip1: Vec<Vec<u8>> = (0..4u32)
        .map(|s| (0..4096).map(|j| ((s * 4096 + j as u32) % 251) as u8).collect())
        .collect();
    for shard in &strip1 {
        worker.push(&Bytes::from(shard.clone())).unwrap();
    }
    let parity1 = worker.finish().unwrap();
    assert_eq!(parity1.len(), 1);

    // Reset + reuse for second strip with different data.
    worker.reset();
    assert_eq!(worker.shards_received(), 0);

    let strip2: Vec<Vec<u8>> = (0..4u32)
        .map(|s| {
            (0..4096)
                .map(|j| ((s * 4096 + j as u32 + 100) % 251) as u8)
                .collect()
        })
        .collect();
    for shard in &strip2 {
        worker.push(&Bytes::from(shard.clone())).unwrap();
    }
    let parity2 = worker.finish().unwrap();
    assert_eq!(parity2.len(), 1);
    assert_ne!(parity1, parity2);
}

#[test]
fn ec_worker_too_many_shards() {
    let scheme = EcScheme::new(4, 1);
    let mut worker = EcWorker::new(scheme);

    for _ in 0..4 {
        worker.push(&Bytes::from(vec![0u8; 4096])).unwrap();
    }
    // 5th shard should fail.
    let result = worker.push(&Bytes::from(vec![0u8; 4096]));
    assert!(result.is_err());
}

#[test]
fn ec_worker_shards_received() {
    let scheme = EcScheme::new(4, 1);
    let mut worker = EcWorker::new(scheme);

    assert_eq!(worker.shards_received(), 0);
    worker.push(&Bytes::from(vec![0u8; 4096])).unwrap();
    assert_eq!(worker.shards_received(), 1);
    worker.push(&Bytes::from(vec![0u8; 4096])).unwrap();
    assert_eq!(worker.shards_received(), 2);
    worker.reset();
    assert_eq!(worker.shards_received(), 0);
}
