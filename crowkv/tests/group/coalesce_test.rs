// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! R36 server-side proposal coalescing integration tests.
//!
//! These tests verify that concurrent single-key proposes are micro-batched
//! into one multi-key Paxos proposal (one slot, one quorum round) when
//! coalescing is enabled, and that the legacy one-proposal-per-key path is
//! unchanged when coalescing is disabled.

use std::sync::Arc;

use crowkv::cluster::group::{ProposeResult, PxGroup};
use crowkv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::CrowKVConfig;

#[allow(clippy::cast_possible_truncation)]
fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1u8); // op_count
    buf.push(0u8); // Put
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

fn coalesce_group(window_us: u64, max_keys: usize) -> Arc<PxGroup> {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    let config = CrowKVConfig {
        wal_early_ack: false,
        async_engine_apply: false,
        paxos: crowkv::common::config::PaxosConfig {
            coalesce_window_us: window_us,
            coalesce_max_keys: max_keys,
            ..crowkv::common::config::PaxosConfig::DEFAULT
        },
        ..CrowKVConfig::default()
    };
    group.set_from_config(&config);
    let arc = Arc::new(group);
    arc.set_self_weak();
    arc
}

#[tokio::test]
async fn coalesce_disabled_when_window_zero() {
    // coalesce_window_us = 0 → each propose gets its own slot.
    let group = coalesce_group(0, 32);

    let r1 = group.propose(encode_put(b"k1", b"v1"), Some(1), Some(1)).await;
    let r2 = group.propose(encode_put(b"k2", b"v2"), Some(1), Some(2)).await;

    let slot1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    let slot2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    assert_ne!(slot1, slot2, "disabled coalescer must not share slots");
}

#[tokio::test]
async fn coalesce_concurrent_ops_share_slot() {
    // 10ms window: concurrent proposes should batch into one slot.
    let group = coalesce_group(10_000, 32);

    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"k1", b"v1"), Some(10), Some(1)).await });
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"k2", b"v2"), Some(20), Some(1)).await });
    let g = Arc::clone(&group);
    let h3 = tokio::spawn(async move { g.propose(encode_put(b"k3", b"v3"), Some(30), Some(1)).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();

    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    let s3 = match r3 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    assert_eq!(s1, s2, "coalesced ops must share slot");
    assert_eq!(s2, s3, "coalesced ops must share slot");
}

#[tokio::test]
async fn coalesce_dedup_tags_recorded() {
    // After a coalesced batch commits, each (client_id, seq) must be
    // individually retrievable via dedup_lookup.
    let group = coalesce_group(10_000, 32);

    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"dk1", b"v1"), Some(100), Some(1)).await });
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"dk2", b"v2"), Some(200), Some(1)).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let slot = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    match r2 {
        ProposeResult::Chosen { slot: s2 } => assert_eq!(slot, s2),
        other => panic!("expected Chosen, got {other:?}"),
    }

    let learner = &group.local_replica().learner;
    assert_eq!(learner.dedup_lookup(100, 1), Some(slot));
    assert_eq!(learner.dedup_lookup(200, 1), Some(slot));
}

#[tokio::test]
async fn coalesce_max_keys_flushes_immediately() {
    // max_keys = 2: the second op fills the batch and flushes without
    // waiting for the timer. Both ops share a slot.
    let group = coalesce_group(60_000_000, 2);

    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"mk1", b"v1"), Some(1), Some(1)).await });
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"mk2", b"v2"), Some(2), Some(1)).await });

    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();

    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    assert_eq!(s1, s2, "max_keys flush must batch both ops");
}

#[tokio::test]
async fn coalesce_applies_all_keys_to_engine() {
    // After a coalesced batch commits, all keys must be visible in the
    // engine.
    let group = coalesce_group(10_000, 32);

    let g = Arc::clone(&group);
    let h1 = tokio::spawn(async move { g.propose(encode_put(b"ak1", b"v1"), Some(1), Some(1)).await });
    let g = Arc::clone(&group);
    let h2 = tokio::spawn(async move { g.propose(encode_put(b"ak2", b"v2"), Some(2), Some(1)).await });

    let _ = h1.await.unwrap();
    let _ = h2.await.unwrap();

    let learner = &group.local_replica().learner;
    let v1 = learner.engine_get(b"ak1").await.expect("ak1 missing");
    let v2 = learner.engine_get(b"ak2").await.expect("ak2 missing");
    assert_eq!(v1.1, b"v1");
    assert_eq!(v2.1, b"v2");
}

#[tokio::test]
async fn coalesce_sequential_batches_get_increasing_slots() {
    // Two sequential batches (each with one op) must get different slots.
    let group = coalesce_group(5_000, 32);

    let r1 = group.propose(encode_put(b"sb1", b"v1"), Some(1), Some(1)).await;
    // Wait past the window so the first batch flushes before the second.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let r2 = group.propose(encode_put(b"sb2", b"v2"), Some(2), Some(1)).await;

    let s1 = match r1 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    let s2 = match r2 {
        ProposeResult::Chosen { slot } => slot,
        other => panic!("expected Chosen, got {other:?}"),
    };
    assert!(s2 > s1, "second batch must get a higher slot");
}
