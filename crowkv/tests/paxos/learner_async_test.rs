// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Regression gate for the deferred
//! caller-side conversion: proves `PxLearner::engine_get` genuinely awaits
//! through a real `KVFuture::Pending` end-to-end, not just that
//! `CrowtreeEngine::get` constructs one in isolation
//! (`kv/crowtree_engine_test.rs`'s `get_constructs_pending_for_genuine_demand_load_miss`
//! covers that layer). This is the test the design doc's §6 explicitly
//! deferred adding until `PxLearner` actually had an `async fn` to test.

use crowkv::kv::{CrowtreeEngine, CrowtreeOptions};
use crowkv::paxos::learner::PxLearner;
use crowkv::paxos::roles::{Learner, PxBallot, PxLogEntry};

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(1u8); // op_count
    payload.push(0u8); // Put
    payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    payload
}

/// `PxLearner::engine_get`, backed by a durable (file-backed)
/// `CrowtreeEngine`, resolves correctly whether the underlying
/// `KVFuture` is `Ready` (resident hit) or genuinely `Pending` (demand-load
/// miss after eviction) -- `.await`ing either case must produce the same
/// answer a synchronous `into_ready()` would have, for the case where it's
/// actually still legal to call that (the `Ready` case).
#[tokio::test]
async fn engine_get_resolves_correctly_across_both_ready_and_pending() {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let engine = CrowtreeEngine::open(&CrowtreeOptions {
        path: Some(tmp.path().to_string_lossy().into_owned()),
        ..Default::default()
    })
    .expect("open durable engine");
    // Grab a shared handle to the underlying `Crowtree` *before* boxing
    // `engine` into `PxLearner` -- `Arc<Crowtree>`, so flush/snapshot/evict
    // below act on the exact same tree the learner's `KVEngine::get` reads.
    let handle = engine.handle();
    let learner = PxLearner::with_engine(Box::new(engine));

    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: bytes::Bytes::from(encode_put_payload(b"k", b"v")),
    };
    learner.learn(entry, None, None).await;

    // Resident hit: the underlying KVFuture is Ready here (no eviction yet).
    assert_eq!(
        learner.engine_get(b"k").await,
        Some((1, b"v".to_vec())),
        "resident hit resolves via the Ready fast path"
    );

    // Force the leaf unloaded: `engine_get`'s next call now has to await a
    // genuine KVFuture::Pending underneath (see the engine-layer
    // `get_constructs_pending_for_genuine_demand_load_miss` regression
    // guard in `kv/crowtree_engine_test.rs` for the isolated version of
    // this same property).
    handle.flush().expect("flush");
    handle.snapshot().expect("snapshot");
    let evicted = handle.evict_clean_leaves(0);
    assert!(
        evicted > 0,
        "snapshot should have made the leaf clean and evictable"
    );

    assert_eq!(
        learner.engine_get(b"k").await,
        Some((1, b"v".to_vec())),
        "demand-load miss resolves via the Pending path with the same answer"
    );
}
