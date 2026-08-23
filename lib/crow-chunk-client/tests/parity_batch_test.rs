// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! UTs for `ParityBatch` — parallel write tracker.

use crow_chunk_client::{IoError, ParityBatch};

#[tokio::test]
async fn parity_batch_join_all_success() {
    let mut batch = ParityBatch::new();
    for _ in 0..5 {
        batch.spawn(tokio::spawn(async { Ok(()) }));
    }
    assert_eq!(batch.in_flight(), 5);
    batch.join_all().await.unwrap();
    assert_eq!(batch.in_flight(), 0);
}

#[tokio::test]
async fn parity_batch_join_all_first_error() {
    let mut batch = ParityBatch::new();
    batch.spawn(tokio::spawn(async {
        Err(IoError::WriteFailed("injected".into()))
    }));
    batch.spawn(tokio::spawn(async { Ok(()) }));
    batch.spawn(tokio::spawn(async { Ok(()) }));
    let result = batch.join_all().await;
    assert!(matches!(result, Err(IoError::WriteFailed(_))));
}

#[tokio::test]
async fn parity_batch_abort_all() {
    let mut batch = ParityBatch::new();
    for _ in 0..3 {
        batch.spawn(tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Ok(())
        }));
    }
    assert_eq!(batch.in_flight(), 3);
    batch.abort_all();
    assert_eq!(batch.in_flight(), 0);
}

#[tokio::test]
async fn parity_batch_empty_join() {
    let mut batch = ParityBatch::new();
    assert_eq!(batch.in_flight(), 0);
    batch.join_all().await.unwrap();
}

#[tokio::test]
async fn parity_batch_panic_propagates() {
    let mut batch = ParityBatch::new();
    batch.spawn(tokio::spawn(async {
        panic!("task panicked");
    }));
    let result = batch.join_all().await;
    assert!(matches!(result, Err(IoError::Internal(_))));
}
