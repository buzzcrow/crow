// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Full-stack E2E test: real KV cluster + diskdb + chunkdb in-process.
//!
//! Verifies the allocate → append → seal → query → delete lifecycle
//! against a real 3-node crow-kv-server cluster with diskdb running
//! in-process as a gRPC server.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster};
use crow_chunkdb::lifecycle::LifecycleError;
use crow_protocol::chunkdb::rpc::{ChunkState, ChunkType, StripType};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn chunkdb_full_stack_allocate_seal_delete() {
    // Skip if crow-kv-server binary is not built.
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && common::cluster::crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start the kv cluster.
    let cluster = KvCluster::start().await;
    eprintln!("kv cluster started");

    // 2. Seed hardware metadata.
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;
    eprintln!("hardware seeded");

    // 3. Start diskdb in-process.
    let diskdb = DiskdbServer::start(&cluster).await;
    eprintln!("diskdb started: {}", diskdb.grpc_endpoint);

    // 4. Wire chunkdb handler.
    let harness = ChunkdbHarness::start(&cluster).await;
    eprintln!("chunkdb harness ready");

    // 5. Allocate a chunk (1 unit = 1 MB per strip, 3 mirror copies).
    let chunk = harness
        .handler
        .allocate_chunk(
            None,
            1, // 1 unit per strip
            1, // 1 strip
            StripType::Mirror,
            0,
            0,
            3, // 3 mirror copies
            ChunkType::Repo,
        )
        .await
        .expect("allocate_chunk");
    assert_eq!(chunk.state, ChunkState::Active as i32);
    assert!(!chunk.strips.is_empty(), "chunk should have strips");
    eprintln!("chunk allocated: {} strips", chunk.strips.len());

    // 6. Query the chunk.
    let chunk_id = chunk.id.as_ref().expect("chunk has id");
    let queried = harness.handler.query_chunk(chunk_id).await.expect("query_chunk");
    assert_eq!(queried.id, chunk.id);
    assert_eq!(queried.state, ChunkState::Active as i32);
    eprintln!("chunk queried");

    // 7. Seal the chunk.
    let sealed = harness
        .handler
        .seal_chunk(chunk_id, 100)
        .await
        .expect("seal_chunk");
    assert_eq!(sealed.state, ChunkState::Sealed as i32);
    assert_eq!(sealed.sealed_length, 100);
    eprintln!("chunk sealed");

    // 8. Delete the chunk.
    let deleted = harness
        .handler
        .delete_chunk(chunk_id)
        .await
        .expect("delete_chunk");
    assert_eq!(deleted.state, ChunkState::Deleted as i32);
    eprintln!("chunk deleted");

    // 9. Delete again → should return ChunkNotFound (GAP-9).
    let result = harness.handler.delete_chunk(chunk_id).await;
    assert!(
        matches!(result, Err(LifecycleError::ChunkNotFound)),
        "second delete should return ChunkNotFound, got {result:?}"
    );
    eprintln!("second delete returned ChunkNotFound (correct)");

    // 10. Query after delete → should return the deleted chunk (state=Deleted).
    let queried_after = harness
        .handler
        .query_chunk(chunk_id)
        .await
        .expect("query after delete");
    assert_eq!(queried_after.state, ChunkState::Deleted as i32);
    eprintln!("chunk queried after delete (state=Deleted)");
}
