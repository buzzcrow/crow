// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for [`ops::kv_logical`] validation paths. The fan-out + rollback
//! logic requires a running cluster and is covered by E2E tests in
//! Phase 4; here we verify the guard clauses that run before any RPC.

use crowdb_console_shared::config::ConsoleConfig;
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{self, OpContext};

fn ctx() -> OpContext {
    OpContext::new("127.0.0.1:1".into(), vec![], ConsoleConfig::default())
}

#[tokio::test]
async fn add_store_no_nodes_no_servers_validation() {
    let ctx = ctx();
    let err = ops::kv_logical::add_store(&ctx, 1, &[]).await.unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "nodes"));
}

#[tokio::test]
async fn remove_store_zero_validation() {
    let ctx = ctx();
    let err = ops::kv_logical::remove_store(&ctx, 0).await.unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "store_id"));
}

#[tokio::test]
async fn add_group_empty_nodes_validation() {
    let ctx = ctx();
    let err = ops::kv_logical::add_group(&ctx, 1, 1, 1, &[]).await.unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "nodes"));
}

#[tokio::test]
async fn remove_group_zero_zero_validation() {
    let ctx = ctx();
    let err = ops::kv_logical::remove_group(&ctx, 0, 0).await.unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "group_id"));
}
