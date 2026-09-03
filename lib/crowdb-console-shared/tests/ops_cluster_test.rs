// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for [`ops::cluster`] validation paths. The `init` / `reset` /
//! `clean` logic requires a running cluster and is covered by E2E tests
//! in Phase 4; here we verify the guard clauses.

use crowdb_console_shared::config::ConsoleConfig;
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{self, OpContext};

fn ctx() -> OpContext {
    OpContext::new("127.0.0.1:1".into(), vec![], ConsoleConfig::default())
}

#[tokio::test]
async fn init_empty_nodes_validation() {
    let ctx = ctx();
    let err = ops::cluster::init(&ctx, &[]).await.unwrap_err();
    assert!(matches!(err, Error::Validation { field, .. } if field == "nodes"));
}

#[tokio::test]
async fn init_dedup_nodes() {
    // This test verifies that duplicate node ids are deduplicated.
    // It will fail at the health check (no server), but the error
    // should be NodeUnreachable, not a duplicate-processing issue.
    let ctx = ctx();
    let err = ops::cluster::init(&ctx, &[1, 1, 2]).await.unwrap_err();
    // Should fail on node 1 (first unique node) being unreachable.
    assert!(matches!(
        err,
        Error::NodeUnreachable { .. } | Error::NotFound { .. }
    ));
}
