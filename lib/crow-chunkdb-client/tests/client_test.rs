// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkdbClient` unit tests — error mapping, retry config.

use crow_chunkdb_client::{ChunkdbClientError, RetryConfig};
use std::time::Duration;
use tonic::Status;

#[test]
fn retry_config_default() {
    let r = RetryConfig::default();
    assert_eq!(r.max_retries, 3);
    assert_eq!(r.initial_backoff, Duration::from_millis(50));
}

#[test]
fn retry_config_custom() {
    let r = RetryConfig {
        max_retries: 5,
        initial_backoff: Duration::from_millis(100),
    };
    assert_eq!(r.max_retries, 5);
    assert_eq!(r.initial_backoff, Duration::from_millis(100));
}

#[test]
fn is_transient_unavailable() {
    let err = ChunkdbClientError::Unavailable("test".into());
    assert!(err.is_transient());
}

#[test]
fn is_transient_deadline_exceeded() {
    let err = ChunkdbClientError::DeadlineExceeded("test".into());
    assert!(err.is_transient());
}

#[test]
fn is_transient_unreachable() {
    let err = ChunkdbClientError::Unreachable("test".into());
    assert!(err.is_transient());
}

#[test]
fn is_not_transient_not_found() {
    let err = ChunkdbClientError::NotFound("test".into());
    assert!(!err.is_transient());
}

#[test]
fn is_not_transient_already_exists() {
    let err = ChunkdbClientError::AlreadyExists("test".into());
    assert!(!err.is_transient());
}

#[test]
fn is_not_transient_failed_precondition() {
    let err = ChunkdbClientError::FailedPrecondition("test".into());
    assert!(!err.is_transient());
}

#[test]
fn is_not_transient_aborted() {
    let err = ChunkdbClientError::Aborted("test".into());
    assert!(!err.is_transient());
}

#[test]
fn from_status_unavailable() {
    let status = Status::unavailable("server down");
    let err = crow_chunkdb_client::from_status(&status);
    assert!(matches!(err, ChunkdbClientError::Unavailable(_)));
    assert!(err.is_transient());
}

#[test]
fn from_status_not_found() {
    let status = Status::not_found("chunk not found");
    let err = crow_chunkdb_client::from_status(&status);
    assert!(matches!(err, ChunkdbClientError::NotFound(_)));
    assert!(!err.is_transient());
}

#[test]
fn from_status_already_exists() {
    let status = Status::already_exists("chunk exists");
    let err = crow_chunkdb_client::from_status(&status);
    assert!(matches!(err, ChunkdbClientError::AlreadyExists(_)));
}

#[test]
fn from_status_failed_precondition() {
    let status = Status::failed_precondition("invalid state");
    let err = crow_chunkdb_client::from_status(&status);
    assert!(matches!(err, ChunkdbClientError::FailedPrecondition(_)));
}

#[test]
fn from_status_aborted() {
    let status = Status::aborted("conflict");
    let err = crow_chunkdb_client::from_status(&status);
    assert!(matches!(err, ChunkdbClientError::Aborted(_)));
}

#[test]
fn from_status_internal() {
    let status = Status::internal("boom");
    let err = crow_chunkdb_client::from_status(&status);
    assert!(matches!(err, ChunkdbClientError::Internal(_)));
}
