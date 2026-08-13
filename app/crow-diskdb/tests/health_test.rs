// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `/ready` (and `/health` alias) endpoint tests.

use std::sync::Arc;

use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crow_diskdb::health;
use crow_diskdb::liveness::lifecycle::StartupPhase;
use crow_diskdb::model::disk_group_container::DdbDiskGroupContainer;

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.expect("body");
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test]
async fn ready_returns_503_when_init() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"init\""), "body: {body}");
    assert!(body.contains("\"ready\":false"), "body: {body}");
}

#[tokio::test]
async fn ready_returns_503_when_recovering() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Recovering);
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"recovering\""), "body: {body}");
}

#[tokio::test]
async fn ready_returns_200_when_up() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"up\""), "body: {body}");
    assert!(body.contains("\"ready\":true"), "body: {body}");
    assert!(body.contains("\"degraded\":false"), "body: {body}");
}

#[tokio::test]
async fn health_alias_returns_same_as_ready() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("\"phase\":\"up\""), "body: {body}");
}

#[tokio::test]
async fn ready_reports_degraded_flag() {
    let container = Arc::new(DdbDiskGroupContainer::new(1));
    container.set_lifecycle_phase(StartupPhase::Up);
    container.enter_degraded_mode();
    let app = health::router(container);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Still 200 (phase is Up — instance is alive), but degraded=true.
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(body.contains("\"degraded\":true"), "body: {body}");
    assert!(body.contains("\"ready\":true"), "body: {body}");
}
