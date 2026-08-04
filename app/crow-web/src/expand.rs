// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Axum extractor for the `?recursive=` query parameter.
//!
//! Wraps `crow_console_shared::expand::RecursiveDepth` so handlers
//! can take a `Recursive` parameter directly. Malformed values surface
//! as `400 Validation` with the parse error in the body.
//!
//! Key work: `Recursive(depth)` extractor, malformed-value rejection.

use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::expand::RecursiveDepth;
use serde::Deserialize;

use crate::error::ErrorBody;

#[derive(Debug, Clone, Copy, Default)]
pub struct Recursive(pub RecursiveDepth);

#[derive(Debug, Deserialize)]
struct RecursiveQuery {
    #[serde(default)]
    recursive: Option<String>,
}

#[async_trait::async_trait]
impl<S> FromRequestParts<S> for Recursive
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<ErrorBody>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let q = Query::<RecursiveQuery>::from_request_parts(parts, state)
            .await
            .map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorBody {
                        error: format!("invalid query string: {e}"),
                    }),
                )
            })?;
        let raw = q.0.recursive.unwrap_or_default();
        match RecursiveDepth::parse(&raw) {
            Ok(d) => Ok(Recursive(d)),
            Err(e) => Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorBody {
                    error: format!("{e}"),
                }),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Request;
    use axum::routing::get;
    use axum::Router;

    async fn handler(Recursive(d): Recursive) -> String {
        format!("{d:?}")
    }

    fn app() -> Router {
        Router::new().route("/x", get(handler))
    }

    async fn call(uri: &str) -> (StatusCode, String) {
        use tower::ServiceExt;
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn extractor_parses_absent() {
        let (s, b) = call("/x").await;
        assert_eq!(s, StatusCode::OK);
        assert!(b.contains("None"));
    }

    #[tokio::test]
    async fn extractor_parses_levels_and_all() {
        let (_, b) = call("/x?recursive=3").await;
        assert!(b.contains("Levels(3)"));
        let (_, b) = call("/x?recursive=all").await;
        assert!(b.contains("All"));
    }

    #[tokio::test]
    async fn extractor_rejects_malformed_with_400() {
        let (s, body) = call("/x?recursive=nope").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(body.contains("not an integer"));
    }

    #[tokio::test]
    async fn extractor_rejects_out_of_range_with_400() {
        let (s, body) = call("/x?recursive=99").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(body.contains("exceeds maximum"));
    }
}
