// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Axum middleware that propagates the `x-crowdb-kv-corr-id` header.
//!
//! For every inbound request:
//! 1. Read `x-crowdb-kv-corr-id` from the request, or generate a new one
//!    via `shared::corr_id::new()` if absent.
//! 2. Open a `shared::corr_id::scope` so any outbound client call made
//!    inside the handler attaches the same id to its
//!    `x-crowdb-kv-corr-id` header (see `ConsoleClient` /
//!    `ServerClient` plumbing).
//! 3. Add the id to the response headers so the SPA / CLI can echo it
//!    in user-facing error messages and operators can grep one log
//!    line out of many.
//!
//! Inbound ids are taken verbatim — the SPA / CLI generate them with
//! the same `shared::corr_id::new()` helper, so cross-process correlation
//! works without any negotiation.

use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use crowdb_console_shared::corr_id;

/// `axum::middleware::from_fn(corr_id_layer)` handler. See module docs.
pub async fn corr_id_layer(req: Request, next: Next) -> Response {
    let header_name = HeaderName::from_static(corr_id::HEADER);
    let cid = req
        .headers()
        .get(&header_name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map_or_else(corr_id::generate, ToString::to_string);

    let cid_for_header = cid.clone();
    let mut resp = corr_id::scope(cid, next.run(req)).await;

    if let Ok(value) = HeaderValue::from_str(&cid_for_header) {
        resp.headers_mut().insert(header_name, value);
    }
    resp
}
