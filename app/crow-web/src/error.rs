// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use axum::http::StatusCode;
use axum::Json;
use crow_console_shared::error::Error;
use serde::Serialize;

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

pub fn err_400(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::BAD_REQUEST, Json(ErrorBody { error: msg.into() }))
}

pub fn err_409(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::CONFLICT, Json(ErrorBody { error: msg.into() }))
}

pub fn err_500(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody { error: msg.into() }),
    )
}

pub fn err_502(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::BAD_GATEWAY, Json(ErrorBody { error: msg.into() }))
}

/// Map a `crow-console-core` `Error` into a JSON error response. Any
/// `UpstreamRpc` failure (the only kind these mgmt helpers return) is
/// surfaced as a `502 Bad Gateway` so the frontend can distinguish
/// "console is broken" from "the upstream server returned 4xx/5xx".
#[allow(clippy::needless_pass_by_value)]
pub fn map_err(e: Error) -> (StatusCode, Json<ErrorBody>) {
    err_502(format!("{e}"))
}

/// Map a `ConsoleConfig` mutation error to an HTTP status:
/// `Conflict` → 409, `NotFound` → 404, `Validation` → 400, anything
/// else → 500.
#[allow(clippy::needless_pass_by_value)]
pub fn map_config_err(e: Error) -> (StatusCode, Json<ErrorBody>) {
    let msg = format!("{e}");
    match e {
        Error::Conflict { .. } => (StatusCode::CONFLICT, Json(ErrorBody { error: msg })),
        Error::NotFound { .. } => (StatusCode::NOT_FOUND, Json(ErrorBody { error: msg })),
        Error::Validation { .. } => err_400(msg),
        _ => err_500(msg),
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn map_persist_err(e: Error) -> (StatusCode, Json<ErrorBody>) {
    err_500(format!("persist: {e}"))
}
