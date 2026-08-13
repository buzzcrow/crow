// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP health/readiness endpoint.
//!
//! Exposes the current `StartupPhase` (+ degraded flag) so operators
//! and orchestrators can poll readiness during recovery. The gRPC
//! service starts before recovery completes; this endpoint is the
//! observable signal that recovery is done.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::liveness::lifecycle::StartupPhase;
use crate::model::disk_group_container::DdbDiskGroupContainer;

/// Shared state for the health router.
#[derive(Clone)]
pub struct HealthState {
    pub container: Arc<DdbDiskGroupContainer>,
}

/// `GET /ready` response body.
#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    /// Current startup phase: `init` / `syncing` / `recovering` / `up`.
    pub phase: &'static str,
    /// `true` when the instance is in degraded mode (missed heartbeats).
    pub degraded: bool,
    /// `true` when `phase == "up"` and not degraded — ready to serve
    /// mutating RPCs. Convenience flag for load-balancer probes.
    pub ready: bool,
}

/// Build the health router bound to the given container.
pub fn router(container: Arc<DdbDiskGroupContainer>) -> Router {
    Router::new()
        .route("/ready", get(ready))
        .route("/health", get(ready))
        .with_state(HealthState { container })
}

/// `GET /ready` (also aliased as `/health`).
///
/// Returns `200` when `phase == "up"` (regardless of degraded — the
/// instance is alive and serving), `503` while still recovering so
/// load-balancer probes back off until recovery completes.
async fn ready(State(state): State<HealthState>) -> (StatusCode, Json<ReadyResponse>) {
    let phase = state.container.lifecycle_phase();
    let degraded = state.container.is_degraded();
    let ready = phase == StartupPhase::Up;
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(ReadyResponse {
            phase: phase.as_str(),
            degraded,
            ready,
        }),
    )
}
