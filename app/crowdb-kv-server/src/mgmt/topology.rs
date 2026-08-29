// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology export and metrics endpoints.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use utoipa::ToSchema;

use crowdb_kv::cluster::status::StoreStatus;
use crowdb_protocol::mgmt::{MetricField, MetricPoint, MetricsResponse, TopologyResponse};

use super::RegistryArc;

/// `GET /topology` (alias `/top`) — full hierarchy with per-remote RPC
/// metrics and cheap kv-store stats.
#[utoipa::path(
        get,
        path = "/topology",
        tag = "management",
        responses((status = 200, description = "Cluster topology status", body = TopologyResponse))
    )]
pub(super) async fn export_topology(State(state): State<RegistryArc>) -> impl IntoResponse {
    let stores: Vec<StoreStatus> = state.stores.iter().map(|entry| entry.value().status()).collect();
    let body = serde_json::to_string_pretty(&TopologyResponse { stores }).unwrap();
    ([("content-type", "application/json")], body)
}

/// Query params for `GET /metrics`.
#[derive(Deserialize, ToSchema)]
pub(crate) struct MetricsQuery {
    /// Metric name prefix filter (e.g. `s.1.g.2.`). Default empty = all.
    #[serde(default)]
    prefix: String,
}

/// `GET /metrics` — structured snapshot of all registry metrics matching
/// the `prefix` query param. Does not reset window state. Intended for
/// the GUI Inspector and script/scrape consumers.
#[utoipa::path(
        get,
        path = "/metrics",
        tag = "management",
        params(("prefix" = Option<String>, Query, description = "Metric name prefix filter")),
        responses((status = 200, description = "Metric snapshot", body = MetricsResponse))
    )]
pub(super) async fn metrics(
    State(state): State<RegistryArc>,
    Query(q): Query<MetricsQuery>,
) -> impl IntoResponse {
    let timestamp = crowdb_kv::metrics::iso8601_now();
    let window_secs = 5.0; // approximate — snapshot path does not track elapsed
    let metrics: Vec<MetricPoint> = state
        .metrics_registry
        .as_ref()
        .map(|reg| {
            let reg = reg.lock().unwrap();
            reg.snapshot_struct(&q.prefix, window_secs)
                .iter()
                .map(metric_point_to_dto)
                .collect()
        })
        .unwrap_or_default();
    let body = serde_json::to_string_pretty(&MetricsResponse {
        window_secs,
        timestamp,
        metrics,
    })
    .unwrap();
    ([("content-type", "application/json")], body)
}

#[allow(clippy::cast_precision_loss)]
fn metric_point_to_dto(p: &crowdb_kv::metrics::MetricPoint) -> MetricPoint {
    use crowdb_kv::metrics::MetricPoint as KvMetricPoint;
    let kind = p.kind().to_string();
    let fields = match p {
        KvMetricPoint::Counter {
            count, tps, total, ..
        } => vec![("count", *count as f64), ("tps", *tps), ("total", *total as f64)],
        KvMetricPoint::Gauge { value, .. } => vec![("value", *value as f64)],
        KvMetricPoint::Bandwidth {
            count,
            avg_size,
            rate,
            total_bytes,
            ..
        } => vec![
            ("count", *count as f64),
            ("avg_size", *avg_size as f64),
            ("rate", *rate as f64),
            ("total_bytes", *total_bytes as f64),
        ],
        KvMetricPoint::Histogram {
            count,
            avg_ns,
            p50_ns,
            p99_ns,
            max_ns,
            total,
            ..
        } => vec![
            ("count", *count as f64),
            ("avg_ns", *avg_ns as f64),
            ("p50_ns", *p50_ns as f64),
            ("p99_ns", *p99_ns as f64),
            ("max_ns", *max_ns as f64),
            ("total", *total as f64),
        ],
        KvMetricPoint::Summary {
            count,
            avg_ns,
            max_ns,
            total,
            ..
        } => vec![
            ("count", *count as f64),
            ("avg_ns", *avg_ns as f64),
            ("max_ns", *max_ns as f64),
            ("total", *total as f64),
        ],
    };
    MetricPoint {
        name: match p {
            KvMetricPoint::Counter { name, .. }
            | KvMetricPoint::Gauge { name, .. }
            | KvMetricPoint::Bandwidth { name, .. }
            | KvMetricPoint::Histogram { name, .. }
            | KvMetricPoint::Summary { name, .. } => name.clone(),
        },
        kind,
        fields: fields
            .into_iter()
            .map(|(key, value)| MetricField {
                key: key.to_string(),
                value,
            })
            .collect(),
    }
}
