// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! In-memory registry for async management operations. Long-running
//! operations (step-down, remove leader, add replica catch-up) return an
//! operation ID immediately; callers poll `GET /operations/:id` for
//! completion status.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use utoipa::ToSchema;

/// Kind of async operation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationKind {
    StepDown,
    RemoveReplica,
    AddReplica,
    StopServer,
}

impl OperationKind {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::StepDown => "step_down",
            Self::RemoveReplica => "remove_replica",
            Self::AddReplica => "add_replica",
            Self::StopServer => "stop_server",
        }
    }
}

/// Lifecycle status of an async operation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl OperationStatus {
    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Target of an async operation, for status display.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[allow(clippy::struct_field_names)]
pub(crate) struct OperationTarget {
    pub(crate) store_id: u64,
    pub(crate) group_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) replica_id: Option<u64>,
}

/// A single async operation tracked by the registry.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct Operation {
    pub(crate) id: u64,
    pub(crate) kind: OperationKind,
    pub(crate) status: OperationStatus,
    pub(crate) target: OperationTarget,
    pub(crate) started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

/// In-memory operation registry. Thread-safe via `DashMap`.
pub(crate) struct OperationRegistry {
    operations: DashMap<u64, Operation>,
    next_id: AtomicU64,
}

impl OperationRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            operations: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Create a new operation in `Pending` status and return its ID.
    /// The caller should then spawn a task that calls `update_status` to
    /// drive it through `Running` → `Completed`/`Failed`.
    #[must_use]
    pub(crate) fn create(&self, kind: OperationKind, target: OperationTarget) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        self.operations.insert(
            id,
            Operation {
                id,
                kind,
                status: OperationStatus::Pending,
                target,
                started_at_ms: now,
                completed_at_ms: None,
                error: None,
            },
        );
        id
    }

    /// Get a snapshot of an operation by ID.
    #[must_use]
    pub(crate) fn get(&self, id: u64) -> Option<Operation> {
        self.operations.get(&id).map(|r| r.clone())
    }

    /// Update an operation's status. Sets `completed_at` when transitioning
    /// to `Completed` or `Failed`. Sets `error` when `Failed`.
    pub(crate) fn update_status(&self, id: u64, status: OperationStatus, error: Option<String>) {
        if let Some(mut op) = self.operations.get_mut(&id) {
            op.status = status;
            if status == OperationStatus::Completed || status == OperationStatus::Failed {
                op.completed_at_ms = Some(now_ms());
            }
            if let Some(e) = error {
                op.error = Some(e);
            }
        }
    }

    /// Remove completed operations older than `ttl`. Called periodically by
    /// a background cleanup task.
    #[allow(dead_code)]
    pub(crate) fn cleanup_expired(&self, ttl: Duration) {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let now = now_ms();
        self.operations.retain(|_, op| {
            if op.status == OperationStatus::Completed || op.status == OperationStatus::Failed {
                if let Some(completed) = op.completed_at_ms {
                    return now.saturating_sub(completed) < ttl_ms;
                }
            }
            true
        });
    }
}

impl Default for OperationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Wrapper for shared state passed to HTTP handlers — bundles the store
/// registry and the operation registry. Implements `Deref` to
/// `KvStoreRegistry` so existing handlers that call `state.stores`,
/// `state.get_store()`, etc. work unchanged.
#[derive(Clone)]
pub struct AppState {
    pub(crate) registry: Arc<crate::store_registry::KvStoreRegistry>,
    pub(crate) operations: Arc<OperationRegistry>,
}

impl std::ops::Deref for AppState {
    type Target = crate::store_registry::KvStoreRegistry;
    fn deref(&self) -> &Self::Target {
        &self.registry
    }
}

impl AppState {
    #[must_use]
    pub fn new(registry: Arc<crate::store_registry::KvStoreRegistry>) -> Self {
        Self {
            registry,
            operations: Arc::new(OperationRegistry::new()),
        }
    }

    /// Construct with an explicit operation registry (for testing or when
    /// the registry must be shared across routers).
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn with_operations(
        registry: Arc<crate::store_registry::KvStoreRegistry>,
        operations: Arc<OperationRegistry>,
    ) -> Self {
        Self { registry, operations }
    }
}
