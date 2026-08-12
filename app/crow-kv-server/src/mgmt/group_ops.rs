// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Group management endpoints: list, add, remove, join, flush, step-down,
//! readiness, and async operation polling.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use utoipa::ToSchema;

use crow_kv::cluster::group_election::LeaderElection;
use crow_kv::cluster::local_replica::PxLocalReplicaRole;
use crow_protocol::mgmt::{
    AddGroupInitialRole, AddGroupRequest, GroupSummary, StepDownRequest, StepDownResult,
};

use super::{err_json, ErrorResponse, RegistryArc};
use crate::operation_registry::{Operation, OperationKind, OperationStatus, OperationTarget};

/// Request body for [`join_group_via_snapshot`]: bootstrap a new/far-lagging
/// group member by pulling a snapshot from an existing member instead of
/// replaying full Paxos history.
#[derive(ToSchema, Deserialize)]
pub(super) struct JoinGroupRequest {
    replica_id: u64,
    /// gRPC endpoint (`host:port`) of an existing, already-caught-up member
    /// of this group to pull the snapshot from. Must run the **same**
    /// crow-tree backend as this store -- `KVEngine::snapshot_import`
    /// is only ever meaningful fed a stream from the same engine kind's
    /// `snapshot_export`.
    peer_endpoint: String,
}

/// Query parameter for backward-compatible synchronous mode.
#[derive(Debug, Deserialize)]
pub(super) struct SyncQuery {
    #[serde(default)]
    sync: Option<bool>,
}

impl SyncQuery {
    fn is_sync(&self) -> bool {
        self.sync.unwrap_or(false)
    }
}

/// Response for `GET /stores/:sid/groups/:gid/ready`.
#[derive(Serialize, ToSchema)]
pub(super) struct ReadinessResponse {
    pub ready: bool,
    pub leader_id: u64,
    pub term: u64,
    pub voting_replicas: u32,
    pub reachable_replicas: u32,
    pub max_applied_slot: u64,
    pub min_applied_slot: u64,
    pub lag: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response for `GET /operations/:id`.
#[derive(Serialize, ToSchema)]
pub(super) struct OperationResponse {
    pub id: u64,
    pub kind: String,
    pub status: String,
    pub target: OperationTarget,
    pub started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl From<Operation> for OperationResponse {
    fn from(op: Operation) -> Self {
        Self {
            id: op.id,
            kind: op.kind.as_str().to_string(),
            status: op.status.as_str().to_string(),
            target: op.target,
            started_at_ms: op.started_at_ms,
            completed_at_ms: op.completed_at_ms,
            error: op.error,
        }
    }
}

/// Response for async operations that return an operation ID.
#[derive(Serialize, ToSchema)]
pub(super) struct AsyncOperationResponse {
    pub operation_id: u64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(super) struct FlushResult {
    store_id: u64,
    group_id: u64,
    accepted: bool,
}

/// Spawn a background task that polls group readiness until a new leader
/// appears, then marks the operation as completed or failed.
fn spawn_leader_wait(
    state: RegistryArc,
    operation_id: u64,
    store_id: u64,
    group_id: u64,
    timeout: std::time::Duration,
) {
    state
        .operations
        .update_status(operation_id, OperationStatus::Running, None);

    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                state.operations.update_status(
                    operation_id,
                    OperationStatus::Failed,
                    Some("timed out waiting for new leader".to_string()),
                );
                return;
            }

            let Some(store) = state.get_store(store_id) else {
                state.operations.update_status(
                    operation_id,
                    OperationStatus::Failed,
                    Some("store disappeared during operation".to_string()),
                );
                return;
            };
            let Some(group) = store.get_group(group_id) else {
                state.operations.update_status(
                    operation_id,
                    OperationStatus::Failed,
                    Some("group disappeared during operation".to_string()),
                );
                return;
            };

            if group.leader_id() != 0 {
                state
                    .operations
                    .update_status(operation_id, OperationStatus::Completed, None);
                info!(
                    store_id,
                    group_id,
                    operation_id,
                    new_leader = group.leader_id(),
                    "async leader-wait operation completed"
                );
                return;
            }

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    });
}

#[utoipa::path(
        get,
        path = "/stores/{sid}/groups",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        responses(
            (status = 200, description = "Groups in the store", body = Vec<GroupSummary>),
            (status = 404, description = "Store not found", body = ErrorResponse)
        )
    )]
pub(super) async fn list_groups(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
) -> Result<Json<Vec<GroupSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    let groups: Vec<GroupSummary> = store
        .group_summaries()
        .into_iter()
        .map(
            |(group_id, local_replica_id, leader_id, remote_count)| GroupSummary {
                group_id,
                local_replica_id,
                leader_id,
                remote_count,
            },
        )
        .collect();

    Ok(Json(groups))
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups",
        tag = "management",
        params(("sid" = u64, Path, description = "Store id")),
        request_body = AddGroupRequest,
        responses(
            (status = 201, description = "Group created"),
            (status = 404, description = "Store not found", body = ErrorResponse),
            (status = 409, description = "Group already exists", body = ErrorResponse)
        )
    )]
pub(super) async fn add_group(
    State(state): State<RegistryArc>,
    Path(sid): Path<u64>,
    Json(req): Json<AddGroupRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    if store.get_group(req.group_id).is_some() {
        return Err(err_json(
            StatusCode::CONFLICT,
            format!("group {} already exists in store {sid}", req.group_id),
        ));
    }

    debug!(
        store_id = sid,
        group_id = req.group_id,
        replica_id = req.replica_id,
        "creating PxGroup with local replica via management API"
    );
    let initial_role = match req.initial_role.unwrap_or(AddGroupInitialRole::Leader) {
        AddGroupInitialRole::Leader => PxLocalReplicaRole::Leader,
        AddGroupInitialRole::Follower => PxLocalReplicaRole::Follower,
    };
    let group = crate::startup::create_group_with_wal(
        sid,
        req.group_id,
        req.replica_id,
        initial_role,
        &state.config,
        state.wal_backend.clone(),
        state.crowtree_backend,
    )
    .await
    .map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "failed to create WAL-backed group {} in store {sid}: {e}",
                req.group_id
            ),
        )
    })?;
    let group = group;
    // Defer the election driver when the caller is about to wire remotes
    // (multi-replica restore / creation). Avoids a `quorum == 1` self-election
    // running `bulk_phase1` / `repair_once` against only itself, which can
    // erase committed data. The driver is started by
    // the subsequent `add_remote_replicas` rebuild.
    let start_election = req.start_election.unwrap_or(true);
    if start_election && group.quorum() == 1 {
        let current_term = group.local_replica().current_term_snapshot();
        if current_term == 0 {
            group.local_replica().become_candidate(1);
            group.local_replica().persist_current_vote().await;
            group.local_replica().become_leader();
        }
        group.stamp_proposing_term(group.local_replica().current_term_snapshot());
    }
    if start_election {
        store.add_group(group);
    } else if group.quorum() > 1 {
        // Remotes were restored from the persisted config file, so the
        // group already has the correct quorum. Start the election driver
        // now — the quorum=1 deferral does not apply.
        info!(
            store_id = sid,
            group_id = req.group_id,
            quorum = group.quorum(),
            "starting election driver for group with persisted config remotes"
        );
        store.add_group(group);
    } else {
        store.add_group_without_election(group);
    }

    info!(
        store_id = sid,
        group_id = req.group_id,
        replica_id = req.replica_id,
        start_election,
        "PxGroup added via management API"
    );
    Ok(StatusCode::CREATED)
}

/// `POST /stores/{sid}/groups/{gid}/join` — new-member snapshot join
///: create this
/// store's local replica for `gid` and bootstrap its state by pulling a
/// snapshot from `peer_endpoint` instead of replaying full Paxos history.
///
/// The group is added **without** wiring any remotes and **without**
/// starting its election driver (mirrors `add_group`'s `quorum == 1`
/// self-election guard) — this replica is not yet part of the group's
/// topology on either side. The caller's follow-up steps:
/// 1. `POST.../remotes` on **this** store to wire the group's existing
///    members as this replica's remotes (`voting: true`, since they're
///    already established).
/// 2. `POST.../remotes` on **each existing member** to add this replica
///    as their remote with `voting: false`, so it starts receiving
///    heartbeat/repair catch-up for the WAL tail without affecting quorum.
/// 3. Once caught up, re-add this replica everywhere with `voting: true`
///    to promote it to a full voting member.
#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/join",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = JoinGroupRequest,
        responses(
            (status = 201, description = "Group created and snapshot import succeeded"),
            (status = 404, description = "Store not found", body = ErrorResponse),
            (status = 409, description = "Group already exists", body = ErrorResponse),
            (status = 502, description = "Snapshot pull from peer_endpoint failed", body = ErrorResponse)
        )
    )]
pub(super) async fn join_group_via_snapshot(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Json(req): Json<JoinGroupRequest>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    if store.get_group(gid).is_some() {
        return Err(err_json(
            StatusCode::CONFLICT,
            format!("group {gid} already exists in store {sid}"),
        ));
    }

    info!(
        store_id = sid,
        group_id = gid,
        replica_id = req.replica_id,
        peer_endpoint = %req.peer_endpoint,
        "joining PxGroup via snapshot pull"
    );
    let group = crate::startup::create_group_with_wal(
        sid,
        gid,
        req.replica_id,
        PxLocalReplicaRole::Follower,
        &state.config,
        state.wal_backend.clone(),
        state.crowtree_backend,
    )
    .await
    .map_err(|e| {
        err_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create WAL-backed group {gid} in store {sid}: {e}"),
        )
    })?;

    let at_slot = group.join_via_snapshot(&req.peer_endpoint).await.map_err(|e| {
        err_json(
            StatusCode::BAD_GATEWAY,
            format!("snapshot join against {} failed: {e}", req.peer_endpoint),
        )
    })?;
    // The frontier moved from 0 to `at_slot` (or further, if a concurrent
    // catch-up already advanced it) after `create_group_with_wal` computed
    // `next_slot` from a still-empty replica; recompute so a future
    // proposal (once this replica becomes leader-eligible) doesn't clash
    // with an already-applied slot.
    let next_slot = group
        .local_replica()
        .highest_seen_slot()
        .max(group.local_replica().last_chosen_slot())
        .max(group.local_replica().contiguous_applied())
        .saturating_add(1)
        .max(1);
    group.set_next_slot(next_slot);

    // No remotes wired yet on either side -- same self-election hazard
    // `add_group` guards against, so defer the driver (see that handler's
    // comment for the full reasoning).
    store.add_group_without_election(group);

    info!(
        store_id = sid,
        group_id = gid,
        replica_id = req.replica_id,
        at_slot,
        "PxGroup joined via snapshot; remotes must be wired separately"
    );
    Ok(StatusCode::CREATED)
}

#[utoipa::path(
        delete,
        path = "/stores/{sid}/groups/{gid}",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        responses(
            (status = 200, description = "Group removed"),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
pub(super) async fn remove_group(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;

    info!(
        store_id = sid,
        group_id = gid,
        "removing PxGroup via management API"
    );
    if !store.remove_group(gid) {
        return Err(err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        ));
    }

    info!(
        store_id = sid,
        group_id = gid,
        "PxGroup removed via management API"
    );
    Ok(StatusCode::OK)
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/step-down",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        request_body = StepDownRequest,
        responses(
            (status = 200, description = "Step-down attempted; `accepted` is false if this node was not leader", body = StepDownResult),
            (status = 404, description = "Store or group not found", body = ErrorResponse)
        )
    )]
pub(super) async fn step_down(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
    Query(sync): Query<SyncQuery>,
    Json(body): Json<StepDownRequest>,
) -> Result<axum::response::Response, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let reply = group.step_down_if_leader(&body.reason);
    info!(
        store_id = sid,
        group_id = gid,
        accepted = reply.accepted,
        sync = sync.is_sync(),
        "step-down requested via management API"
    );

    if sync.is_sync() || !reply.accepted {
        // Synchronous mode, or step-down was not accepted (no async needed)
        return Ok(Json(StepDownResult {
            accepted: reply.accepted,
            current_term: reply.current_term,
            current_leader_id: reply.current_leader_id,
        })
        .into_response());
    }

    // Async mode: create operation, spawn leader-wait task, return 202
    let op_id = state.operations.create(
        OperationKind::StepDown,
        OperationTarget {
            store_id: sid,
            group_id: gid,
            replica_id: None,
        },
    );
    spawn_leader_wait(state, op_id, sid, gid, std::time::Duration::from_secs(10));

    Ok((
        StatusCode::ACCEPTED,
        Json(AsyncOperationResponse {
            operation_id: op_id,
            status: "pending".to_string(),
        }),
    )
        .into_response())
}

#[utoipa::path(
    get,
    path = "/stores/{sid}/groups/{gid}/ready",
    tag = "management",
    params(
        ("sid" = u64, Path, description = "Store id"),
        ("gid" = u64, Path, description = "Group id")
    ),
    responses(
        (status = 200, description = "Group is ready", body = ReadinessResponse),
        (status = 503, description = "Group is not ready", body = ReadinessResponse),
        (status = 404, description = "Store or group not found", body = ErrorResponse)
    )
)]
pub(super) async fn group_readiness(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<(StatusCode, Json<ReadinessResponse>), (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    let status = group.status();
    let leader_id = status.leader_id;
    let local_applied = group.local_replica().contiguous_applied();

    let mut voting_count = 0u32;
    let mut reachable_count = 0u32;
    let max_applied = local_applied;
    let min_applied = local_applied;

    if group.local_replica().voting {
        voting_count += 1;
        if status.local_replica.status != crow_kv::cluster::status::StatusLevel::Unhealthy {
            reachable_count += 1;
        }
    }

    for remote in &status.remotes {
        if remote.voting {
            voting_count += 1;
            if remote.status != crow_kv::cluster::status::StatusLevel::Unhealthy {
                reachable_count += 1;
            }
        }
    }

    let lag = 0u64;

    let ready = leader_id != 0 && reachable_count > voting_count / 2;
    let reason = if leader_id == 0 {
        Some("no leader elected".to_string())
    } else if reachable_count <= voting_count / 2 {
        Some(format!("quorum not reachable: {reachable_count}/{voting_count}"))
    } else {
        None
    };

    let resp = ReadinessResponse {
        ready,
        leader_id,
        term: group.local_replica().current_term_snapshot(),
        voting_replicas: voting_count,
        reachable_replicas: reachable_count,
        max_applied_slot: max_applied,
        min_applied_slot: min_applied,
        lag,
        reason,
    };

    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((code, Json(resp)))
}

#[utoipa::path(
    get,
    path = "/operations/{id}",
    tag = "management",
    params(
        ("id" = u64, Path, description = "Operation id")
    ),
    responses(
        (status = 200, description = "Operation status", body = OperationResponse),
        (status = 404, description = "Operation not found", body = ErrorResponse)
    )
)]
pub(super) async fn get_operation(
    State(state): State<RegistryArc>,
    Path(id): Path<u64>,
) -> Result<Json<OperationResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.operations.get(id) {
        Some(op) => Ok(Json(op.into())),
        None => Err(err_json(
            StatusCode::NOT_FOUND,
            format!("operation {id} not found"),
        )),
    }
}

#[utoipa::path(
        post,
        path = "/stores/{sid}/groups/{gid}/flush",
        tag = "management",
        params(
            ("sid" = u64, Path, description = "Store id"),
            ("gid" = u64, Path, description = "Group id")
        ),
        responses(
            (status = 200, description = "Local replica's L0 memtable drained into L1", body = FlushResult),
            (status = 404, description = "Store or group not found on this node", body = ErrorResponse)
        )
    )]
pub(super) async fn flush_group(
    State(state): State<RegistryArc>,
    Path((sid, gid)): Path<(u64, u64)>,
) -> Result<Json<FlushResult>, (StatusCode, Json<ErrorResponse>)> {
    let store = state
        .get_store(sid)
        .ok_or_else(|| err_json(StatusCode::NOT_FOUND, format!("store {sid} not found")))?;
    let group = store.get_group(gid).ok_or_else(|| {
        err_json(
            StatusCode::NOT_FOUND,
            format!("group {gid} not found in store {sid}"),
        )
    })?;

    // `KVEngine::flush` forces a freeze + drain of every memtable up to
    // `contiguous_slot` into L1 (in-memory only; never touches the page
    // store). Cheap when L0 is empty. Used by the bench's
    // `--flush-after-prepopulate` flag to produce a clean L1-only scan
    // baseline; also useful as an admin drain.
    group.local_replica().learner.engine().flush();
    info!(
        store_id = sid,
        group_id = gid,
        "engine flush requested via management API"
    );
    Ok(Json(FlushResult {
        store_id: sid,
        group_id: gid,
        accepted: true,
    }))
}
