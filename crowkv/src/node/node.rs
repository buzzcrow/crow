//! `PxNode` — runtime container for a single CrowKV group member.
//!
//! Wraps the consensus state (`PxAcceptor`) with a configurable role
//! (leader or follower).  The acceptor is shared via `Arc<Mutex>` so it
//! can be accessed both by direct callers (tests) and by the gRPC
//! `PxService` implementation running inside the tonic server.
//!
//! Introduced in P1 M2. See `doc/plan/plan-consensus.md` §1 M2.3.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::group::group::PxGroup;
use crate::common::config::KvConfig;
use crate::node::server::GrpcTaskState;
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::error::{PxPaxosError, PxPaxosPhase, PxRetryAction};
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{
    AcceptReply, Acceptor, Ballot, Learner, LogEntry, LogEntryKind, PrepareReply,
};
use crate::rpc::connection_pool::PxPeerConnectionPool;
use crate::rpc::{AcceptRequest, AcceptedValue, PrepareRequest};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Hard-coded role for a node in P1 M2 (no election yet).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PxNodeRole {
    /// Drives Prepare / Accept rounds, tracks quorum.
    Leader,
    /// Only serves Acceptor handlers.
    Follower,
}

/// Paxos execution mode.  Set once at construction; never changed at runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PxPaxosMode {
    /// Full Phase-1 + Phase-2 per slot (classic Paxos).
    Classic,
    /// Phase-2 only; leader holds a promise for all slots (Multi-Paxos steady state).
    Leader,
}

/// Runtime container for one group member.
///
/// In P1 M2 tests construct nodes directly and interact with them over the
/// embedded gRPC servers rather than spawning the `crowkv-server` binary.
#[derive(Clone)]
pub struct PxNode {
    pub id: u64,
    pub role: PxNodeRole,
    pub acceptor: Arc<tokio::sync::Mutex<PxAcceptor>>,
    pub learner: PxLearner,
    pub(crate) server_state: Arc<Mutex<GrpcTaskState>>,
    group: Arc<Mutex<Option<PxGroup>>>,
    peer_pool: PxPeerConnectionPool,
    next_slot: Arc<AtomicU64>,
    paxos_mode: PxPaxosMode,
    pub(crate) config: KvConfig,
}

impl PxNode {
    /// Create a new node with the given id, role and Paxos mode.
    ///
    /// Default callers should pass [`PxPaxosMode::Leader`]; use [`PxPaxosMode::Classic`]
    /// when you explicitly need a full Phase-1 round per slot.
    pub fn new(id: u64, role: PxNodeRole, mode: PxPaxosMode) -> Self {
        Self::with_config(id, role, mode, KvConfig::default())
    }

    pub fn with_config(id: u64, role: PxNodeRole, mode: PxPaxosMode, config: KvConfig) -> Self {
        Self {
            id,
            role,
            acceptor: Arc::new(tokio::sync::Mutex::new(PxAcceptor::new())),
            learner: PxLearner::new(),
            server_state: Arc::new(Mutex::new(GrpcTaskState {
                handle: None,
                shutdown_tx: None,
                listen_addr: None,
            })),
            group: Arc::new(Mutex::new(None)),
            peer_pool: PxPeerConnectionPool::default(),
            next_slot: Arc::new(AtomicU64::new(1)),
            paxos_mode: mode,
            config,
        }
    }

    pub async fn kv_put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        if let Some(resp) = self.check_leader_or_redirect() {
            return resp;
        }
        self.propose_kv(
            vec![(key, Some(value))],
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }

    pub async fn kv_delete(
        &self,
        key: Vec<u8>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        if let Some(resp) = self.check_leader_or_redirect() {
            return resp;
        }
        let resp = self
            .propose_kv(
                vec![(key.clone(), None)],
                optional_u64(client_id),
                Some(seq),
                request_id,
                request_create_ms,
            )
            .await;
        // `not_found` is only meaningful for direct delete responses;
        // after Paxos the value may have already been removed.
        resp
    }

    pub async fn kv_batch_write(
        &self,
        items: Vec<crate::rpc::KvBatchItem>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        if let Some(resp) = self.check_leader_or_redirect() {
            return resp;
        }
        let payload = Self::encode_kv_batch_items(&items);
        self.propose_kv_payload(
            payload,
            optional_u64(client_id),
            Some(seq),
            request_id,
            request_create_ms,
        )
        .await
    }

    /// Convenience: is this node the leader?
    pub fn is_leader(&self) -> bool {
        self.role == PxNodeRole::Leader
    }

    pub fn set_role(&mut self, role: PxNodeRole) {
        info!(
            node_id = self.id,
            old_role = ?self.role,
            new_role = ?role,
            "node role updated"
        );
        self.role = role;
    }

    /// Return the socket address the embedded gRPC server is listening on, if
    /// it has been started.
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.server_state.lock().unwrap().listen_addr
    }

    /// Phase-1 `Prepare` handler — delegates to the in-memory acceptor.
    pub async fn on_prepare(&self, slot: u64, ballot: Ballot) -> PrepareReply {
        self.acceptor.lock().await.prepare(slot, ballot).await
    }

    /// Phase-2 `Accept` handler — delegates to the in-memory acceptor.
    pub async fn on_accept(&self, entry: LogEntry) -> AcceptReply {
        self.acceptor.lock().await.accept(entry).await
    }

    /// Read the currently accepted value at a slot (for verification).
    pub async fn accepted_at(&self, slot: u64) -> Option<LogEntry> {
        self.acceptor.lock().await.accepted_at(slot)
    }

    /// Read the currently promised ballot at a slot (for verification).
    pub async fn promised_at(&self, slot: u64) -> Option<Ballot> {
        self.acceptor.lock().await.promised_at(slot)
    }

    /// Attach group topology so KV ops can redirect to the leader.
    pub fn with_group(&mut self, group: PxGroup) {
        info!(
            node_id = self.id,
            group_id = group.group_config.group_id,
            leader_id = group.leader_id,
            members = group.group_config.members.len(),
            "node group updated"
        );
        *self.group.lock().unwrap() = Some(group);
    }

    fn check_leader_or_redirect(&self) -> Option<crate::rpc::KvResponse> {
        if self.role == PxNodeRole::Leader {
            return None;
        }
        let hint = self
            .group
            .lock()
            .unwrap()
            .as_ref()
            .and_then(PxGroup::leader_endpoint)
            .unwrap_or_default();
        warn!(
            node_id = self.id,
            role = ?self.role,
            leader_hint = hint,
            "rejecting kv request on non-leader; next step: retry request at leader_hint"
        );
        Some(crate::rpc::KvResponse {
            version: 1,
            ok: false,
            revision: 0,
            error: "not leader".to_string(),
            not_found: false,
            not_leader_hint: hint,
            request_id: 0,
            request_create_ms: 0,
        })
    }

    // ── KV operations (P1 M2 — runs through Paxos) ──────────

    /// Encode a batch of KV operations into the minimal binary wire format
    /// consumed by `PxLearner::learn`.
    fn encode_kv_payload(ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(ops.len() as u8);
        for (key, value_opt) in ops {
            buf.push(if value_opt.is_some() { 0 } else { 1 });
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            let value_len = value_opt.as_ref().map(|v| v.len()).unwrap_or(0) as u32;
            buf.extend_from_slice(&value_len.to_le_bytes());
            if let Some(value) = value_opt {
                buf.extend_from_slice(value);
            }
        }
        buf
    }

    fn encode_kv_batch_items(items: &[crate::rpc::KvBatchItem]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(items.len() as u8);
        for item in items {
            buf.push(if item.is_delete { 1 } else { 0 });
            buf.extend_from_slice(&(item.key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&item.key);
            let value_len = if item.is_delete {
                0
            } else {
                item.value.len() as u32
            };
            buf.extend_from_slice(&value_len.to_le_bytes());
            if !item.is_delete {
                buf.extend_from_slice(&item.value);
            }
        }
        buf
    }

    /// Pick the next slot, create a `LogEntry`, run the appropriate Paxos
    /// round (Classic = Phase-1 + Phase-2, Leader = Phase-2 only), and
    /// apply to every learner once a quorum is reached.
    async fn propose_kv(
        &self,
        ops: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        client_id: Option<u64>,
        seq: Option<u64>,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let payload = Self::encode_kv_payload(&ops);
        self.propose_kv_payload(payload, client_id, seq, request_id, request_create_ms)
            .await
    }

    async fn propose_kv_payload(
        &self,
        payload: Vec<u8>,
        client_id: Option<u64>,
        seq: Option<u64>,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let peer_endpoints = self.peer_endpoints();
        let total = peer_endpoints.len() + 1;
        let quorum = total / 2 + 1;
        let mut slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
        let mut last_error = String::new();

        warn!(
            node_id = self.id,
            "temporary PxNode.next_slot slot allocation is active; next step: move slot ownership to P1 M4 proposer/group"
        );
        info!(
            node_id = self.id,
            request_id,
            request_create_ms,
            client_id = ?client_id,
            seq = ?seq,
            peer_count = peer_endpoints.len(),
            total,
            quorum,
            "start kv paxos proposal"
        );

        'slot_retry: for slot_attempt in 0..self.config.max_slot_retries {
            let base_entry = self.base_entry(slot, payload.clone(), client_id, seq);
            let mut force_prepare = self.paxos_mode == PxPaxosMode::Classic;
            let mut min_round = 0u64;

            for attempt in 0..self.config.max_paxos_retries {
                let mut entry = base_entry.clone();
                let mut adopted_foreign_value = false;
                debug!(
                    node_id = self.id,
                    request_id,
                    slot,
                    slot_attempt,
                    attempt,
                    force_prepare,
                    min_round,
                    "start paxos attempt"
                );

                if force_prepare {
                    match self
                        .run_prepare_phase(
                            slot,
                            payload.as_slice(),
                            client_id,
                            seq,
                            &peer_endpoints,
                            quorum,
                            min_round,
                        )
                        .await
                    {
                        PrepareAttempt::Proceed {
                            entry: prepared_entry,
                            foreign_value,
                        } => {
                            entry = prepared_entry;
                            adopted_foreign_value = foreign_value;
                        }
                        PrepareAttempt::Retry {
                            next_min_round,
                            error,
                        } => {
                            warn!(
                                node_id = self.id,
                                request_id,
                                slot,
                                attempt,
                                next_min_round,
                                error = error.keyword(),
                                "prepare retry requested; next step: retry same slot with higher ballot"
                            );
                            last_error = error.keyword().to_string();
                            min_round = next_min_round;
                            sleep(self.retry_backoff(attempt)).await;
                            continue;
                        }
                        PrepareAttempt::Fail { error } => {
                            error!(
                                node_id = self.id,
                                request_id,
                                slot,
                                attempt,
                                error = error.keyword(),
                                "prepare failed; next step: inspect peer liveness and quorum"
                            );
                            last_error = error.keyword().to_string();
                            break;
                        }
                    }
                } else if min_round > entry.ballot.round {
                    entry.ballot.round = min_round;
                }

                match self.run_accept_phase(&entry, &peer_endpoints, quorum).await {
                    AcceptAttempt::Chosen => {
                        self.learn_chosen_entry(&entry);
                        info!(
                            node_id = self.id,
                            request_id,
                            slot = entry.slot,
                            round = entry.ballot.round,
                            leader_id = entry.ballot.leader_id,
                            "paxos entry chosen and learned locally"
                        );

                        if adopted_foreign_value || entry.payload != payload {
                            last_error = PxPaxosError::ForeignValueChosen { slot }
                                .keyword()
                                .to_string();
                            warn!(
                                node_id = self.id,
                                request_id,
                                slot,
                                error = last_error,
                                "foreign value chosen; next step: retry client value on next slot"
                            );
                            slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
                            continue 'slot_retry;
                        }

                        return crate::rpc::KvResponse {
                            version: 1,
                            ok: true,
                            revision: slot,
                            error: String::new(),
                            not_found: false,
                            not_leader_hint: String::new(),
                            request_id,
                            request_create_ms,
                        };
                    }
                    AcceptAttempt::Retry {
                        next_min_round,
                        error,
                    } => {
                        warn!(
                            node_id = self.id,
                            request_id,
                            slot,
                            attempt,
                            next_min_round,
                            error = error.keyword(),
                            "accept retry requested; next step: run prepare with higher ballot"
                        );
                        last_error = error.keyword().to_string();
                        min_round = next_min_round;
                        force_prepare = true;
                        sleep(self.retry_backoff(attempt)).await;
                        continue;
                    }
                    AcceptAttempt::Fail { error } => {
                        error!(
                            node_id = self.id,
                            request_id,
                            slot,
                            attempt,
                            error = error.keyword(),
                            "accept failed; next step: inspect peer liveness and quorum"
                        );
                        last_error = error.keyword().to_string();
                        break;
                    }
                }
            }

            warn!(
                node_id = self.id,
                request_id,
                slot,
                last_error,
                "slot proposal failed; next step: retry on next allocated slot while budget remains"
            );
            slot = self.next_slot.fetch_add(1, Ordering::SeqCst);
        }

        error!(
            node_id = self.id,
            request_id,
            last_error,
            max_paxos_retries = self.config.max_paxos_retries,
            max_slot_retries = self.config.max_slot_retries,
            "CRITICAL: paxos proposal exhausted retry budget; next step: inspect quorum, transport, and ballot conflicts"
        );
        crate::rpc::KvResponse {
            version: 1,
            ok: false,
            revision: 0,
            error: if last_error.is_empty() {
                "paxos retry exhausted".to_string()
            } else {
                format!(
                    "{} (after {} paxos retries, {} slot retries)",
                    last_error, self.config.max_paxos_retries, self.config.max_slot_retries
                )
            },
            not_found: false,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
        }
    }

    fn base_entry(
        &self,
        slot: u64,
        payload: Vec<u8>,
        client_id: Option<u64>,
        seq: Option<u64>,
    ) -> LogEntry {
        LogEntry {
            slot,
            ballot: Ballot {
                round: 0,
                leader_id: self.id,
            },
            term: 0,
            kind: LogEntryKind::Write,
            payload,
            client_id,
            seq,
        }
    }

    fn consider_accepted(adopted: &mut Option<LogEntry>, candidate: LogEntry) {
        let should_replace = adopted
            .as_ref()
            .map(|current| candidate.ballot > current.ballot)
            .unwrap_or(true);
        if should_replace {
            *adopted = Some(candidate);
        }
    }

    fn peer_endpoints(&self) -> Vec<String> {
        self.group
            .lock()
            .unwrap()
            .as_ref()
            .map(|ci| {
                ci.group_config
                    .members
                    .iter()
                    .filter(|member| member.voting && member.node_id != self.id)
                    .map(|member| member.endpoint.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn run_prepare_phase(
        &self,
        slot: u64,
        payload: &[u8],
        client_id: Option<u64>,
        seq: Option<u64>,
        peer_endpoints: &[String],
        quorum: usize,
        min_round: u64,
    ) -> PrepareAttempt {
        let mut max_round = min_round;
        if let Some(b) = self.promised_at(slot).await {
            max_round = max_round.max(b.round);
        }

        let ballot = Ballot {
            round: max_round + 1,
            leader_id: self.id,
        };
        debug!(
            node_id = self.id,
            slot,
            round = ballot.round,
            peer_count = peer_endpoints.len(),
            quorum,
            "run prepare phase"
        );
        let mut entry = self.base_entry(slot, payload.to_vec(), client_id, seq);
        entry.ballot = ballot;

        let mut promised = 0usize;
        let mut highest_rejected_round: Option<u64> = None;
        let mut adopted: Option<LogEntry> = None;

        match self.on_prepare(slot, ballot).await {
            PrepareReply::Promised { accepted, .. } => {
                promised += 1;
                if let Some(prev) = accepted {
                    Self::consider_accepted(&mut adopted, prev);
                }
            }
            PrepareReply::Rejected {
                current_promised, ..
            } => {
                highest_rejected_round = Some(current_promised.round);
            }
        }

        for endpoint in peer_endpoints {
            match self.prepare_remote(endpoint, slot, ballot).await {
                Ok(PrepareReply::Promised { accepted, .. }) => {
                    promised += 1;
                    if let Some(prev) = accepted {
                        Self::consider_accepted(&mut adopted, prev);
                    }
                }
                Ok(PrepareReply::Rejected {
                    current_promised, ..
                }) => {
                    let candidate = current_promised.round;
                    highest_rejected_round = Some(
                        highest_rejected_round
                            .map(|r| r.max(candidate))
                            .unwrap_or(candidate),
                    );
                }
                Err(error) => {
                    warn!(
                        node_id = self.id,
                        slot,
                        endpoint,
                        error = %error,
                        "prepare rpc failed; next step: verify peer endpoint and connection state"
                    );
                }
            }
        }

        if promised < quorum {
            if let Some(round) = highest_rejected_round {
                let error = PxPaxosError::PrepareRejected {
                    promised: Ballot::new(round, 0),
                };
                let next_min_round = match error.retry_action() {
                    PxRetryAction::RetrySameSlot {
                        min_round: Some(round),
                        ..
                    } => round,
                    _ => round,
                };
                return PrepareAttempt::Retry {
                    next_min_round,
                    error,
                };
            }
            return PrepareAttempt::Fail {
                error: PxPaxosError::QuorumUnavailable {
                    phase: PxPaxosPhase::Prepare,
                },
            };
        }

        let mut foreign_value = false;
        if let Some(prev) = adopted {
            foreign_value = prev.payload.as_slice() != payload;
            if foreign_value {
                warn!(
                    node_id = self.id,
                    slot,
                    adopted_round = prev.ballot.round,
                    adopted_leader_id = prev.ballot.leader_id,
                    "prepare adopted foreign value; next step: preserve Paxos safety and retry client value on another slot"
                );
            }
            entry = prev;
        }
        PrepareAttempt::Proceed {
            entry,
            foreign_value,
        }
    }

    async fn run_accept_phase(
        &self,
        entry: &LogEntry,
        peer_endpoints: &[String],
        quorum: usize,
    ) -> AcceptAttempt {
        let mut accepted = 0usize;
        let mut highest_rejected_round: Option<u64> = None;
        debug!(
            node_id = self.id,
            slot = entry.slot,
            round = entry.ballot.round,
            peer_count = peer_endpoints.len(),
            quorum,
            "run accept phase"
        );

        match self.on_accept(entry.clone()).await {
            AcceptReply::Accepted { .. } => {
                accepted += 1;
            }
            AcceptReply::Rejected {
                current_promised, ..
            } => {
                highest_rejected_round = Some(current_promised.round);
            }
        }

        for endpoint in peer_endpoints {
            match self.accept_remote(endpoint, entry).await {
                Ok(AcceptReply::Accepted { .. }) => {
                    accepted += 1;
                }
                Ok(AcceptReply::Rejected {
                    current_promised, ..
                }) => {
                    let candidate = current_promised.round;
                    highest_rejected_round = Some(
                        highest_rejected_round
                            .map(|r| r.max(candidate))
                            .unwrap_or(candidate),
                    );
                }
                Err(error) => {
                    warn!(
                        node_id = self.id,
                        slot = entry.slot,
                        endpoint,
                        error = %error,
                        "accept rpc failed; next step: verify peer endpoint and connection state"
                    );
                }
            }
        }

        if accepted >= quorum {
            return AcceptAttempt::Chosen;
        }

        if let Some(round) = highest_rejected_round {
            let error = PxPaxosError::AcceptRejected {
                promised: Ballot::new(round, 0),
            };
            let next_min_round = match error.retry_action() {
                PxRetryAction::RetrySameSlot {
                    min_round: Some(round),
                    ..
                } => round,
                _ => round + 1,
            };
            return AcceptAttempt::Retry {
                next_min_round,
                error,
            };
        }
        AcceptAttempt::Fail {
            error: PxPaxosError::QuorumUnavailable {
                phase: PxPaxosPhase::Accept,
            },
        }
    }

    fn learn_chosen_entry(&self, entry: &LogEntry) {
        self.learner.learn(entry.clone());
    }

    async fn prepare_remote(
        &self,
        endpoint: &str,
        slot: u64,
        ballot: Ballot,
    ) -> Result<PrepareReply, tonic::Status> {
        let mut client = self.peer_pool.grpc_client(endpoint).await?;
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot,
                round: ballot.round,
                leader_id: ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
            })
            .await?
            .into_inner();

        if resp.rejected {
            Ok(PrepareReply::Rejected {
                slot: resp.slot,
                current_promised: Ballot::new(resp.rejected_round, resp.rejected_leader_id),
            })
        } else {
            Ok(PrepareReply::Promised {
                slot: resp.slot,
                accepted: resp
                    .previously_accepted
                    .map(Self::accepted_value_to_log_entry),
            })
        }
    }

    async fn accept_remote(
        &self,
        endpoint: &str,
        entry: &LogEntry,
    ) -> Result<AcceptReply, tonic::Status> {
        let mut client = self.peer_pool.grpc_client(endpoint).await?;
        let resp = client
            .accept(AcceptRequest {
                version: 1,
                slot: entry.slot,
                round: entry.ballot.round,
                leader_id: entry.ballot.leader_id,
                term: entry.term,
                value: Some(AcceptedValue {
                    slot: entry.slot,
                    round: entry.ballot.round,
                    leader_id: entry.ballot.leader_id,
                    term: entry.term,
                    payload: entry.payload.clone(),
                }),
                request_id: 0,
                request_create_ms: 0,
                client_id: entry.client_id.unwrap_or(0),
                seq: entry.seq.unwrap_or(0),
            })
            .await?
            .into_inner();

        if resp.rejected {
            Ok(AcceptReply::Rejected {
                slot: resp.slot,
                current_promised: Ballot::new(resp.rejected_round, resp.rejected_leader_id),
            })
        } else {
            Ok(AcceptReply::Accepted {
                slot: resp.slot,
                ballot: Ballot::new(resp.round, resp.leader_id),
            })
        }
    }

    fn accepted_value_to_log_entry(value: AcceptedValue) -> LogEntry {
        LogEntry {
            slot: value.slot,
            ballot: Ballot::new(value.round, value.leader_id),
            term: value.term,
            kind: LogEntryKind::Write,
            payload: value.payload,
            client_id: None,
            seq: None,
        }
    }

    fn retry_backoff(&self, attempt: usize) -> Duration {
        let factor = 1u64 << attempt.min(6);
        Duration::from_millis(self.config.retry_base_backoff_ms.saturating_mul(factor))
    }
}

enum PrepareAttempt {
    Proceed {
        entry: LogEntry,
        foreign_value: bool,
    },
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}

enum AcceptAttempt {
    Chosen,
    Retry {
        next_min_round: u64,
        error: PxPaxosError,
    },
    Fail {
        error: PxPaxosError,
    },
}

fn optional_u64(value: u64) -> Option<u64> {
    if value == 0 {
        None
    } else {
        Some(value)
    }
}
