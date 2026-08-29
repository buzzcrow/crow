// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]

//! Hand-written Rust types replacing the prost-generated `crowdb_kv.rpc`
//! consensus types. API-compatible with the former
//! proto-generated structs.

use bytes::Bytes;

// ── Classic Paxos prepare/accept ────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct PrepareRequest {
    pub version: u32,
    pub slot: u64,
    pub round: u64,
    pub leader_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    pub group_id: u64,
    /// P1 M3: leader's current term for the two-fence rule.
    pub term: u64,
    /// P5 M2: leader's current membership_epoch for the exact-match
    /// membership-epoch fence. Absent/0 from an old binary is treated as
    /// epoch 0, matching a freshly-created group that has never had a
    /// membership mutation.
    pub membership_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct AcceptedValue {
    pub slot: u64,
    pub round: u64,
    pub leader_id: u64,
    pub term: u64,
    pub payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct PromiseResponse {
    pub version: u32,
    pub slot: u64,
    pub round: u64,
    pub leader_id: u64,
    pub previously_accepted: Option<AcceptedValue>,
    pub rejected: bool,
    pub rejected_round: u64,
    pub rejected_leader_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// P1 M3: responder's current term so the proposer can detect a stale
    /// leader (term < responder.current_term -> step down with TermStale).
    pub term: u64,
    /// P1 M3: responder echoes back TermStale when the prepare's term is lower
    /// than its own. Carries `term` as the new term to adopt.
    pub term_stale: bool,
    /// P5 M2: responder's own membership_epoch, always populated (not just
    /// on mismatch) so the proposer can log/compare even when it matches.
    pub membership_epoch: u64,
    /// P5 M2: set when the proposer's membership_epoch did not exactly
    /// match `membership_epoch` above. Distinct from `rejected`/`term_stale`
    /// -- the proposer must not bump its ballot in response, only refresh
    /// its membership view.
    pub epoch_mismatch: bool,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct AcceptRequest {
    pub version: u32,
    pub slot: u64,
    pub round: u64,
    pub leader_id: u64,
    pub term: u64,
    pub value: Option<AcceptedValue>,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// Legacy single dedup tag (fields 9/10). Kept populated with the first
    /// coalesced tag (or 0) for backward-compat with older followers during a
    /// rolling upgrade. New replicas prefer `dedup_tags` (field 13), which
    /// carries one tag per coalesced client op.
    pub client_id: u64,
    pub seq: u64,
    pub group_id: u64,
    /// P5 M2: see PrepareRequest.membership_epoch.
    pub membership_epoch: u64,
    /// R36: one (client_id, seq) dedup tag per coalesced client op, all
    /// mapping to this slot. Empty for repair/election entries and for
    /// older leaders (fall back to legacy client_id/seq).
    pub dedup_tags: Vec<DedupTag>,
}

/// R36: a single (client_id, seq) dedup mapping for a chosen slot.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct DedupTag {
    pub client_id: u64,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct AcceptedResponse {
    pub version: u32,
    pub slot: u64,
    pub round: u64,
    pub leader_id: u64,
    pub rejected: bool,
    pub rejected_round: u64,
    pub rejected_leader_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// P1 M3: responder's current term.
    pub term: u64,
    /// P1 M3: term-stale rejection (see PromiseResponse.term_stale).
    pub term_stale: bool,
    /// P5 M2: see PromiseResponse.membership_epoch.
    pub membership_epoch: u64,
    /// P5 M2: see PromiseResponse.epoch_mismatch.
    pub epoch_mismatch: bool,
}

// ── P1 M3: leader election ──────────────────────────────────────

/// PreVote round (Raft-style). PreCandidate sends proposed_term =
/// current_term + 1 without bumping its own term; peers reply granted=true if
/// they would vote for it in proposed_term (log-up-to-date check + vote
/// lockout window). This avoids disruption from partitioned nodes whose term
/// would otherwise leap past the cluster's.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct PreVoteRequest {
    pub version: u32,
    pub group_id: u64,
    /// proposed term (current_term + 1)
    pub term: u64,
    pub candidate_id: u64,
    pub accepted_log_tip_slot: u64,
    pub accepted_log_tip_term: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct PreVoteResponse {
    pub version: u32,
    pub group_id: u64,
    /// responder's current_term (for stale detect)
    pub term: u64,
    pub granted: bool,
    /// Learner frontier piggy-back for the candidate's bulk-Phase-1 ceiling.
    pub contiguous_chosen: u64,
    pub last_chosen_term: u64,
    pub highest_seen_slot: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
}

/// Real RequestVote round (same shape as PreVote; consumes the vote slot).
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct RequestVoteRequest {
    pub version: u32,
    pub group_id: u64,
    /// term that won the prevote quorum
    pub term: u64,
    pub candidate_id: u64,
    pub accepted_log_tip_slot: u64,
    pub accepted_log_tip_term: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct RequestVoteResponse {
    pub version: u32,
    pub group_id: u64,
    pub term: u64,
    pub granted: bool,
    pub contiguous_chosen: u64,
    pub last_chosen_term: u64,
    pub highest_seen_slot: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
}

/// Heartbeat (also extends the follower's election deadline and vote-lockout
/// window). `lease_grant_until_ms_mono` and `t_send_ms_mono` are the leader's
/// monotonic-clock timestamps; followers do not treat them as wall-clock.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct HeartbeatRequest {
    pub version: u32,
    pub group_id: u64,
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_slot: u64,
    pub prev_log_term: u64,
    pub committed_safe_slot: u64,
    pub lease_grant_until_ms_mono: u64,
    pub t_send_ms_mono: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct HeartbeatResponse {
    pub version: u32,
    pub group_id: u64,
    pub term: u64,
    pub success: bool,
    pub contiguous_chosen: u64,
    pub last_chosen_term: u64,
    pub contiguous_applied: u64,
    pub highest_seen_slot: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// Highest slot durably captured in this follower's own last engine
    /// snapshot (`snapshot_slot`; mirrors `WalEngine::snapshot_slot()`).
    /// The leader aggregates this across voting peers to compute the group's
    /// real "durable on leader + >=1 peer" watermark, instead of
    /// approximating it with `contiguous_applied`/`group_safe_slot`.
    pub durable_snapshot_slot: u64,
}

/// Admin step-down primitive (decision §7.1: strict fence).
///
/// Accept iff is_leader && self.id == target_leader_id && term == current_term.
/// On reject, response echoes current_term and current_leader_id so the admin
/// client can reissue against the right replica.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct StepDownRequest {
    pub version: u32,
    pub group_id: u64,
    pub term: u64,
    pub target_leader_id: u64,
    pub reason: String,
    pub request_id: u64,
    pub request_create_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct StepDownResponse {
    pub version: u32,
    pub group_id: u64,
    pub accepted: bool,
    pub current_term: u64,
    pub current_leader_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
}

// ── Learner stream ──────────────────────────────────────────────

/// Per-peer bidi LearnerStream. Multiplexes Accept + Heartbeat + Chosen
/// notifications on one stream per (group_id, peer_id) pair so heartbeats
/// cannot reorder ahead of an Accept they logically follow. Prepare /
/// RequestVote / PreVote / StepDown remain unary RPCs.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct ChosenNotification {
    pub version: u32,
    pub group_id: u64,
    pub slot: u64,
    pub term: u64,
    pub leader_id: u64,
    pub request_id: u64,
    pub request_create_ms: u64,
    /// R65: ballot round of the chosen value. The full chosen ballot is
    /// (ballot_round, leader_id). The follower compares its accepted ballot
    /// against this to detect stale values before applying.
    pub ballot_round: u64,
}

/// R63: fire-and-forget batch chosen notice covering a slot range. Carries
/// only `(start_slot, end_slot, term, leader_id)` — no per-slot payload. The
/// follower checks its local acceptor for each slot and advances the chosen
/// frontier for present ones; missing slots remain gaps for the full-accept
/// catch-up to fill.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct BatchChosenNotification {
    pub version: u32,
    pub group_id: u64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub term: u64,
    pub leader_id: u64,
    /// R65: ballot round of the chosen values in this range. All slots in
    /// the batch share the same ballot (leader's current ballot).
    pub ballot_round: u64,
}

/// R65: follower-driven catch-up request. The follower sends this for a
/// slot it is missing or has a stale value at a lower ballot. The leader
/// replies with the chosen value + ballot, or runs classic Paxos to resolve
/// the slot if the leader itself doesn't have it.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct FetchGapRequest {
    pub version: u32,
    pub group_id: u64,
    pub slot: u64,
    pub term: u64,
    pub leader_id: u64,
}

/// R65: leader's reply to a FetchGap request. Carries the chosen value at
/// the chosen ballot so the follower can overwrite any stale lower-ballot
/// value and apply.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FetchGapResponse {
    pub version: u32,
    pub group_id: u64,
    pub slot: u64,
    pub term: u64,
    pub ballot_round: u64,
    pub leader_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct LearnerStreamRequest {
    pub frame: Option<learner_stream_request::Frame>,
}

/// Nested oneof module matching prost's codegen pattern.
pub mod learner_stream_request {
    #[derive(Clone, PartialEq, Debug)]
    pub enum Frame {
        Accept(super::AcceptRequest),
        Heartbeat(super::HeartbeatRequest),
        Chosen(super::ChosenNotification),
        BatchChosen(super::BatchChosenNotification),
        FetchGap(super::FetchGapRequest),
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct LearnerStreamResponse {
    pub frame: Option<learner_stream_response::Frame>,
}

/// Nested oneof module matching prost's codegen pattern.
pub mod learner_stream_response {
    #[derive(Clone, PartialEq, Debug)]
    pub enum Frame {
        Accepted(super::AcceptedResponse),
        Heartbeat(super::HeartbeatResponse),
        FetchGapReply(super::FetchGapResponse),
    }
}

// ── #20 New-member snapshot install ─────────────────────────────

/// `SnapshotService::StreamSnapshot` request — identifies the group whose
/// snapshot the joining replica wants to pull.
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct SnapshotRequest {
    pub group_id: u64,
}

/// One item of the streamed snapshot transfer. `KVEngine::snapshot_export`'s
/// byte stream is chunked into `data` frames of bounded size; the exported
/// `at_slot` itself is embedded in that byte stream and returned by
/// `KVEngine::snapshot_import`, so it is not repeated here. The **first**
/// item of every response stream is always a `header`, carrying
/// `term_at_slot` -- needed (alongside `at_slot`) to seed the joining
/// replica's learner frontier -- since it cannot be recovered from the
/// exported bytes alone.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SnapshotStreamItem {
    pub payload: Option<snapshot_stream_item::Payload>,
}

/// Nested oneof module matching prost's codegen pattern.
pub mod snapshot_stream_item {
    #[derive(Clone, PartialEq, Debug)]
    pub enum Payload {
        Header(super::SnapshotHeader),
        Data(Vec<u8>),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct SnapshotHeader {
    /// Term of the log entry chosen at the snapshot's own `at_slot`, read from
    /// the exporting replica's own acceptor (`accepted_at(at_slot)`).
    pub term_at_slot: u64,
    /// P5 M2: the exporting replica's current membership_epoch, so a
    /// freshly-joining replica seeds its own epoch fence from this instead
    /// of starting at 0 -- otherwise it could never receive a Prepare/Accept
    /// (even as a non-voting catch-up learner) from a group that has ever
    /// had a membership change, since every such request is stamped with
    /// the leader's *current* (non-zero) epoch.
    pub membership_epoch: u64,
}
