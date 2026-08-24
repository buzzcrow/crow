// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::must_use_candidate)]

//! Zero-copy `FB<Type>Ref` wrappers for KV consensus response types
//! (design-crow-rpc.md §6, R32).
//!
//! Each wrapper holds a `&[u8]` reference to the control buffer,
//! parses the root on construction, and exposes typed accessor methods
//! that read through the root pointer — no per-field copy, no owned
//! intermediate struct.

use crate::kv_consensus_fb::{
    FBAcceptedResponse, FBFetchGapResponse, FBHeartbeatResponse, FBKvRetCode, FBPreVoteResponse,
    FBPromiseResponse, FBRequestVoteResponse, FBSnapshotResponse, FBStepDownResponse,
};

// `parse_root` is hoisted to the parent `fb_wrappers` module (R117)
// so both `kv_consensus` and `kv_client` reuse it without duplication.
use super::parse_root;

// ── FBPromiseResponseRef ─────────────────────────────────────────

/// Zero-copy view over an `FBPromiseResponse` control buffer.
pub struct FBPromiseResponseRef<'a> {
    root: Option<FBPromiseResponse<'a>>,
}

impl<'a> FBPromiseResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBPromiseResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn slot(&self) -> u64 {
        self.root.map_or(0, |r| r.slot())
    }
    pub fn round(&self) -> u64 {
        self.root.map_or(0, |r| r.round())
    }
    pub fn leader_id(&self) -> u64 {
        self.root.map_or(0, |r| r.leader_id())
    }
    pub fn rejected(&self) -> bool {
        self.root.is_some_and(|r| r.rejected())
    }
    pub fn rejected_round(&self) -> u64 {
        self.root.map_or(0, |r| r.rejected_round())
    }
    pub fn rejected_leader_id(&self) -> u64 {
        self.root.map_or(0, |r| r.rejected_leader_id())
    }
    pub fn term(&self) -> u64 {
        self.root.map_or(0, |r| r.term())
    }
    pub fn term_stale(&self) -> bool {
        self.root.is_some_and(|r| r.term_stale())
    }
    pub fn membership_epoch(&self) -> u64 {
        self.root.map_or(0, |r| r.membership_epoch())
    }
    pub fn epoch_mismatch(&self) -> bool {
        self.root.is_some_and(|r| r.epoch_mismatch())
    }
}

// ── FBAcceptedResponseRef ────────────────────────────────────────

/// Zero-copy view over an `FBAcceptedResponse` control buffer.
pub struct FBAcceptedResponseRef<'a> {
    root: Option<FBAcceptedResponse<'a>>,
}

impl<'a> FBAcceptedResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBAcceptedResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn slot(&self) -> u64 {
        self.root.map_or(0, |r| r.slot())
    }
    pub fn round(&self) -> u64 {
        self.root.map_or(0, |r| r.round())
    }
    pub fn leader_id(&self) -> u64 {
        self.root.map_or(0, |r| r.leader_id())
    }
    pub fn rejected(&self) -> bool {
        self.root.is_some_and(|r| r.rejected())
    }
    pub fn rejected_round(&self) -> u64 {
        self.root.map_or(0, |r| r.rejected_round())
    }
    pub fn rejected_leader_id(&self) -> u64 {
        self.root.map_or(0, |r| r.rejected_leader_id())
    }
    pub fn term(&self) -> u64 {
        self.root.map_or(0, |r| r.term())
    }
    pub fn term_stale(&self) -> bool {
        self.root.is_some_and(|r| r.term_stale())
    }
    pub fn membership_epoch(&self) -> u64 {
        self.root.map_or(0, |r| r.membership_epoch())
    }
    pub fn epoch_mismatch(&self) -> bool {
        self.root.is_some_and(|r| r.epoch_mismatch())
    }
}

// ── FBHeartbeatResponseRef ───────────────────────────────────────

/// Zero-copy view over an `FBHeartbeatResponse` control buffer.
pub struct FBHeartbeatResponseRef<'a> {
    root: Option<FBHeartbeatResponse<'a>>,
}

impl<'a> FBHeartbeatResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBHeartbeatResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn term(&self) -> u64 {
        self.root.map_or(0, |r| r.term())
    }
    pub fn success(&self) -> bool {
        self.root.is_some_and(|r| r.success())
    }
    pub fn contiguous_chosen(&self) -> u64 {
        self.root.map_or(0, |r| r.contiguous_chosen())
    }
    pub fn last_chosen_term(&self) -> u64 {
        self.root.map_or(0, |r| r.last_chosen_term())
    }
    pub fn contiguous_applied(&self) -> u64 {
        self.root.map_or(0, |r| r.contiguous_applied())
    }
    pub fn highest_seen_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.highest_seen_slot())
    }
    pub fn durable_snapshot_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.durable_snapshot_slot())
    }
}

// ── FBPreVoteResponseRef ─────────────────────────────────────────

/// Zero-copy view over an `FBPreVoteResponse` control buffer.
pub struct FBPreVoteResponseRef<'a> {
    root: Option<FBPreVoteResponse<'a>>,
}

impl<'a> FBPreVoteResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBPreVoteResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn term(&self) -> u64 {
        self.root.map_or(0, |r| r.term())
    }
    pub fn granted(&self) -> bool {
        self.root.is_some_and(|r| r.granted())
    }
    pub fn contiguous_chosen(&self) -> u64 {
        self.root.map_or(0, |r| r.contiguous_chosen())
    }
    pub fn last_chosen_term(&self) -> u64 {
        self.root.map_or(0, |r| r.last_chosen_term())
    }
    pub fn highest_seen_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.highest_seen_slot())
    }
}

// ── FBRequestVoteResponseRef ─────────────────────────────────────

/// Zero-copy view over an `FBRequestVoteResponse` control buffer.
pub struct FBRequestVoteResponseRef<'a> {
    root: Option<FBRequestVoteResponse<'a>>,
}

impl<'a> FBRequestVoteResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBRequestVoteResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn term(&self) -> u64 {
        self.root.map_or(0, |r| r.term())
    }
    pub fn granted(&self) -> bool {
        self.root.is_some_and(|r| r.granted())
    }
    pub fn contiguous_chosen(&self) -> u64 {
        self.root.map_or(0, |r| r.contiguous_chosen())
    }
    pub fn last_chosen_term(&self) -> u64 {
        self.root.map_or(0, |r| r.last_chosen_term())
    }
    pub fn highest_seen_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.highest_seen_slot())
    }
}

// ── FBStepDownResponseRef ────────────────────────────────────────

/// Zero-copy view over an `FBStepDownResponse` control buffer.
pub struct FBStepDownResponseRef<'a> {
    root: Option<FBStepDownResponse<'a>>,
}

impl<'a> FBStepDownResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBStepDownResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn accepted(&self) -> bool {
        self.root.is_some_and(|r| r.accepted())
    }
    pub fn current_term(&self) -> u64 {
        self.root.map_or(0, |r| r.current_term())
    }
    pub fn current_leader_id(&self) -> u64 {
        self.root.map_or(0, |r| r.current_leader_id())
    }
}

// ── FBFetchGapResponseRef ────────────────────────────────────────

/// Zero-copy view over an `FBFetchGapResponse` control buffer.
pub struct FBFetchGapResponseRef<'a> {
    root: Option<FBFetchGapResponse<'a>>,
}

impl<'a> FBFetchGapResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBFetchGapResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn version(&self) -> u32 {
        self.root.map_or(0, |r| r.version())
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn slot(&self) -> u64 {
        self.root.map_or(0, |r| r.slot())
    }
    pub fn term(&self) -> u64 {
        self.root.map_or(0, |r| r.term())
    }
    pub fn ballot_round(&self) -> u64 {
        self.root.map_or(0, |r| r.ballot_round())
    }
    pub fn leader_id(&self) -> u64 {
        self.root.map_or(0, |r| r.leader_id())
    }
    /// Returns the payload bytes (zero-copy — borrows from the buffer).
    pub fn payload(&self) -> Option<&[u8]> {
        self.root.and_then(|r| r.payload()).map(|v| v.bytes())
    }
}

// ── FBSnapshotResponseRef ────────────────────────────────────────

/// Zero-copy view over an `FBSnapshotResponse` control buffer.
/// The snapshot bytes are in the frame's data buffer, not in this
/// control buffer — the caller accesses them via `Response::data`.
pub struct FBSnapshotResponseRef<'a> {
    root: Option<FBSnapshotResponse<'a>>,
}

impl<'a> FBSnapshotResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBSnapshotResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvRetCode {
        self.root.map_or(FBKvRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn term_at_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.term_at_slot())
    }
    pub fn membership_epoch(&self) -> u64 {
        self.root.map_or(0, |r| r.membership_epoch())
    }
    pub fn at_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.at_slot())
    }
}
