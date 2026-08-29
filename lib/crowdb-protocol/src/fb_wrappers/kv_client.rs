// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::must_use_candidate)]

//! Zero-copy `FB<Type>Ref` wrappers for KV client-facing response
//! types (design-crowdb-rpc.md §6, R117).
//!
//! Each wrapper holds a `&[u8]` reference to the control buffer,
//! parses the root on construction, and exposes typed accessor methods
//! that read through the root pointer — no per-field copy, no owned
//! intermediate struct.

use crate::kv_client_fb::{
    FBBytes, FBCreateSnapshotResponse, FBKvClientRetCode, FBKvJournalScanResponse, FBKvResponse,
    FBKvScanResponse, FBListSnapshotsResponse, FBReleaseSnapshotResponse, FBSnapshotScanResponse,
    FBWatchNotify, FBWatchNotifyError,
};

use super::parse_root;

// ── FBKvResponseRef ─────────────────────────────────────────────

/// Zero-copy view over an `FBKvResponse` control buffer. Shared by
/// Put / Get / Delete / `BatchWrite` (all return `FBKvResponse`).
pub struct FBKvResponseRef<'a> {
    root: Option<FBKvResponse<'a>>,
}

impl<'a> FBKvResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBKvResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
    pub fn revision(&self) -> u64 {
        self.root.map_or(0, |r| r.revision())
    }
    pub fn not_found(&self) -> bool {
        self.root.is_some_and(|r| r.not_found())
    }
    pub fn not_leader_hint(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.not_leader_hint())
    }
    pub fn value(&self) -> Option<&[u8]> {
        self.root.and_then(|r| r.value()).map(|v| v.bytes())
    }
    pub fn read_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.read_slot())
    }
    pub fn safe_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.safe_slot())
    }
}

// ── FBKvScanResponseRef ─────────────────────────────────────────

/// Zero-copy view over an `FBKvScanResponse` control buffer.
pub struct FBKvScanResponseRef<'a> {
    root: Option<FBKvScanResponse<'a>>,
}

impl<'a> FBKvScanResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBKvScanResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
    pub fn truncated(&self) -> bool {
        self.root.is_some_and(|r| r.truncated())
    }
    pub fn items(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<crate::kv_client_fb::FBKvScanItem<'a>>>>
    {
        self.root.and_then(|r| r.items())
    }
    pub fn read_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.read_slot())
    }
    pub fn not_leader_hint(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.not_leader_hint())
    }
    pub fn count(&self) -> u64 {
        self.root.map_or(0, |r| r.count())
    }
    pub fn timed_out(&self) -> bool {
        self.root.is_some_and(|r| r.timed_out())
    }
}

// ── FBKvJournalScanResponseRef ──────────────────────────────────

/// Zero-copy view over an `FBKvJournalScanResponse` control buffer.
pub struct FBKvJournalScanResponseRef<'a> {
    root: Option<FBKvJournalScanResponse<'a>>,
}

impl<'a> FBKvJournalScanResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBKvJournalScanResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
    pub fn ops(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<crate::kv_client_fb::FBKvJournalOp<'a>>>>
    {
        self.root.and_then(|r| r.ops())
    }
    pub fn truncated(&self) -> bool {
        self.root.is_some_and(|r| r.truncated())
    }
    pub fn last_op_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.last_op_slot())
    }
    pub fn read_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.read_slot())
    }
    pub fn not_leader_hint(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.not_leader_hint())
    }
}

// ── FBCreateSnapshotResponseRef ─────────────────────────────────

/// Zero-copy view over an `FBCreateSnapshotResponse` control buffer.
pub struct FBCreateSnapshotResponseRef<'a> {
    root: Option<FBCreateSnapshotResponse<'a>>,
}

impl<'a> FBCreateSnapshotResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBCreateSnapshotResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
    pub fn snapshot_handle(&self) -> u64 {
        self.root.map_or(0, |r| r.snapshot_handle())
    }
    pub fn at_slot(&self) -> u64 {
        self.root.map_or(0, |r| r.at_slot())
    }
    pub fn not_leader_hint(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.not_leader_hint())
    }
}

// ── FBListSnapshotsResponseRef ──────────────────────────────────

/// Zero-copy view over an `FBListSnapshotsResponse` control buffer.
pub struct FBListSnapshotsResponseRef<'a> {
    root: Option<FBListSnapshotsResponse<'a>>,
}

impl<'a> FBListSnapshotsResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBListSnapshotsResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
    pub fn snapshots(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<crate::kv_client_fb::FBSnapshotInfo<'a>>>>
    {
        self.root.and_then(|r| r.snapshots())
    }
}

// ── FBSnapshotScanResponseRef ───────────────────────────────────

/// Zero-copy view over an `FBSnapshotScanResponse` control buffer.
pub struct FBSnapshotScanResponseRef<'a> {
    root: Option<FBSnapshotScanResponse<'a>>,
}

impl<'a> FBSnapshotScanResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBSnapshotScanResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
    pub fn truncated(&self) -> bool {
        self.root.is_some_and(|r| r.truncated())
    }
    pub fn items(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<crate::kv_client_fb::FBKvScanItem<'a>>>>
    {
        self.root.and_then(|r| r.items())
    }
}

// ── FBReleaseSnapshotResponseRef ────────────────────────────────

/// Zero-copy view over an `FBReleaseSnapshotResponse` control buffer.
pub struct FBReleaseSnapshotResponseRef<'a> {
    root: Option<FBReleaseSnapshotResponse<'a>>,
}

impl<'a> FBReleaseSnapshotResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBReleaseSnapshotResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBKvClientRetCode {
        self.root.map_or(FBKvClientRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| r.ok())
    }
}

// ── FBWatchNotifyRef ────────────────────────────────────────────

/// Zero-copy view over an `FBWatchNotify` control buffer (server→
/// client push frame). `keys`/`values` are vectors of `FBBytes`
/// wrapper tables (flatbuffers rejects `[[ubyte]]` nested vectors).
pub struct FBWatchNotifyRef<'a> {
    root: Option<FBWatchNotify<'a>>,
}

impl<'a> FBWatchNotifyRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBWatchNotify>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn prefix(&self) -> Option<&[u8]> {
        self.root.and_then(|r| r.prefix()).map(|v| v.bytes())
    }
    /// Iterate the changed keys (each a `&[u8]` borrowed from the
    /// `FBBytes` wrapper). Returns `None` if the field is absent or
    /// the buffer is invalid.
    pub fn keys(&self) -> Option<FBBytesVecIter<'a>> {
        self.root.and_then(|r| r.keys()).map(FBBytesVecIter::new)
    }
    pub fn slot(&self) -> u64 {
        self.root.map_or(0, |r| r.slot())
    }
    /// Iterate the changed values (each a `&[u8]`; empty = Delete).
    pub fn values(&self) -> Option<FBBytesVecIter<'a>> {
        self.root.and_then(|r| r.values()).map(FBBytesVecIter::new)
    }
}

// ── FBWatchNotifyErrorRef ───────────────────────────────────────

/// Zero-copy view over an `FBWatchNotifyError` control buffer
/// (server→client push frame).
pub struct FBWatchNotifyErrorRef<'a> {
    root: Option<FBWatchNotifyError<'a>>,
}

impl<'a> FBWatchNotifyErrorRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBWatchNotifyError>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn group_id(&self) -> u64 {
        self.root.map_or(0, |r| r.group_id())
    }
    pub fn not_leader_hint(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.not_leader_hint())
    }
    pub fn error(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error())
    }
}

// ── FBBytesVecIter ──────────────────────────────────────────────

/// Iterator over a `Vector<ForwardsUOffset<FBBytes>>`, yielding each
/// inner `&[u8]` (the `data` field of the `FBBytes` wrapper table).
/// Used by `FBWatchNotifyRef::keys` / `values`.
pub struct FBBytesVecIter<'a> {
    inner: flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<FBBytes<'a>>>,
    idx: usize,
}

impl<'a> FBBytesVecIter<'a> {
    fn new(inner: flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<FBBytes<'a>>>) -> Self {
        Self { inner, idx: 0 }
    }
}

impl<'a> Iterator for FBBytesVecIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.inner.len() {
            return None;
        }
        let bytes = self.inner.get(self.idx);
        self.idx += 1;
        bytes.data().map(|v| v.bytes())
    }
}
