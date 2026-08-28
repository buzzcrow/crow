// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Startup lifecycle state — lock-free phase tracking for the diskdb
//! server.
//!
//! The service starts crow-rpc immediately (phase `Syncing`/`Loading`)
//! so health checks work during zone loading. RPCs that mutate state
//! are gated on phase `Up`; read-only RPCs are allowed earlier.

use std::sync::atomic::{AtomicU8, Ordering};

/// Startup phase of the diskdb server.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPhase {
    /// Initial state — config loaded, nothing running.
    Init = 0,
    /// Keep-alive tick running — populating in-memory state.
    Syncing = 1,
    /// Zone loading in progress — loading zone bitmaps from KV records.
    Loading = 2,
    /// Fully up — all disk-groups loaded, serving all RPCs.
    Up = 3,
}

impl StartupPhase {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Syncing => "syncing",
            Self::Loading => "loading",
            Self::Up => "up",
        }
    }

    /// Whether mutating RPCs (allocate/free/rebuild) are allowed.
    #[must_use]
    pub fn allows_mutating_rpcs(self) -> bool {
        self == Self::Up
    }
}

/// Lock-free lifecycle state (atomic phase).
pub struct LifecycleState(AtomicU8);

impl LifecycleState {
    #[must_use]
    pub fn new() -> Self {
        Self(AtomicU8::new(StartupPhase::Init as u8))
    }

    #[must_use]
    pub fn get(&self) -> StartupPhase {
        let v = self.0.load(Ordering::Acquire);
        match v {
            0 => StartupPhase::Init,
            1 => StartupPhase::Syncing,
            2 => StartupPhase::Loading,
            _ => StartupPhase::Up,
        }
    }

    pub fn set(&self, phase: StartupPhase) {
        self.0.store(phase as u8, Ordering::Release);
    }
}

impl Default for LifecycleState {
    fn default() -> Self {
        Self::new()
    }
}
