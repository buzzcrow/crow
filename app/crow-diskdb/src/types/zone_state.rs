// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Zone health and allocation-lifecycle states.

use serde::{Deserialize, Serialize};

/// Zone hardware health, derived from the underlying disk during refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ZoneState {
    Healthy,
    Missing,
    Bad,
}

/// Zone allocation lifecycle. Transitions are performed via CAS for
/// lock-free serialization within one diskdb instance.
/// `#[repr(u8)]` so it can be stored in an `AtomicU8`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ZoneAllocationState {
    Active = 0,
    Busy = 1,
    Error = 2,
    Full = 3,
}

impl ZoneAllocationState {
    /// Decode a `u8` (e.g. from an `AtomicU8` load) into a state.
    /// Unknown values map to `Error` (defensive — corruption/bug).
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Active,
            1 => Self::Busy,
            // 2 and unknown both map to Error; explicit arm documents the known value.
            #[allow(clippy::match_same_arms)]
            2 => Self::Error,
            3 => Self::Full,
            _ => Self::Error,
        }
    }
}
