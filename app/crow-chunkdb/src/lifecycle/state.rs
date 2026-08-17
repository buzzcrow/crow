// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk state machine — `Init → Active → Sealed → Deleted`.
//!
//! Design §9: validates transitions; invalid transitions return
//! `InvalidStateTransition`.

use crow_protocol::chunkdb::rpc::ChunkState as ProtoChunkState;

/// Chunk lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkState {
    Init,
    Active,
    Sealed,
    Deleted,
}

impl ChunkState {
    /// Convert from proto i32 state.
    #[must_use]
    pub fn from_proto(state: i32) -> Self {
        match ProtoChunkState::try_from(state) {
            Ok(ProtoChunkState::Init) => Self::Init,
            Ok(ProtoChunkState::Active) => Self::Active,
            Ok(ProtoChunkState::Sealed) => Self::Sealed,
            Ok(ProtoChunkState::Deleted) => Self::Deleted,
            Err(_) => Self::Init,
        }
    }

    /// Convert to proto i32 state.
    #[must_use]
    pub fn to_proto(self) -> i32 {
        match self {
            Self::Init => ProtoChunkState::Init as i32,
            Self::Active => ProtoChunkState::Active as i32,
            Self::Sealed => ProtoChunkState::Sealed as i32,
            Self::Deleted => ProtoChunkState::Deleted as i32,
        }
    }

    /// Check if the chunk can be appended to (must be Active).
    pub fn check_can_append(self) -> Result<(), StateTransitionError> {
        if self == Self::Active {
            Ok(())
        } else {
            Err(StateTransitionError::new(self, "Active"))
        }
    }

    /// Check if the chunk can be sealed (must be Active).
    pub fn check_can_seal(self) -> Result<(), StateTransitionError> {
        if self == Self::Active {
            Ok(())
        } else {
            Err(StateTransitionError::new(self, "Active"))
        }
    }

    /// Check if the chunk can be deleted (must be Active or Sealed).
    pub fn check_can_delete(self) -> Result<(), StateTransitionError> {
        if self == Self::Active || self == Self::Sealed {
            Ok(())
        } else {
            Err(StateTransitionError::new(self, "Active|Sealed"))
        }
    }
}

/// State transition error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid state transition: {from:?} → {expected} (expected {expected})")]
pub struct StateTransitionError {
    pub from: ChunkState,
    pub expected: &'static str,
}

impl StateTransitionError {
    #[must_use]
    pub fn new(from: ChunkState, expected: &'static str) -> Self {
        Self { from, expected }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trip() {
        for state in [
            ChunkState::Init,
            ChunkState::Active,
            ChunkState::Sealed,
            ChunkState::Deleted,
        ] {
            let proto = state.to_proto();
            assert_eq!(ChunkState::from_proto(proto), state);
        }
    }

    #[test]
    fn active_can_append() {
        assert!(ChunkState::Active.check_can_append().is_ok());
        assert!(ChunkState::Sealed.check_can_append().is_err());
        assert!(ChunkState::Deleted.check_can_append().is_err());
    }

    #[test]
    fn active_can_seal() {
        assert!(ChunkState::Active.check_can_seal().is_ok());
        assert!(ChunkState::Sealed.check_can_seal().is_err());
        assert!(ChunkState::Deleted.check_can_seal().is_err());
    }

    #[test]
    fn active_or_sealed_can_delete() {
        assert!(ChunkState::Active.check_can_delete().is_ok());
        assert!(ChunkState::Sealed.check_can_delete().is_ok());
        assert!(ChunkState::Deleted.check_can_delete().is_err());
        assert!(ChunkState::Init.check_can_delete().is_err());
    }
}
