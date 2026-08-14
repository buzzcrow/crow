// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Lifecycle state machine unit tests.

use crow_chunkdb::lifecycle::state::{ChunkState, StateTransitionError};
use crow_protocol::chunkdb::rpc::ChunkState as ProtoChunkState;

#[test]
fn state_round_trip_proto() {
    for (rust, proto) in [
        (ChunkState::Init, ProtoChunkState::Init),
        (ChunkState::Active, ProtoChunkState::Active),
        (ChunkState::Sealed, ProtoChunkState::Sealed),
        (ChunkState::Deleted, ProtoChunkState::Deleted),
    ] {
        assert_eq!(ChunkState::from_proto(proto as i32), rust);
        assert_eq!(rust.to_proto(), proto as i32);
    }
}

#[test]
fn from_proto_invalid_defaults_to_init() {
    assert_eq!(ChunkState::from_proto(999), ChunkState::Init);
}

#[test]
fn active_can_append() {
    assert!(ChunkState::Active.check_can_append().is_ok());
    assert!(ChunkState::Sealed.check_can_append().is_err());
    assert!(ChunkState::Deleted.check_can_append().is_err());
    assert!(ChunkState::Init.check_can_append().is_err());
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

#[test]
fn transition_error_message() {
    let err = StateTransitionError::new(ChunkState::Deleted, "Active|Sealed");
    let msg = err.to_string();
    assert!(msg.contains("Deleted"));
    assert!(msg.contains("Active|Sealed"));
}
