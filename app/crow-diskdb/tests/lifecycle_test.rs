// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `LifecycleState` + `StartupPhase` tests.

use crow_diskdb::liveness::lifecycle::{LifecycleState, StartupPhase};

#[test]
fn test_lifecycle_default_is_init() {
    let state = LifecycleState::new();
    assert_eq!(state.get(), StartupPhase::Init);
}

#[test]
fn test_lifecycle_transitions() {
    let state = LifecycleState::new();
    state.set(StartupPhase::Syncing);
    assert_eq!(state.get(), StartupPhase::Syncing);
    state.set(StartupPhase::Loading);
    assert_eq!(state.get(), StartupPhase::Loading);
    state.set(StartupPhase::Up);
    assert_eq!(state.get(), StartupPhase::Up);
}

#[test]
fn test_phase_allows_mutating_rpcs() {
    assert!(!StartupPhase::Init.allows_mutating_rpcs());
    assert!(!StartupPhase::Syncing.allows_mutating_rpcs());
    assert!(!StartupPhase::Loading.allows_mutating_rpcs());
    assert!(StartupPhase::Up.allows_mutating_rpcs());
}

#[test]
fn test_phase_as_str() {
    assert_eq!(StartupPhase::Init.as_str(), "init");
    assert_eq!(StartupPhase::Syncing.as_str(), "syncing");
    assert_eq!(StartupPhase::Loading.as_str(), "loading");
    assert_eq!(StartupPhase::Up.as_str(), "up");
}

#[test]
fn test_container_lifecycle_phase() {
    use crow_diskdb::model::disk_group_container::DdbDiskGroupContainer;
    let container = DdbDiskGroupContainer::new(1);
    assert_eq!(container.lifecycle_phase(), StartupPhase::Init);
    container.set_lifecycle_phase(StartupPhase::Up);
    assert_eq!(container.lifecycle_phase(), StartupPhase::Up);
}
