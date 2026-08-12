// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `StatusManager` — applies status transitions and computes effective status.

use crow_protocol::common::HwStatus;
use std::time::{Duration, Instant};

/// Result of a status transition check.
pub type TransitionResult<T> = Result<T, String>;

/// Manages hardware status transitions.
pub struct StatusManager {
    temp_failure_timeout: Duration,
}

impl StatusManager {
    pub fn new(temp_failure_timeout_secs: u32) -> Self {
        Self {
            temp_failure_timeout: Duration::from_secs(u64::from(temp_failure_timeout_secs)),
        }
    }

    /// Check if a status transition is legal (design doc §9).
    pub fn is_legal_transition(from: HwStatus, to: HwStatus) -> bool {
        match (from, to) {
            (HwStatus::Init, HwStatus::Up | HwStatus::Offline | HwStatus::Maintenance) => true,
            (HwStatus::Up, HwStatus::Suspect | HwStatus::Offline | HwStatus::Maintenance) => true,
            (HwStatus::Suspect, HwStatus::Up | HwStatus::Missing | HwStatus::Offline) => true,
            (HwStatus::Missing, HwStatus::Bad | HwStatus::Up) => true,
            (HwStatus::Offline, HwStatus::Maintenance | HwStatus::Up)
            | (HwStatus::Maintenance, HwStatus::Offline) => true,
            (HwStatus::Bad, _) | (_, HwStatus::Bad) => false,
            _ => false,
        }
    }

    /// Apply a status transition, validating legality.
    pub fn apply_transition(from: HwStatus, to: HwStatus) -> TransitionResult<()> {
        if Self::is_legal_transition(from, to) {
            Ok(())
        } else {
            Err(format!("illegal transition: {from:?} -> {to:?}"))
        }
    }

    /// Compute effective status = `max(node, group, disk)`.
    pub fn effective_status(node: HwStatus, group: HwStatus, disk: HwStatus) -> HwStatus {
        node.max(group).max(disk)
    }

    /// Check if allocation is allowed (Up only).
    pub fn allows_allocate(effective: HwStatus) -> bool {
        effective == HwStatus::Up
    }

    /// Check if free is allowed (Up, Maintenance, or Suspect).
    pub fn allows_free(effective: HwStatus) -> bool {
        matches!(
            effective,
            HwStatus::Up | HwStatus::Maintenance | HwStatus::Suspect
        )
    }

    /// Check suspect timeouts — transitions Suspect > timeout to Offline.
    /// Returns true if a timeout transition was applied.
    pub fn check_suspect_timeout(&self, suspect_since: Instant, now: Instant) -> bool {
        now.duration_since(suspect_since) >= self.temp_failure_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legal_transitions() {
        assert!(StatusManager::is_legal_transition(HwStatus::Init, HwStatus::Up));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Up,
            HwStatus::Suspect
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Suspect,
            HwStatus::Up
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Suspect,
            HwStatus::Missing
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Missing,
            HwStatus::Bad
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Missing,
            HwStatus::Up
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Offline,
            HwStatus::Maintenance
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Maintenance,
            HwStatus::Offline
        ));
        assert!(StatusManager::is_legal_transition(
            HwStatus::Offline,
            HwStatus::Up
        ));
    }

    #[test]
    fn test_illegal_transitions() {
        assert!(!StatusManager::is_legal_transition(HwStatus::Up, HwStatus::Init));
        assert!(!StatusManager::is_legal_transition(HwStatus::Bad, HwStatus::Up));
        assert!(!StatusManager::is_legal_transition(HwStatus::Up, HwStatus::Bad));
        assert!(!StatusManager::is_legal_transition(
            HwStatus::Init,
            HwStatus::Suspect
        ));
    }

    #[test]
    fn test_effective_status() {
        assert_eq!(
            StatusManager::effective_status(HwStatus::Up, HwStatus::Up, HwStatus::Up),
            HwStatus::Up
        );
        assert_eq!(
            StatusManager::effective_status(HwStatus::Up, HwStatus::Up, HwStatus::Offline),
            HwStatus::Offline
        );
        assert_eq!(
            StatusManager::effective_status(HwStatus::Up, HwStatus::Maintenance, HwStatus::Up),
            HwStatus::Maintenance
        );
    }

    #[test]
    fn test_allows_allocate() {
        assert!(StatusManager::allows_allocate(HwStatus::Up));
        assert!(!StatusManager::allows_allocate(HwStatus::Maintenance));
        assert!(!StatusManager::allows_allocate(HwStatus::Offline));
    }

    #[test]
    fn test_allows_free() {
        assert!(StatusManager::allows_free(HwStatus::Up));
        assert!(StatusManager::allows_free(HwStatus::Maintenance));
        assert!(StatusManager::allows_free(HwStatus::Suspect));
        assert!(!StatusManager::allows_free(HwStatus::Offline));
    }
}
