// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `HwStateMachine` — validates + applies hardware status transitions
//! and computes effective status.
//!
//! The machine is stateless beyond `temp_failure_timeout`; current
//! status lives on the domain objects (`DdbDisk.effective_status`,
//! `DdbDiskGroup` status). Each `transition_*` call validates
//! legality, runs entry side-effects on the domain object, and returns
//! the new status. The caller is responsible for any disk-group-level
//! follow-up (e.g. `rebuild_allocating_disks`) — keeping the machine
//! free of disk-group back-references.

use std::time::{Duration, Instant};

use crowdb_protocol::common::HwStatus;

use crate::model::disk::DdbDisk;
use crate::model::disk_group::DdbDiskGroup;

/// Operations a status may permit or deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Allocate,
    Free,
    Rebuild,
    Probe,
}

/// Error returned by `transition_*` on an illegal transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IllegalTransition {
    pub from: HwStatus,
    pub to: HwStatus,
}

impl std::fmt::Display for IllegalTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "illegal transition: {:?} -> {:?}", self.from, self.to)
    }
}

impl std::error::Error for IllegalTransition {}

/// Stateless hardware status transition machine.
#[derive(Clone)]
pub struct HwStateMachine {
    temp_failure_timeout: Duration,
}

impl HwStateMachine {
    #[must_use]
    pub fn new(temp_failure_timeout_secs: u32) -> Self {
        Self {
            temp_failure_timeout: Duration::from_secs(u64::from(temp_failure_timeout_secs)),
        }
    }

    /// Check if a status transition is legal (design doc §9).
    #[must_use]
    pub fn is_legal_transition(from: HwStatus, to: HwStatus) -> bool {
        match (from, to) {
            (HwStatus::Init, HwStatus::Up | HwStatus::Offline | HwStatus::Maintenance) => true,
            (HwStatus::Up, HwStatus::Suspect | HwStatus::Offline | HwStatus::Maintenance) => true,
            (HwStatus::Suspect, HwStatus::Up | HwStatus::Missing | HwStatus::Offline) => true,
            (HwStatus::Missing, HwStatus::Bad | HwStatus::Up) => true,
            (HwStatus::Offline, HwStatus::Maintenance | HwStatus::Up)
            | (HwStatus::Maintenance, HwStatus::Offline) => true,
            // Operator override: mark a Bad disk Up after physical repair.
            (HwStatus::Bad, HwStatus::Up) => true,
            (HwStatus::Bad, _) | (_, HwStatus::Bad) => false,
            _ => false,
        }
    }

    /// Validate + apply a disk transition, running entry side-effects.
    /// Returns the new status, or `Err` on an illegal transition.
    pub fn transition_disk(&self, disk: &DdbDisk, to: HwStatus) -> Result<HwStatus, IllegalTransition> {
        let from = *disk.effective_status.read().unwrap();
        if !Self::is_legal_transition(from, to) {
            return Err(IllegalTransition { from, to });
        }
        Self::on_enter_disk(to, disk);
        *disk.effective_status.write().unwrap() = to;
        Ok(to)
    }

    /// Validate + apply a disk-group transition. Disk-group status is
    /// stored on `DdbDiskGroup.status`; this validates legality, runs
    /// entry side-effects, and updates the status.
    pub fn transition_disk_group(
        &self,
        dg: &DdbDiskGroup,
        to: HwStatus,
    ) -> Result<HwStatus, IllegalTransition> {
        let from = *dg.status.read().unwrap();
        if !Self::is_legal_transition(from, to) {
            return Err(IllegalTransition { from, to });
        }
        Self::on_enter_disk_group(to, dg);
        *dg.status.write().unwrap() = to;
        Ok(to)
    }

    /// Effective status = `max(node, group, disk)` — unchanged.
    #[must_use]
    pub fn effective_status(node: HwStatus, group: HwStatus, disk: HwStatus) -> HwStatus {
        node.max(group).max(disk)
    }

    /// Per-state operation permission (replaces `allows_allocate`/
    /// `allows_free`).
    #[must_use]
    pub fn permits(status: HwStatus, op: Op) -> bool {
        match op {
            Op::Allocate => status == HwStatus::Up,
            Op::Free => matches!(status, HwStatus::Up | HwStatus::Maintenance | HwStatus::Suspect),
            Op::Rebuild | Op::Probe => true,
        }
    }

    /// Entry side-effects for a disk. Reserved hook for future
    /// per-status disk-level side-effects (e.g. zone marking on Bad
    /// once per-zone recovery exists). v1 has no disk-level
    /// side-effects — the disk's `effective_status` is the sole
    /// gatekeeper for the allocate path, and the caller handles
    /// disk-group-level follow-up like `rebuild_allocating_disks`.
    /// Kept as a called no-op (from `transition_disk`) so the hook
    /// point is wired and a future side-effect lands in one place
    /// rather than being scattered across callers.
    pub fn on_enter_disk(_status: HwStatus, _disk: &DdbDisk) {}

    /// Entry side-effects for a disk-group. v1 has no disk-group-level
    /// side-effects; reserved for future use.
    pub fn on_enter_disk_group(_status: HwStatus, _dg: &DdbDiskGroup) {
        // No disk-group-level side-effects in v1.
    }

    /// Check suspect timeouts — transitions Suspect > timeout to
    /// Offline. Returns true if the timeout has elapsed.
    #[must_use]
    pub fn check_suspect_timeout(&self, suspect_since: Instant, now: Instant) -> bool {
        now.duration_since(suspect_since) >= self.temp_failure_timeout
    }
}
