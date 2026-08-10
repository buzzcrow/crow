// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Disk health state and physical disk technology.

use serde::{Deserialize, Serialize};

/// Health state for a physical disk, probed locally. Distinct from
/// operator-set `Status` (which is synced from group 0). Not stored in
/// group 0 — each diskdb instance probes its own disks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiskState {
    Init,
    Active,
    Suspect,
    Missing,
    Bad,
}

/// Physical disk technology. Determines which zone implementation is
/// used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DiskType {
    /// Conventional block HDD.
    BlockHdd,
    /// Block SSD or simulated zoned SSD.
    BlockSsd,
    /// Native zoned SSD.
    ZoneSsd,
    /// Shingled Magnetic Recording HDD.
    SmrHdd,
}
