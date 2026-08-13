// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb domain model — the in-memory model of what the system *is*:
//! disk-groups, disks, zones, allocation/free orchestration, domain
//! errors, and record read-models. No I/O, no transport — pure domain
//! logic + invariants.

pub mod alloc;
pub mod disk;
pub mod disk_group;
pub mod disk_group_container;
pub mod records;
pub mod zone;

pub use alloc::FreeError;
pub use disk::{ActiveZoneContext, DdbDisk, DiskUsage};
pub use disk_group::{AllocClaim, AllocError, AllocateDiskContext, DdbDiskGroup, DiskGroupUsage};
pub use disk_group_container::DdbDiskGroupContainer;
pub use records::{BusyRecord, FreeRecord, ZoneRecords};
pub use zone::{AllocatedRange, DdbZone, DdbZoneHealth, ZoneUsage};
