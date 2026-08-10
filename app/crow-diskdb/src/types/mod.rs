// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Core types for diskdb.
//!
//! Mirrors the protocol hierarchy (`DiskGroup` → `Disk` → `Zone`) and
//! adds the journal/snapshot types that are internal to diskdb (not
//! exposed via gRPC). See `doc/design/diskdb/design-crow-diskdb.md`.

pub mod disk;
pub mod disk_group;
pub mod disk_state;
pub mod ids;
pub mod instance;
pub mod journal;
pub mod node;
pub mod status;
pub mod zone_state;

pub use disk::DiskMeta;
pub use disk_group::DiskGroupMeta;
pub use disk_state::{DiskState, DiskType};
pub use ids::{ClaimSnapshot, DiskGroupId, DiskUuid, NodeId, Segment};
pub use instance::InstanceMeta;
pub use journal::{BusyRecord, FreeRecord, ZoneRecord};
pub use node::NodeMeta;
pub use status::{effective_status, Status};
pub use zone_state::{ZoneAllocationState, ZoneState};
