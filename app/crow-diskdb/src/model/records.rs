// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Domain record read-models — the in-memory view of the durable
//! `BusyBlockKey`/`FreeBlockKey`/`ZoneKey` records from `crow-protocol`.
//! Used by R73 recovery (strategy 1 full scan + strategy 2 journal
//! replay).

use crow_protocol::diskdb::rpc::{BusyBlockValue, FreeBlockValue, ZoneValue};
use crow_protocol::key::{BusyBlockKey, FreeBlockKey};

/// A busy-block record read from the data group.
#[derive(Debug, Clone)]
pub struct BusyRecord {
    pub key: BusyBlockKey,
    pub value: BusyBlockValue,
}

/// A free-block record read from the data group.
#[derive(Debug, Clone)]
pub struct FreeRecord {
    pub key: FreeBlockKey,
    pub value: FreeBlockValue,
}

/// All records for one zone (used by R73 recovery).
#[derive(Debug, Clone, Default)]
pub struct ZoneRecords {
    pub zone_value: Option<ZoneValue>,
    pub busy: Vec<BusyRecord>,
    pub free: Vec<FreeRecord>,
}
