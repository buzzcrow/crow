// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]

//! Hand-written Rust types replacing the prost-generated `crow.diskio.rpc`
//! types. API-compatible with the former proto-generated structs.

use serde::{Deserialize, Serialize};

use crate::common::DiskId;

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskWriteRequest {
    pub disk_id: Option<DiskId>,
    pub zone_index: u32,
    pub offset: u64,
    pub size: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskWriteResponse {
    pub written: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskReadRequest {
    pub disk_id: Option<DiskId>,
    pub zone_index: u32,
    pub offset: u64,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskReadResponse {
    pub data: Vec<u8>,
}
