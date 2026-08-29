// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROWDB diskio.
//!
//! Provides async disk write/read/fsync via the crowdb-rpc transport.
//! The client builds flatbuffer control messages (`FBDiskWriteRequest`,
//! `FBDiskReadRequest`, `FBDiskFsyncRequest`), sends them via `RpcClient::call`,
//! and parses the response control to extract the return code.

mod client;

pub use client::{DiskId, DiskIoRetCode, DiskioClient, DiskioError, DiskioResult};
