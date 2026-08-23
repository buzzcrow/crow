// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Client library for CROW diskio.
//!
//! Provides async disk write/read/fsync via the crow-rpc transport.
//! The client builds flatbuffer control messages (`FBDiskWriteRequest`,
//! `FBDiskReadRequest`, `FBDiskFsyncRequest`), sends them via `RpcClient::call`,
//! and parses the response control to extract the return code.

mod client;

pub use client::{DiskId, DiskIoRetCode, DiskioClient, DiskioError, DiskioResult};
