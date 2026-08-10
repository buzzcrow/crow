// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common type aliases that complement the proto types in
//! `common_type.proto`.

/// Node identifier (integer, assigned by the cluster).
pub type NodeId = u64;

/// Disk-group identifier (integer, globally unique).
pub type DiskGroupId = u32;
