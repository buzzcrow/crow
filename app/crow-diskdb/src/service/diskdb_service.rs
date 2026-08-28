// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb service utility functions and constants.
//!
//! The legacy tonic server trait impl has been removed. The crow-rpc
//! dispatch lives in `diskdb_rpc_service.rs`. This module retains
//! shared constants and helpers used by the crow-rpc handlers.

/// Maximum number of blocks per `AllocateBlocks` request.
pub(crate) const MAX_ALLOCATE_COUNT: u32 = 1024;

/// `u32::MAX` sentinel for "all zones on the disk".
pub(crate) const ALL_ZONES: u32 = u32::MAX;

/// Elapsed nanoseconds as u64 (saturating cast from u128).
pub(crate) fn elapsed_ns(start: std::time::Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}
