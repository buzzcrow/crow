// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 0.0.

//! Liveness — keep-alive loop, hardware status machine, and startup
//! lifecycle phase.
//!
//! These three modules are grouped because they collectively answer
//! "is this instance alive and ready, and are its disks alive?":
//!
//! - [`keepalive`] — the driver: heartbeat loop that keeps the server
//!   alive, populates in-memory state, and applies status transitions.
//! - [`status_machine`] — runtime hardware health: disk
//!   Up→Suspect→Bad transitions with entry side-effects.
//! - [`lifecycle`] — startup phase: Init→Syncing→Recovering→Up,
//!   gating mutating RPCs until recovery is complete.

pub mod keepalive;
pub mod lifecycle;
pub mod state_machine;
