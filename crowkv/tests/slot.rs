// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Slot-list subsystem tests.
//!
//! `PxSlotList` is an independently runnable storage tier (chunked, lock-free
//! slot map) that sits between the WAL and the replica layer. These tests
//! exercise it in isolation: insert/get/trim, chunk growth, tail lookup, and
//! atomic guard access.

#[path = "slot/slot_list_test.rs"]
mod slot_list;

#[path = "slot/concurrent_test.rs"]
mod concurrent;

#[path = "slot/reclaim_test.rs"]
mod reclaim;
