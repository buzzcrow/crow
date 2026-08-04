// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Unique port allocator and serialization lock for parallel integration tests.
//!
//! Rust's default test runner executes tests in parallel. Tests that use
//! hardcoded placeholder ports (e.g. `node_id + 10_000`) collide when two
//! tests happen to use the same node IDs. [`unique_port`] solves the port
//! collision. However, cluster tests with tight election timers (5 ms
//! heartbeat) are also sensitive to tokio runtime contention under parallel
//! load, so [`lock`] provides a mutex that tests can hold for their entire
//! duration to prevent timing-induced failures.

use std::sync::{
    atomic::{AtomicU16, Ordering},
    OnceLock,
};

use tokio::sync::Mutex;

/// Global counter — incremented per allocation so every call gets a
/// unique port. Starts well above the ephemeral range to avoid clashes
/// with OS-assigned `:0` ports.
static PORT_COUNTER: AtomicU16 = AtomicU16::new(20_000);

static NET_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// Allocate a unique port for a test node. Each call returns the next
/// available port, guaranteeing no two nodes (even across parallel tests)
/// share the same placeholder port.
pub fn unique_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Acquire the global network test lock. Hold the guard for the duration of
/// the test (store it in the cluster struct) to prevent timing-sensitive
/// election tests from interfering with each other under parallel load.
pub async fn lock() -> tokio::sync::MutexGuard<'static, ()> {
    NET_LOCK.get_or_init(|| Mutex::new(())).lock().await
}
