// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-client RPC `request_id` generator.
//!
//! `RequestId` is a `u64` newtype — the per-frame correlation key
//! extracted from the flatbuffer control message during parse. Each
//! service client owns a `RequestIdGen` that produces monotonically
//! increasing ids via a relaxed `fetch_add`. Per-client (not global)
//! because `request_id` only needs uniqueness within one client's
//! pending map; per-client counters yield smaller numbers → smaller
//! slab pool + pending hashmap.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-frame correlation key. Wraps a `u64` so it is not accidentally
/// interchangeable with raw `u64` at call sites. Convert to `u64` at
/// the FFI boundary via `as_u64()`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub u64);

impl RequestId {
    /// The raw `u64` value, for the FFI boundary.
    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Per-client monotonic `request_id` generator. Thread-safe.
pub struct RequestIdGen {
    counter: AtomicU64,
}

impl RequestIdGen {
    /// Create a new generator starting at 1 (0 is reserved for
    /// "uninitialized" in the C++ slab pool).
    #[must_use]
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }

    /// Return the next `request_id`. Thread-safe; concurrent calls
    /// never return the same value.
    #[must_use]
    pub fn next(&self) -> RequestId {
        RequestId(self.counter.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for RequestIdGen {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn next_is_monotonically_increasing() {
        let gen = RequestIdGen::new();
        let a = gen.next().as_u64();
        let b = gen.next().as_u64();
        let c = gen.next().as_u64();
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
    }

    #[test]
    fn next_is_unique_under_concurrency() {
        let gen = Arc::new(RequestIdGen::new());
        let n_threads = 8;
        let per_thread = 1000;
        let mut handles = Vec::with_capacity(n_threads);
        for _ in 0..n_threads {
            let g = gen.clone();
            handles.push(thread::spawn(move || {
                let mut ids = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    ids.push(g.next().as_u64());
                }
                ids
            }));
        }
        let mut all = Vec::with_capacity(n_threads * per_thread);
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), n_threads * per_thread, "duplicate request_ids");
    }

    #[test]
    fn request_id_as_u64_round_trips() {
        let id = RequestId(42);
        assert_eq!(id.as_u64(), 42);
    }

    #[test]
    fn request_id_is_copy_and_eq() {
        let a = RequestId(10);
        let b = a;
        assert_eq!(a, b);
    }
}
