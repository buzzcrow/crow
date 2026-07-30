// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Monotonic-time helpers: a process-wide [`Instant`] anchor sampled
//! lazily on first use, plus saturating conversions to / from
//! milliseconds-since-anchor. Used by lease bookkeeping atomics and
//! the `t_send_ms_mono` heartbeat field.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Process-start monotonic anchor used to encode `Instant` values as a
/// `u64` millisecond offset for atomic lease bookkeeping and for the
/// monotonic `t_send_ms_mono` wire field. Lazily initialized on first
/// call. Same anchor is reused for the lifetime of the process so
/// converted values stay strictly comparable.
pub fn process_anchor() -> Instant {
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    *ANCHOR.get_or_init(Instant::now)
}

/// Convert a monotonic `Instant` to milliseconds since [`process_anchor`].
/// Saturates if `inst` is somehow before the anchor (cannot happen for
/// `Instant::now()` after the anchor was sampled).
#[allow(clippy::cast_possible_truncation)]
#[must_use]
pub fn instant_to_anchor_ms(inst: Instant) -> u64 {
    inst.saturating_duration_since(process_anchor())
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Reverse of [`instant_to_anchor_ms`].
#[must_use]
pub fn anchor_ms_to_instant(ms: u64) -> Instant {
    process_anchor() + Duration::from_millis(ms)
}
