// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Deterministic operation selection for the `DiskDB` mixed benchmark.

use rand::Rng;

/// Select a free attempt for the mixed workload's 70/30 distribution.
pub fn select_free(rng: &mut impl Rng, mixed: bool) -> bool {
    mixed && rng.gen_range(0..100) >= 70
}
