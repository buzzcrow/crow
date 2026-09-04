// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../src/commands/bench/diskdb_workload.rs"]
mod diskdb_workload;

use std::collections::HashSet;

use diskdb_workload::select_free;
use rand::rngs::SmallRng;
use rand::SeedableRng;

#[test]
fn deterministic_mixed_selection_preserves_live_set_accounting() {
    let mut rng = SmallRng::seed_from_u64(1);
    let mut live = HashSet::new();
    let mut next_id = 0u64;
    let mut allocated = 0u64;
    let mut freed = 0u64;
    let mut free_attempts = 0u64;

    for _ in 0..10_000 {
        if select_free(&mut rng, true) {
            free_attempts += 1;
            if let Some(id) = live.iter().next().copied() {
                assert!(live.remove(&id));
                freed += 1;
            }
        } else {
            assert!(live.insert(next_id));
            next_id += 1;
            allocated += 1;
        }
    }

    assert_eq!(free_attempts, 3_008);
    assert_eq!(allocated, 6_992);
    assert_eq!(freed, 3_007);
    assert_eq!(u64::try_from(live.len()).unwrap(), allocated - freed);

    let mut allocate_only_rng = SmallRng::seed_from_u64(1);
    assert!((0..10_000).all(|_| !select_free(&mut allocate_only_rng, false)));
}
