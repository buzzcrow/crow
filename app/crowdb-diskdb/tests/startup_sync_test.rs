// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup loading ownership and publication tests.

use std::sync::Arc;

use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;

#[test]
fn recovered_group_replaces_only_the_retained_owner_generation() {
    let container = DdbDiskGroupContainer::new(7);
    let initial = Arc::new(DdbDiskGroup::new(100, 10, 1));
    initial.set_bind((1, 1));
    container.replace_disk_group(Arc::clone(&initial));

    let newer = Arc::new(DdbDiskGroup::new(100, 10, 1));
    newer.set_bind((1, 2));
    container.replace_disk_group(Arc::clone(&newer));

    let stale_load = Arc::new(DdbDiskGroup::new(100, 10, 1));
    assert!(!container.replace_disk_group_if_current(&initial, (1, 1), stale_load));
    let retained = container.get_disk_group(100).expect("retained group");
    assert!(Arc::ptr_eq(&retained, &newer));

    let current_load = Arc::new(DdbDiskGroup::new(100, 10, 1));
    current_load.set_bind((1, 2));
    assert!(container.replace_disk_group_if_current(&newer, (1, 2), Arc::clone(&current_load)));
    let retained = container.get_disk_group(100).expect("loaded group");
    assert!(Arc::ptr_eq(&retained, &current_load));
}
