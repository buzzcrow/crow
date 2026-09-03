// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Construction tests for [`CrowdbSysmdClient`]. Delegation is trivial
//! (each method forwards to the wrapped sub-client), so we only verify
//! that the facade builds from both `new` and `from_shared` and that
//! all three sub-clients share the same underlying `Arc<CrowdbKvClient>`.

use std::sync::Arc;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};

#[test]
fn sysmd_client_new_shares_single_arc() {
    let kv = CrowdbKvClient::new(ClientConfig::new(vec!["http://127.0.0.1:1".into()]));
    let sysmd = CrowdbSysmdClient::new(kv);
    // All sub-clients wrap the same Arc; kv() returns a reference to
    // the shared inner client.
    let _ = sysmd.kv();
    // shared_kv returns a clone of the Arc — calling it twice gives
    // two Arc handles to the same allocation.
    let a1 = sysmd.shared_kv();
    let a2 = sysmd.shared_kv();
    assert!(Arc::ptr_eq(&a1, &a2));
}

#[test]
fn sysmd_client_from_shared_preserves_arc() {
    let kv = CrowdbKvClient::new(ClientConfig::new(vec!["http://127.0.0.1:1".into()]));
    let shared = Arc::new(kv);
    let original = Arc::clone(&shared);
    let sysmd = CrowdbSysmdClient::from_shared(shared);
    // The facade's shared_kv must point to the same allocation as the
    // original Arc passed in.
    assert!(Arc::ptr_eq(&original, &sysmd.shared_kv()));
}

#[test]
fn sysmd_client_is_clone() {
    let kv = CrowdbKvClient::new(ClientConfig::new(vec!["http://127.0.0.1:1".into()]));
    let sysmd = CrowdbSysmdClient::new(kv);
    let cloned = sysmd.clone();
    // Clone shares the same underlying Arc.
    assert!(Arc::ptr_eq(&sysmd.shared_kv(), &cloned.shared_kv()));
}
