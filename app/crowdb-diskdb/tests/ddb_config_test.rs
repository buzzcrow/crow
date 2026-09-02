// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Config validation + `load_from_file` tests.

use crowdb_common::config::BaseConfig;
use crowdb_diskdb::ddb_config::{validate, DdbConfig};

#[test]
fn config_validate_accepts_default() {
    let config = DdbConfig::default();
    validate(&config).expect("default config should be valid");
}

#[test]
fn config_validate_rejects_non_power_of_two_block_size() {
    let mut config = DdbConfig::default();
    config.storage.block_size_bytes = 700 * 1024;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_block_size_out_of_range() {
    let mut config = DdbConfig::default();
    config.storage.block_size_bytes = 256 * 1024;
    assert!(validate(&config).is_err());

    let mut config = DdbConfig::default();
    config.storage.block_size_bytes = 4 * 1024 * 1024;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_zone_not_multiple_of_block() {
    let mut config = DdbConfig::default();
    config.storage.zone_size_bytes = 16 * 1024 * 1024 * 1024 + 1;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_granularity_not_equal_to_block() {
    let mut config = DdbConfig::default();
    config.storage.allocate_granularity = 512 * 1024;
    config.storage.block_size_bytes = 1024 * 1024;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_bad_listen_addr() {
    let mut config = DdbConfig::default();
    config.server.listen_addr = "not-an-addr".to_string();
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_zero_sync_interval() {
    let mut config = DdbConfig::default();
    config.sync.sync_interval_secs = 0;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_zero_zone_rotate_count() {
    let mut config = DdbConfig::default();
    config.storage.zone_rotate_count = 0;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_zero_cas_retry_limit() {
    let mut config = DdbConfig::default();
    config.storage.cas_retry_limit = 0;
    assert!(validate(&config).is_err());
}

#[test]
fn config_defaults_match_design() {
    let config = DdbConfig::default();
    assert_eq!(config.storage.cas_retry_limit, 100);
    assert!(!config.storage.validate_owner_on_free);
    assert!(!config.persistence.free_batch_enabled);
    assert_eq!(config.persistence.free_flush_max_batch, 256);
}

#[test]
fn config_load_from_file_roundtrip() {
    let config = DdbConfig::default();
    let toml = toml::to_string_pretty(&config).expect("serialize");
    let tmp = crowdb_test_harness::test_dirs::test_data_dir().join("ddb_config_test.toml");
    std::fs::write(&tmp, &toml).expect("write temp");
    let loaded = crowdb_common::config::load_from_file::<DdbConfig>(&tmp).expect("load");
    assert_eq!(loaded.storage.block_size_bytes, config.storage.block_size_bytes);
    assert_eq!(loaded.server.listen_addr, config.server.listen_addr);
    let _ = std::fs::remove_file(&tmp);
}

/// A minimal `[server]`-only config must parse and fill the remaining
/// sections from `Default` — this is the shape the console's
/// `resolve_diskdb_config_path` fallback generates. Guards against a
/// future required field on a sub-struct silently breaking that path.
#[test]
fn minimal_server_only_config_uses_section_defaults() {
    let toml = "[server]\n\
         listen_addr = \"0.0.0.0:11000\"\n\
         http_listen_addr = \"0.0.0.0:11100\"\n\
         rpc_listen_addr = \"0.0.0.0:11200\"\n\
         kv_server_mgmt_seeds = [\"http://127.0.0.1:10000\"]\n";
    let config: DdbConfig = toml::from_str(toml).expect("minimal config parses");
    config.validate().expect("minimal config validates");
    // Defaults filled in for the omitted sections.
    assert_eq!(config.storage.block_size_bytes, 1024 * 1024);
    assert_eq!(config.heartbeat.interval_secs, 10);
    assert!(config.scanner.ghost.detect);
    assert_eq!(config.sync.sync_interval_secs, 10);
}
