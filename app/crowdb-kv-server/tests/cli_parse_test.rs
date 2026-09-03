// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CLI argument parsing tests. Migrated from the inline `#[cfg(test)] mod
//! tests` in `crowdb-kv-server/src/cli.rs` per `.windsurf/workflows/coding.md`
//! §3 ("integration tests only — do not add new inline test modules").

use clap::Parser;
use crowdb_kv_server::cli::{parse_id_list, parse_port_list, Cli};

#[test]
fn parse_single() {
    assert_eq!(parse_id_list("42").unwrap(), vec![42]);
}

#[test]
fn parse_multiple() {
    assert_eq!(parse_id_list("1,2,3").unwrap(), vec![1, 2, 3]);
}

#[test]
fn parse_range() {
    assert_eq!(parse_id_list("10..13").unwrap(), vec![10, 11, 12, 13]);
}

#[test]
fn parse_mixed() {
    assert_eq!(parse_id_list("5,10..13,20").unwrap(), vec![5, 10, 11, 12, 13, 20]);
}

#[test]
fn parse_dedup() {
    assert_eq!(parse_id_list("1,1,2,2..4").unwrap(), vec![1, 2, 3, 4]);
}

#[test]
fn parse_empty_error() {
    assert!(parse_id_list("").is_err());
}

#[test]
fn parse_bad_range() {
    assert!(parse_id_list("10..5").is_err());
}

#[test]
fn parse_port_out_of_range() {
    assert!(parse_port_list("70000").is_err());
}

#[test]
fn parse_port_valid() {
    assert_eq!(parse_port_list("8080,9090").unwrap(), vec![8080, 9090]);
}

#[test]
fn parse_port_rejects_zero() {
    assert!(parse_port_list("0").is_err());
    assert!(parse_port_list("10000,0").is_err());
    assert!(parse_port_list("0..5").is_err());
}

#[test]
fn parse_root_cli_option() {
    let cli = Cli::parse_from([
        "crowdb-kv-server",
        "--root",
        "/data/N-1",
        "--management-port",
        "10000",
    ]);
    assert_eq!(cli.root, std::path::PathBuf::from("/data/N-1"));
    // --config is now optional.
    assert!(cli.config.is_none());
}

#[test]
fn parse_root_is_required() {
    // Omitting --root should fail (clap rejects the invocation).
    let result = Cli::try_parse_from(["crowdb-kv-server", "--management-port", "10000"]);
    assert!(result.is_err());
}

#[test]
fn management_port_rejects_zero() {
    let result = Cli::try_parse_from(["crowdb-kv-server", "--root", "/tmp/n1", "--management-port", "0"]);
    assert!(result.is_err());
}
