// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for `crowdb_common::config` — `BaseConfig` trait, `load_from_file`,
//! `to_toml`, `log_diff`, and `watch`.

use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use crowdb_common::config::{load_from_file, to_toml, watch, BaseConfig};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use serde::{Deserialize, Serialize};

/// Mock config for testing — two fields, one with serde-skip.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
struct MockConfig {
    #[serde(default)]
    interval_secs: u32,
    #[serde(default)]
    label: String,
    #[serde(skip)]
    runtime_root: PathBuf,
}

impl BaseConfig for MockConfig {
    fn validate(&self) -> Result<(), String> {
        if self.interval_secs == 0 {
            return Err("interval_secs must be > 0".to_string());
        }
        Ok(())
    }

    fn fill_skip_defaults(&mut self) {
        if self.runtime_root.as_os_str().is_empty() {
            self.runtime_root = PathBuf::from("default-root");
        }
    }
}

#[test]
fn load_from_file_valid() {
    let dir = tempdir_in_test_data("config-test");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "interval_secs = 10\nlabel = \"test\"\n").unwrap();
    let config = load_from_file::<MockConfig>(&path).unwrap();
    assert_eq!(config.interval_secs, 10);
    assert_eq!(config.label, "test");
    assert_eq!(config.runtime_root, PathBuf::from("default-root"));
}

#[test]
fn load_from_file_validation_failure() {
    let dir = tempdir_in_test_data("config-test");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "interval_secs = 0\nlabel = \"bad\"\n").unwrap();
    let err = load_from_file::<MockConfig>(&path).unwrap_err();
    assert!(err.contains("interval_secs must be > 0"));
}

#[test]
fn load_from_file_missing_file() {
    let path = PathBuf::from("/nonexistent/config.toml");
    let err = load_from_file::<MockConfig>(&path).unwrap_err();
    assert!(!err.is_empty());
}

#[test]
fn load_from_file_malformed_toml() {
    let dir = tempdir_in_test_data("config-test");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "interval_secs = [invalid\n").unwrap();
    let err = load_from_file::<MockConfig>(&path).unwrap_err();
    assert!(!err.is_empty());
}

#[test]
fn load_from_file_partial_uses_defaults() {
    let dir = tempdir_in_test_data("config-test");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "interval_secs = 5\n").unwrap();
    let config = load_from_file::<MockConfig>(&path).unwrap();
    assert_eq!(config.interval_secs, 5);
    assert_eq!(config.label, "");
    assert_eq!(config.runtime_root, PathBuf::from("default-root"));
}

#[test]
fn to_toml_round_trip() {
    let config = MockConfig {
        interval_secs: 10,
        label: "test".to_string(),
        runtime_root: PathBuf::from("root"),
    };
    let toml_str = to_toml(&config).unwrap();
    let parsed: MockConfig = toml::from_str(&toml_str).unwrap();
    assert_eq!(parsed.interval_secs, config.interval_secs);
    assert_eq!(parsed.label, config.label);
}

#[test]
fn watch_reloads_on_file_change() {
    let dir = tempdir_in_test_data("config-test");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "interval_secs = 10\nlabel = \"v1\"\n").unwrap();

    let (tx, rx) = mpsc::channel::<u32>();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(tx));
    let tx_clone = std::sync::Arc::clone(&tx);

    let _watcher = watch::<MockConfig, _>(&path, move |new| {
        let _ = tx_clone.lock().unwrap().send(new.interval_secs);
    })
    .unwrap();

    // Wait for the watcher to settle, then modify the file.
    std::thread::sleep(Duration::from_secs(1));
    std::fs::write(&path, "interval_secs = 5\nlabel = \"v2\"\n").unwrap();

    // Wait for the debounce (2s) + reload.
    let new_interval = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("reload callback did not fire");
    assert_eq!(new_interval, 5);
}

#[test]
fn watch_skips_invalid_reload() {
    let dir = tempdir_in_test_data("config-test");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "interval_secs = 10\nlabel = \"v1\"\n").unwrap();

    let (tx, rx) = mpsc::channel::<u32>();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(tx));
    let tx_clone = std::sync::Arc::clone(&tx);

    let _watcher = watch::<MockConfig, _>(&path, move |new| {
        let _ = tx_clone.lock().unwrap().send(new.interval_secs);
    })
    .unwrap();

    // Wait for the watcher to settle, then write an invalid config.
    std::thread::sleep(Duration::from_secs(1));
    std::fs::write(&path, "interval_secs = 0\nlabel = \"bad\"\n").unwrap();

    // The callback should NOT fire (validation failure). Wait for
    // the debounce window + a margin, then assert no message.
    std::thread::sleep(Duration::from_secs(5));
    assert!(
        rx.try_recv().is_err(),
        "callback should not fire on invalid config"
    );

    // Now write a valid config — the callback should fire.
    std::fs::write(&path, "interval_secs = 7\nlabel = \"good\"\n").unwrap();
    let new_interval = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("reload callback did not fire after valid config");
    assert_eq!(new_interval, 7);
}
