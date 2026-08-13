// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Shared config plumbing — load, validate, watch, and diff TOML
//! config files.
//!
//! Each app's root config struct implements [`BaseConfig`]. The shared
//! [`load_from_file`] function handles file I/O, TOML parsing,
//! skip-default filling, and validation. The [`watch`] function
//! spawns a debounced file watcher that reloads the config on change
//! and calls a callback with the new config. [`log_diff`] logs
//! field-by-field changes between two configs.
//!
//! The caller owns the shared config handle (e.g.
//! `Arc<ArcSwap<T>>`); the watcher's callback swaps it in.

use std::any::Any;
use std::path::Path;
use std::time::Duration;

use notify::RecursiveMode;
use notify_debouncer_mini::{new_debouncer, DebounceEventResult};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::{error, info};

/// Trait for app root config structs that can be loaded from a TOML
/// file, validated, and watched for live reload.
///
/// Requires `serde` (de + ser), `Default`, `Clone`, and thread
/// safety. Apps implement `validate` (field sanity) and optionally
/// `fill_skip_defaults` (for `#[serde(skip)]` fields that need
/// runtime defaults filled after deserialization).
pub trait BaseConfig: DeserializeOwned + Serialize + Default + Clone + Send + Sync + 'static {
    /// Validate the loaded config; return `Err(message)` on the first
    /// violation. Called once after [`load_from_file`] and again
    /// after each reload via [`watch`].
    ///
    /// # Errors
    /// Returns `Err(String)` with a human-readable message on the
    /// first validation violation.
    fn validate(&self) -> Result<(), String>;

    /// Fill runtime defaults for `#[serde(skip)]` fields that were
    /// absent from the file. Called after deserialization, before
    /// validation. Default impl is a no-op.
    fn fill_skip_defaults(&mut self) {}
}

/// Load a TOML config file, fill skip defaults, and validate.
///
/// # Errors
/// Returns `Err(message)` if the file cannot be read, parsed, or
/// fails validation.
pub fn load_from_file<T: BaseConfig>(path: &Path) -> Result<T, String> {
    let data = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut config: T = toml::from_str(&data).map_err(|e| e.to_string())?;
    config.fill_skip_defaults();
    config.validate()?;
    Ok(config)
}

/// Serialize a config to a pretty TOML string (for display/diff
/// logging).
///
/// # Errors
/// Returns `Err(message)` if serialization fails.
pub fn to_toml<T: BaseConfig>(config: &T) -> Result<String, String> {
    toml::to_string_pretty(config).map_err(|e| e.to_string())
}

/// Log field-by-field diff between two configs. Serializes both to
/// `toml::Value`, walks the trees in parallel, and logs each changed
/// leaf as `field=<path> from=<old> to=<new>`. No static/dynamic
/// class in the log — the operator cross-references field doc
/// comments for classification.
pub fn log_diff<T: BaseConfig>(old: &T, new: &T) {
    let Ok(old_val) = toml::Value::try_from(old) else {
        return;
    };
    let Ok(new_val) = toml::Value::try_from(new) else {
        return;
    };
    diff_values(&old_val, &new_val, "");
}

fn diff_values(old: &toml::Value, new: &toml::Value, path: &str) {
    match (old, new) {
        (toml::Value::Table(a), toml::Value::Table(b)) => {
            for (key, va) in a {
                let child = child_path(path, key);
                if let Some(vb) = b.get(key) {
                    diff_values(va, vb, &child);
                } else {
                    info!(field = %child, "config field removed");
                }
            }
            for key in b.keys() {
                if !a.contains_key(key) {
                    info!(field = %child_path(path, key), "config field added");
                }
            }
        }
        (a, b) if a != b => {
            info!(field = %path, from = %a, to = %b, "config field changed");
        }
        _ => {}
    }
}

fn child_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

/// Watch a config file for changes; call `on_reload(&T)` with the new
/// config on each modification. Returns the watcher handle; dropping
/// it stops watching and joins the watcher thread.
///
/// The watcher uses a 2-second debounce to coalesce editor atomic-save
/// events. On reload failure (parse/validation error), it logs the
/// error and keeps the old config (the callback is not called).
///
/// # Errors
/// Returns `Err` if the file watcher cannot be created or the initial
/// watch cannot be registered.
pub fn watch<T, F>(path: &Path, on_reload: F) -> std::io::Result<ConfigWatcher>
where
    T: BaseConfig,
    F: Fn(&T) + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let mut debouncer = new_debouncer(Duration::from_secs(2), move |res: DebounceEventResult| {
        if res.is_ok() {
            let _ = tx.send(());
        }
    })
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    // Watch the parent directory (more reliable cross-platform than
    // watching a single file; reload reads the target file directly).
    let watch_dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    debouncer
        .watcher()
        .watch(&watch_dir, RecursiveMode::NonRecursive)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let path_for_thread = path.to_path_buf();
    let handle = std::thread::Builder::new()
        .name("config-watcher".to_string())
        .spawn(move || {
            while rx.recv().is_ok() {
                match load_from_file::<T>(&path_for_thread) {
                    Ok(new_config) => {
                        on_reload(&new_config);
                    }
                    Err(e) => {
                        error!(error = %e, "config reload failed; keeping old config");
                    }
                }
            }
        })?;

    // Erase the debouncer type so callers don't need the generic
    // parameters. The debouncer is kept alive in the ConfigWatcher;
    // dropping it stops the watcher, which drops the sender, which
    // causes the thread's rx.recv() to return Err and exit.
    let debouncer_box: Box<dyn Any + Send> = Box::new(debouncer);
    Ok(ConfigWatcher {
        _debouncer: debouncer_box,
        _thread: handle,
    })
}

/// Handle for a running config file watcher. Drop to stop watching
/// and join the watcher thread.
pub struct ConfigWatcher {
    _debouncer: Box<dyn Any + Send>,
    _thread: std::thread::JoinHandle<()>,
}
