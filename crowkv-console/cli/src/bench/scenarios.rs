//! Predefined `crowkv bench stress <name>` scenarios.
//!
//! These are opinionated knob bundles meant to make demos and CI runs
//! one-shot reproducible. Each scenario takes the user-supplied
//! `endpoint` and overlays its own thread / connection / workload
//! settings. Operators tune them by editing this file (and committing
//! the change) so reproducibility is captured by `git`.

use std::time::Duration;

use super::runner::BenchConfig;
use super::workload::WorkloadKind;

/// Look up a scenario by name. Returns `None` for unknown names so the
/// CLI can list available scenarios in its error message.
#[must_use]
pub fn stress_scenario(name: &str, endpoint: impl Into<String>) -> Option<BenchConfig> {
    let endpoint = endpoint.into();
    let mut cfg = BenchConfig::defaults(endpoint, WorkloadKind::Mix);
    match name {
        // Quick-fire burst to surface tail latencies on a fresh server.
        "burst" => {
            cfg.workload = WorkloadKind::Mix;
            cfg.threads = 64;
            cfg.connections = 8;
            cfg.duration = Duration::from_secs(10);
            cfg.key_space = 10_000;
            cfg.value_size = 256;
        }
        // Long, gentle write soak to exercise WAL / GC paths.
        "soak" => {
            cfg.workload = WorkloadKind::Write;
            cfg.threads = 8;
            cfg.connections = 4;
            cfg.duration = Duration::from_secs(60);
            cfg.key_space = 100_000;
            cfg.value_size = 1024;
        }
        // Read-heavy hot key set; stresses the per-replica learner read
        // path (V1 read-from-local, see C6 open gap).
        "hotread" => {
            cfg.workload = WorkloadKind::Read;
            cfg.threads = 32;
            cfg.connections = 4;
            cfg.duration = Duration::from_secs(15);
            cfg.key_space = 64;
            cfg.value_size = 64;
        }
        _ => return None,
    }
    cfg.run_id = Some(format!("stress-{name}"));
    Some(cfg)
}

/// Names recognized by `stress_scenario`. Useful for CLI error output.
#[must_use]
pub fn stress_scenario_names() -> &'static [&'static str] {
    &["burst", "soak", "hotread"]
}

/// Apply a `[bench.stress.<name>]` TOML overlay onto an already-built
/// `BenchConfig`. Each `Some` field on the override replaces the
/// corresponding field; `None` leaves it intact.
///
/// Workload parsing reuses [`crate::workload::WorkloadKind::parse`]; an
/// invalid string is reported as `Err(bad_token)`.
///
/// # Errors
/// Returns the unrecognized workload string when `override.workload` is
/// non-empty but not one of `read|write|list|mix`.
pub fn apply_stress_override(
    cfg: &mut BenchConfig,
    ov: &crowkv_console_shared::config::StressScenarioOverride,
) -> std::result::Result<(), String> {
    if let Some(w) = &ov.workload {
        cfg.workload = super::workload::WorkloadKind::parse(w)?;
    }
    if let Some(t) = ov.threads {
        cfg.threads = t;
    }
    if let Some(c) = ov.connections {
        cfg.connections = c;
    }
    if let Some(d) = ov.duration_secs {
        cfg.duration = std::time::Duration::from_secs(d);
    }
    if let Some(k) = ov.key_space {
        cfg.key_space = k;
    }
    if let Some(v) = ov.value_size {
        cfg.value_size = v;
    }
    Ok(())
}

/// Build a `BenchConfig` for `name` by:
///  1. Looking up the built-in default (if any) via [`stress_scenario`].
///  2. Falling back to a generic `BenchConfig::defaults` for unknown names
///     when an override exists for that name.
///  3. Layering the optional override on top.
///
/// Returns `Err` when both the built-in lookup and the override map miss
/// (so the CLI can list available names) or when the override carries
/// an invalid workload string.
///
/// # Errors
/// `Err("unknown")` if no scenario by that name is defined either as a
/// built-in or in the user config; the unrecognized workload token if
/// the override's `workload` field is invalid.
pub fn resolve_stress_scenario(
    name: &str,
    endpoint: impl Into<String>,
    overrides: &std::collections::BTreeMap<String, crowkv_console_shared::config::StressScenarioOverride>,
) -> std::result::Result<BenchConfig, String> {
    let endpoint = endpoint.into();
    let mut cfg = if let Some(c) = stress_scenario(name, endpoint.clone()) {
        c
    } else {
        // No built-in. Only honour the override if the user actually
        // defined one — otherwise the name is genuinely unknown.
        if !overrides.contains_key(name) {
            return Err("unknown".to_string());
        }
        let mut c = BenchConfig::defaults(endpoint, WorkloadKind::Mix);
        c.run_id = Some(format!("stress-{name}"));
        c
    };
    if let Some(ov) = overrides.get(name) {
        apply_stress_override(&mut cfg, ov)?;
    }
    Ok(cfg)
}
