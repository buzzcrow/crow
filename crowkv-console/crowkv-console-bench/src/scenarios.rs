//! Predefined `crowkv bench stress <name>` scenarios.
//!
//! These are opinionated knob bundles meant to make demos and CI runs
//! one-shot reproducible. Each scenario takes the user-supplied
//! `endpoint` and overlays its own thread / connection / workload
//! settings. Operators tune them by editing this file (and committing
//! the change) so reproducibility is captured by `git`.

use std::time::Duration;

use crate::runner::BenchConfig;
use crate::workload::WorkloadKind;

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
