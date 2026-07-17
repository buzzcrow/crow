// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `bench benchmark` and `bench compare` (R10).

mod testkit;

use std::path::PathBuf;

use crowkv_console_shared::lifecycle;
use testkit::console::{crowkv_cli_bin, run};

fn unique_run_id(tag: &str) -> String {
    format!(
        "test-bench-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
}

fn project_root() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut dir: &std::path::Path = &cwd;
    loop {
        if dir.join("pixi.toml").exists() {
            return dir.to_path_buf();
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return cwd,
        }
    }
}

fn bench_runs_root() -> PathBuf {
    project_root().join("bench-runs")
}

/// Create a test run directory under `bench-runs/<tag>/` and return its path.
fn make_test_run_dir(tag: &str) -> PathBuf {
    let dir = bench_runs_root().join(tag);
    std::fs::create_dir_all(&dir).expect("create test run dir");
    dir
}

/// Find a run directory by partial tag match (case-insensitive).
fn find_run_dir(tag: &str) -> Option<PathBuf> {
    let root = bench_runs_root();
    let Ok(entries) = std::fs::read_dir(&root) else {
        return None;
    };
    let tag_lower = tag.to_ascii_lowercase();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.to_ascii_lowercase().contains(&tag_lower) {
            return Some(entry.path());
        }
    }
    None
}

fn cleanup_run_dir(tag: &str) {
    if let Some(dir) = find_run_dir(tag) {
        let _ = std::fs::remove_dir_all(&dir);
    }
}

fn check_binaries() -> Option<PathBuf> {
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        return None;
    }
    if lifecycle::crowkv_server_bin().is_none() {
        eprintln!("skipping: crowkv-server binary not found");
        return None;
    }
    Some(cli)
}

/// Write a minimal valid Markdown report to `bench-runs/<run_id>/report.md`
/// so `bench compare` can read it without running a full benchmark.
#[allow(clippy::cast_precision_loss, reason = "display-only throughput")]
fn write_minimal_report(run_id: &str, total_ops: u64) {
    let dir = make_test_run_dir(run_id);
    let half = total_ops / 2;
    let qps = total_ops as f64;
    let md = format!(
        "\
# Benchmark Report: {run_id}

- **Started**: 2026-01-01 00:00:00 UTC
- **Finished**: 2026-01-01 00:00:01 UTC
- **Duration**: 1000 ms (measurement)
- **Warmup**: 0 ms (discarded)
- **Workload**: Mix
- **Target**: `127.0.0.1:28001` (store=1, group=1)

## Configuration

| Parameter | Value |
|---|---|
| connections | 2 |
| threads | 2 |
| key_space | 100 |
| value_size | 64 B |

## Summary

| Metric | Value |
|---|---|
| total_ops | {total_ops} |
| throughput | {qps:.1} ops/s |
| total_errors | 0 |
| error_rate | 0.0000 |

## Per-Operation Latency

| op | ops | errors | not_found | avg(us) | p50(us) | p90(us) | p99(us) | p999(us) | max(us) |
|---|---|---|---|---|---|---|---|---|---|
| write | {half} | 0 | 0 | 50 | 48 | 80 | 90 | 95 | 100 |
| read | {half} | 0 | 0 | 20 | 19 | 30 | 35 | 38 | 40 |

## Server Metrics

| Metric | Value |
|---|---|
| wal_append | {half} |
| kv_put | {half} |
| kv_get | {half} |

### System

| Metric | Value |
|---|---|
| cpu_user | 1000 us |
| cpu_sys | 200 us |
| rss | 51200 KB |
| tcp_retransmits | 0 |
| tcp_lost | 0 |
",
    );
    let path = dir.join("report.md");
    std::fs::write(&path, md).unwrap();
}

/// `bench benchmark --mode memory --duration 3s` runs end-to-end, writes a
/// report with server-side metrics, and cleans up the workspace.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_memory_e2e() {
    let Some(cli) = check_binaries() else { return };
    let run_id = unique_run_id("e2e");

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        9920,
        &[
            "bench",
            "benchmark",
            "--mode",
            "memory",
            "--duration-secs",
            "3",
            "--threads",
            "2",
            "--connections",
            "2",
            "--run-id",
            run_id.as_str(),
        ],
    );

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("report:"), "stdout={stdout}");
    assert!(stdout.contains("anomalies:"), "stdout={stdout}");

    // Extract the report path from stdout ("report: <path>").
    let report_line = stdout
        .lines()
        .find(|l| l.contains("report:"))
        .expect("report line in stdout");
    let report_path_str = report_line.split("report:").nth(1).unwrap().trim();
    let report_path = PathBuf::from(report_path_str);
    let content = std::fs::read_to_string(&report_path).expect("report file");
    assert!(content.contains("# Benchmark Report:"), "content={content}");
    assert!(content.contains("| total_ops |"), "content={content}");
    assert!(content.contains("wal_append"), "content={content}");

    // Workspace is inside the run dir; with --keep-workspace not set,
    // the workspace subdir should be removed but the run dir remains.
    let run_dir = report_path.parent().expect("run dir");
    assert!(
        !run_dir.join("workspace").exists(),
        "workspace should be removed after run",
    );

    cleanup_run_dir(run_dir.file_name().unwrap().to_string_lossy().as_ref());
}

/// `--keep-workspace` retains the deploy workspace after the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_keep_workspace() {
    let Some(cli) = check_binaries() else { return };
    let run_id = unique_run_id("keep");

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        9920,
        &[
            "bench",
            "benchmark",
            "--mode",
            "memory",
            "--duration-secs",
            "1",
            "--threads",
            "2",
            "--connections",
            "2",
            "--run-id",
            run_id.as_str(),
            "--keep-workspace",
        ],
    );

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");

    // Extract the report path from stdout to find the run dir.
    let report_line = stdout
        .lines()
        .find(|l| l.contains("report:"))
        .expect("report line in stdout");
    let report_path_str = report_line.split("report:").nth(1).unwrap().trim();
    let run_dir = PathBuf::from(report_path_str)
        .parent()
        .expect("run dir")
        .to_path_buf();

    assert!(
        run_dir.join("workspace").exists(),
        "workspace should be retained with --keep-workspace",
    );

    cleanup_run_dir(run_dir.file_name().unwrap().to_string_lossy().as_ref());
}

/// `bench compare <tag1> <tag2>` prints a side-by-side comparison table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_compare_two_runs() {
    let Some(cli) = check_binaries() else { return };
    let run_id_1 = unique_run_id("cmp-a");
    let run_id_2 = unique_run_id("cmp-b");

    write_minimal_report(&run_id_1, 1000);
    write_minimal_report(&run_id_2, 2000);

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        9920,
        &["bench", "compare", run_id_1.as_str(), run_id_2.as_str()],
    );

    assert_eq!(code, 0, "compare stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("throughput"), "stdout={stdout}");
    assert!(stdout.contains(&run_id_1), "stdout={stdout}");
    assert!(stdout.contains(&run_id_2), "stdout={stdout}");

    cleanup_run_dir(&run_id_1);
    cleanup_run_dir(&run_id_2);
}
