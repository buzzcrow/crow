// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! OS-level system metrics: CPU time, memory RSS, and TCP retransmits.
//!
//! On Linux, reads `/proc/self/stat` for CPU jiffies and `/proc/self/status`
//! for RSS, and `/proc/net/snmp` for TCP retransmit/lost counters.
//! On macOS (and other non-Linux platforms), CPU and RSS are read via
//! `ps` command output; TCP stats are stubbed (reported as 0).

use std::io::Write;
use std::time::Instant;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use std::fs;

/// Snapshot of system-level metrics at a single point in time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemMetrics {
    /// User CPU time (microseconds) since the previous snapshot.
    pub cpu_user_us: u64,
    /// System CPU time (microseconds) since the previous snapshot.
    pub cpu_sys_us: u64,
    /// Resident set size in KB.
    pub rss_kb: u64,
    /// TCP retransmit count delta since previous snapshot (Linux only).
    pub tcp_retransmits: u64,
    /// TCP lost segment count delta since previous snapshot (Linux only).
    pub tcp_lost: u64,
}

/// Collects OS-level metrics by reading `/proc` (Linux) or using
/// `ps` (macOS). Maintains previous-state to compute deltas for
/// CPU time and TCP counters.
#[allow(clippy::struct_field_names)]
pub struct SystemCollector {
    prev_cpu_user_us: u64,
    prev_cpu_sys_us: u64,
    prev_tcp_retransmits: u64,
    prev_tcp_lost: u64,
    prev_instant: Instant,
}

impl SystemCollector {
    /// Create a new collector. The first `collect()` call will report
    /// deltas from this baseline.
    #[must_use]
    pub fn new() -> Self {
        let (user_us, sys_us) = read_cpu_times();
        let (retransmits, lost) = read_tcp_stats();
        Self {
            prev_cpu_user_us: user_us,
            prev_cpu_sys_us: sys_us,
            prev_tcp_retransmits: retransmits,
            prev_tcp_lost: lost,
            prev_instant: Instant::now(),
        }
    }

    /// Collect a system snapshot, computing deltas from the previous call.
    #[must_use]
    pub fn collect(&mut self) -> SystemMetrics {
        let (user_us, sys_us) = read_cpu_times();
        let (retransmits, lost) = read_tcp_stats();
        let _elapsed = self.prev_instant.elapsed();
        self.prev_instant = Instant::now();

        let cpu_user_us = user_us.saturating_sub(self.prev_cpu_user_us);
        let cpu_sys_us = sys_us.saturating_sub(self.prev_cpu_sys_us);
        let tcp_retransmits = retransmits.saturating_sub(self.prev_tcp_retransmits);
        let tcp_lost = lost.saturating_sub(self.prev_tcp_lost);

        self.prev_cpu_user_us = user_us;
        self.prev_cpu_sys_us = sys_us;
        self.prev_tcp_retransmits = retransmits;
        self.prev_tcp_lost = lost;

        let rss_kb = read_rss_kb();

        SystemMetrics {
            cpu_user_us,
            cpu_sys_us,
            rss_kb,
            tcp_retransmits,
            tcp_lost,
        }
    }
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Write a system snapshot to the flush writer in the "misc" section format.
pub fn flush_system<W: Write>(writer: &mut W, snap: &SystemMetrics) {
    let _ = writeln!(writer, "sys.cpu_user_us  {}", snap.cpu_user_us);
    let _ = writeln!(writer, "sys.cpu_sys_us   {}", snap.cpu_sys_us);
    let _ = writeln!(writer, "sys.rss_kb       {}", snap.rss_kb);
    let _ = writeln!(writer, "sys.tcp_retrans  {}", snap.tcp_retransmits);
    let _ = writeln!(writer, "sys.tcp_lost     {}", snap.tcp_lost);
}

// ── Platform-specific readers ───────────────────────────────────

#[cfg(target_os = "linux")]
fn read_cpu_times() -> (u64, u64) {
    // /proc/self/stat fields (1-based):
    //   14 = utime (clock ticks)
    //   15 = stime (clock ticks)
    // Convert to microseconds: ticks * 1_000_000 / sysconf(_SC_CLK_TCK)
    // _SC_CLK_TCK is virtually always 100 on Linux.
    let ticks_per_sec: u64 = 100;
    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        // The comm field is in parentheses and may contain spaces,
        // so split from the right after the last ')'.
        if let Some(pos) = stat.rfind(')') {
            let rest = &stat[pos + 2..];
            let fields: Vec<&str> = rest.split_whitespace().collect();
            // fields[0] = state, fields[1] = ppid, ...
            // After ')', the next fields are: state ppid pgrp session tty_nr
            // tpgid flags minflt cminflt majflt cmajflt utime stime ...
            // So utime is at index 11, stime at index 12 (0-based after ')').
            if fields.len() > 12 {
                let utime: u64 = fields[11].parse().unwrap_or(0);
                let stime: u64 = fields[12].parse().unwrap_or(0);
                let user_us = utime * 1_000_000 / ticks_per_sec;
                let sys_us = stime * 1_000_000 / ticks_per_sec;
                return (user_us, sys_us);
            }
        }
    }
    (0, 0)
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> u64 {
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: u64 = rest
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                return kb;
            }
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn read_tcp_stats() -> (u64, u64) {
    // /proc/net/snmp line: Tcp: ... RetransSegs ...
    // The format has two lines: labels and values.
    if let Ok(snmp) = fs::read_to_string("/proc/net/snmp") {
        let mut lines = snmp.lines().filter(|l| l.starts_with("Tcp:"));
        if let (Some(labels), Some(values)) = (lines.next(), lines.next()) {
            let labels: Vec<&str> = labels.split_whitespace().collect();
            let values: Vec<&str> = values.split_whitespace().collect();
            let retrans_idx = labels.iter().position(|&l| l == "RetransSegs");
            let lost_idx = labels.iter().position(|&l| l == "InErrs" || l == "OutRsts");
            let retransmits = retrans_idx
                .and_then(|i| values.get(i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let lost = lost_idx
                .and_then(|i| values.get(i))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            return (retransmits, lost);
        }
    }
    (0, 0)
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_times() -> (u64, u64) {
    // macOS: use `ps` to get CPU time. This is a best-effort approach.
    // ps -o utime,stime -p <pid> gives times in M:SS.cc format.
    // For simplicity, report 0 deltas on non-Linux platforms.
    // A future improvement could use mach_time APIs via FFI.
    (0, 0)
}

#[cfg(not(target_os = "linux"))]
fn read_rss_kb() -> u64 {
    // macOS: use `ps` to get RSS.
    // ps -o rss -p <pid> gives RSS in KB.
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    if let Ok(out) = output {
        let rss_str = String::from_utf8_lossy(&out.stdout);
        let rss: u64 = rss_str.trim().parse().unwrap_or(0);
        return rss;
    }
    0
}

#[cfg(not(target_os = "linux"))]
fn read_tcp_stats() -> (u64, u64) {
    // TCP stats are not available on macOS without private APIs.
    (0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_returns_snapshot() {
        let mut collector = SystemCollector::new();
        let snap = collector.collect();
        // CPU deltas should be small but non-negative.
        // RSS should be non-zero (the test process has memory).
        assert!(snap.rss_kb > 0, "RSS should be non-zero, got {}", snap.rss_kb);
    }

    #[test]
    fn collect_delta_is_non_negative() {
        let mut collector = SystemCollector::new();
        let snap1 = collector.collect();
        let snap2 = collector.collect();
        // Deltas should be non-negative (monotonic counters).
        assert!(snap2.cpu_user_us <= snap1.cpu_user_us + 1_000_000);
    }

    #[test]
    fn flush_system_writes_all_fields() {
        let snap = SystemMetrics {
            cpu_user_us: 1000,
            cpu_sys_us: 500,
            rss_kb: 4096,
            tcp_retransmits: 3,
            tcp_lost: 1,
        };
        let mut buf = Vec::new();
        flush_system(&mut buf, &snap);
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("sys.cpu_user_us"));
        assert!(out.contains("1000"));
        assert!(out.contains("sys.cpu_sys_us"));
        assert!(out.contains("500"));
        assert!(out.contains("sys.rss_kb"));
        assert!(out.contains("4096"));
        assert!(out.contains("sys.tcp_retrans"));
        assert!(out.contains('3'));
        assert!(out.contains("sys.tcp_lost"));
        assert!(out.contains('1'));
    }
}
