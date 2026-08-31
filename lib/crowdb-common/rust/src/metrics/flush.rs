// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::Write;

use super::registry::{BandwidthEntry, CounterEntry, GaugeEntry, HistogramEntry, SummaryEntry};

fn tps(count: u64, window_secs: f64) -> u64 {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    {
        (count as f64 / window_secs) as u64
    }
}

pub(super) fn flush_counters<W: Write>(
    writer: &mut W,
    entries: &[CounterEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&CounterEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&CounterEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush();
            // Show counters with window activity or a non-zero
            // cumulative total; hide truly idle counters (count=0
            // total=0) to keep the log concise.
            (snap.count > 0 || snap.total > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "total",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.total,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

pub(super) fn flush_histograms<W: Write>(
    writer: &mut W,
    entries: &[HistogramEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&HistogramEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&HistogramEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush();
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "avg(us)",
        "p50(us)",
        "p99(us)",
        "max(us)",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.avg / 1000,
            snap.p50 / 1000,
            snap.p99 / 1000,
            snap.max / 1000,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

pub(super) fn flush_summaries<W: Write>(
    writer: &mut W,
    entries: &[SummaryEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&SummaryEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&SummaryEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush();
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "avg(us)",
        "max(us)",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.avg / 1000,
            snap.max / 1000,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

pub(super) fn flush_bandwidths<W: Write>(
    writer: &mut W,
    entries: &[BandwidthEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&BandwidthEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&BandwidthEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush(window_secs);
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>12}  {:>10}  {:>9}",
        "",
        "count",
        "tps(/s)",
        "avg_size(KB)",
        "rate(KB/s)",
        "total(KB)",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        #[allow(clippy::cast_precision_loss)]
        let avg_kb = snap.avg_size as f64 / 1024.0;
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>12.1}  {:>10}  {:>9}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            avg_kb,
            snap.rate / 1024,
            snap.total_bytes / 1024,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

pub(super) fn flush_gauges<W: Write>(writer: &mut W, entries: &[GaugeEntry], width: usize) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&GaugeEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let non_zero: Vec<&GaugeEntry> = sorted
        .iter()
        .filter(|e| e.handle.snapshot() != 0)
        .copied()
        .collect();
    if non_zero.is_empty() {
        return;
    }
    let _ = writeln!(writer, "{:<width$}  {:>8}", "", "value", width = width);
    for e in &non_zero {
        let val = e.handle.snapshot();
        let name_w = e.name.len().max(width);
        let _ = writeln!(writer, "{:<name_w$}  {:>8}", e.name, val, name_w = name_w);
    }
}
