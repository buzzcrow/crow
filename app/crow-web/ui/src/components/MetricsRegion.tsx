// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import type { MetricsResponse, MetricPoint, ElectionState, ReadState } from '../types';

/** Format a nanosecond value as a human-readable latency string. */
function fmtNs(ns: number): string {
  if (ns === 0) return '0';
  if (ns < 1_000) return `${ns}ns`;
  if (ns < 1_000_000) return `${(ns / 1_000).toFixed(1)}µs`;
  if (ns < 1_000_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`;
  return `${(ns / 1_000_000_000).toFixed(2)}s`;
}

/** Format a byte count as a human-readable size string. */
function fmtBytes(b: number): string {
  if (b < 1_024) return `${b}B`;
  if (b < 1_024 * 1_024) return `${(b / 1_024).toFixed(1)}KB`;
  if (b < 1_024 * 1_024 * 1_024) return `${(b / (1_024 * 1_024)).toFixed(1)}MB`;
  return `${(b / (1_024 * 1_024 * 1_024)).toFixed(2)}GB`;
}

/** Extract a numeric field from a metric point by key. */
function field(p: MetricPoint, key: string): number | undefined {
  const f = p.fields.find((f) => f.key === key);
  return f?.value;
}

/** Render a single metric point as a label/value row. */
function MetricRow({ p }: { p: MetricPoint }) {
  let value: string;
  switch (p.kind) {
    case 'counter': {
      const count = field(p, 'count') ?? 0;
      const tps = field(p, 'tps') ?? 0;
      const total = field(p, 'total') ?? 0;
      value = `${count} (${tps.toFixed(1)}/s, total ${total})`;
      break;
    }
    case 'gauge': {
      value = String(field(p, 'value') ?? 0);
      break;
    }
    case 'bandwidth': {
      const count = field(p, 'count') ?? 0;
      const avg = field(p, 'avg_size') ?? 0;
      const rate = field(p, 'rate') ?? 0;
      value = `${count} ops, avg ${fmtBytes(avg)}, ${fmtBytes(rate)}/s`;
      break;
    }
    case 'histogram': {
      const count = field(p, 'count') ?? 0;
      const p50 = field(p, 'p50_ns') ?? 0;
      const p99 = field(p, 'p99_ns') ?? 0;
      value = `${count} ops, p50 ${fmtNs(p50)}, p99 ${fmtNs(p99)}`;
      break;
    }
    case 'summary': {
      const count = field(p, 'count') ?? 0;
      const avg = field(p, 'avg_ns') ?? 0;
      const max = field(p, 'max_ns') ?? 0;
      value = `${count} ops, avg ${fmtNs(avg)}, max ${fmtNs(max)}`;
      break;
    }
    default:
      value = p.fields.map((f) => `${f.key}=${f.value}`).join(', ');
  }
  // Strip the metric prefix for display brevity.
  const shortName = p.name.replace(/^s\.\d+\.(g\.\d+\.)?/, '');
  return (
    <div className="tw-flex tw-items-start tw-justify-between tw-px-3 tw-py-1.5 tw-text-xs tw-gap-2">
      <dt className="tw-text-muted tw-flex-shrink-0 tw-truncate" title={p.name}>
        {shortName}
      </dt>
      <dd className="tw-font-mono tw-text-text tw-text-right tw-select-text tw-whitespace-nowrap">{value}</dd>
    </div>
  );
}

/** Collapsible metrics region for the Inspector Details tab. */
export function MetricsRegion({ data }: { data: MetricsResponse | null }) {
  if (!data || data.metrics.length === 0) {
    return (
      <Section title="Metrics">
        <p className="tw-px-3 tw-py-2 tw-text-xs tw-text-muted">No metrics available.</p>
      </Section>
    );
  }
  return (
    <Section title={`Metrics (window ${data.window_secs.toFixed(0)}s)`}>
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {data.metrics.map((p) => (
          <MetricRow key={p.name} p={p} />
        ))}
      </dl>
    </Section>
  );
}

/** Election state region. */
export function ElectionStateRegion({ state }: { state: ElectionState }) {
  const rows: { label: string; value: string }[] = [
    { label: 'Term', value: String(state.current_term) },
    { label: 'Elections', value: String(state.election_count) },
    ...(state.last_heartbeat_age_ms != null
      ? [{ label: 'Heartbeat Age', value: `${state.last_heartbeat_age_ms}ms` }]
      : []),
    ...(state.lease_remaining_ms != null
      ? [{ label: 'Lease Remaining', value: `${state.lease_remaining_ms}ms` }]
      : []),
    { label: 'Phase 1 In-Flight', value: String(state.bulk_phase1_in_flight_slots) },
    { label: 'Step-downs (higher term)', value: String(state.step_downs_higher_term) },
    { label: 'Step-downs (lease)', value: String(state.step_downs_lease_unrenewable) },
    { label: 'Step-downs (admin)', value: String(state.step_downs_admin) },
  ];
  return (
    <Section title="Election State">
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {rows.map((r) => (
          <div
            key={r.label}
            className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-text-xs tw-gap-2"
          >
            <dt className="tw-text-muted tw-flex-shrink-0">{r.label}</dt>
            <dd className="tw-font-mono tw-text-text tw-text-right tw-select-text">{r.value}</dd>
          </div>
        ))}
      </dl>
    </Section>
  );
}

/** Read-path state region. */
export function ReadStateRegion({ state }: { state: ReadState }) {
  const rows: { label: string; value: string }[] = [
    { label: 'Lease Valid', value: state.lease_valid ? 'Yes' : 'No' },
    { label: 'Contiguous Applied', value: String(state.contiguous_applied) },
    { label: 'Safe Slot', value: String(state.safe_slot) },
  ];
  return (
    <Section title="Read State">
      <dl className="tw-divide-y tw-divide-border tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
        {rows.map((r) => (
          <div
            key={r.label}
            className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-text-xs tw-gap-2"
          >
            <dt className="tw-text-muted tw-flex-shrink-0">{r.label}</dt>
            <dd className="tw-font-mono tw-text-text tw-text-right tw-select-text">{r.value}</dd>
          </div>
        ))}
      </dl>
    </Section>
  );
}

/** Section wrapper with a heading. */
function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="tw-space-y-1">
      <h4 className="tw-text-[10px] tw-uppercase tw-tracking-wider tw-text-muted tw-px-1">{title}</h4>
      {children}
    </div>
  );
}
