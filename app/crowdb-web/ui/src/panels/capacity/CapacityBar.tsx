// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { busyPct, formatBytes } from '../../utils/capacity';

interface CapacityBarProps {
  capacity: number;
  busy: number;
  /** Width of the bar in tailwind units (e.g. 'tw-w-24'). */
  barWidth?: string;
  /** Show the numeric percentage label. */
  showPct?: boolean;
}

/** Compact capacity bar + percentage label. */
export function CapacityBar({ capacity, busy, barWidth = 'tw-w-24', showPct = true }: CapacityBarProps) {
  const pct = busyPct(capacity, busy);
  return (
    <div className="tw-flex tw-items-center tw-gap-3">
      <div className={`${barWidth} tw-h-2 tw-bg-bg tw-rounded-full tw-overflow-hidden`}>
        <div
          className="tw-h-full tw-bg-accent tw-transition-all"
          style={{ width: `${pct}%` }}
        />
      </div>
      {showPct && <span className="tw-text-xs tw-text-muted tw-w-12 tw-text-right">{pct}%</span>}
      <span className="tw-text-xs tw-text-muted">{formatBytes(busy)} / {formatBytes(capacity)}</span>
    </div>
  );
}
