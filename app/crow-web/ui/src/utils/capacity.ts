// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

/** Format bytes as a human-readable string (B, KB, MB, GB, TB, PB). */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

/** Busy percentage, rounded. Returns 0 when capacity is non-positive. */
export function busyPct(capacity: number, busy: number): number {
  if (capacity <= 0) return 0;
  return Math.round((busy / capacity) * 100);
}

/** Green (free) → amber → red (busy) color by busy percentage. */
export function busyColor(pct: number): string {
  if (pct < 30) return '#22c55e';
  if (pct < 60) return '#eab308';
  if (pct < 85) return '#f97316';
  return '#ef4444';
}

/** Disk type label from the numeric proto enum. */
export function diskTypeLabel(t: number): string {
  switch (t) {
    case 0: return 'BlockHdd';
    case 1: return 'BlockSsd';
    case 2: return 'ZoneSsd';
    case 3: return 'SmrHdd';
    default: return `type:${t}`;
  }
}
