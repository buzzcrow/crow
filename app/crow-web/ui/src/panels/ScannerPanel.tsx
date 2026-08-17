// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { Activity, Loader2 } from 'lucide-react';
import type { ScanStatusResponse, ScanSummaryDto } from '../types';

interface ScannerPanelProps {
  scanStatus: ScanStatusResponse | null;
  readonly?: boolean;
  actionLoading: string | null;
  onScan: () => void;
}

function SummaryRow({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="tw-flex tw-items-center tw-justify-between tw-py-1">
      <span className="tw-text-xs tw-text-muted">{label}</span>
      <span className={`tw-text-sm tw-font-medium ${color || 'tw-text-text'}`}>{value}</span>
    </div>
  );
}

export function ScannerPanel({ scanStatus, readonly, actionLoading, onScan }: ScannerPanelProps) {
  const summary: ScanSummaryDto | undefined = scanStatus?.summary;
  const hasRun = scanStatus?.has_run ?? false;
  const inProgress = scanStatus?.scan_in_progress ?? false;

  return (
    <div className="tw-bg-panel tw-rounded-lg tw-p-4">
      <div className="tw-flex tw-items-center tw-justify-between tw-mb-3">
        <h3 className="tw-text-sm tw-font-semibold tw-text-text">Scanner</h3>
        {!readonly && (
          <button
            onClick={onScan}
            disabled={actionLoading === 'scan-all' || inProgress}
            className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-text-xs tw-bg-accent tw-text-white tw-rounded-md disabled:tw-opacity-50"
          >
            {actionLoading === 'scan-all' || inProgress ? (
              <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" />
            ) : (
              <Activity className="tw-h-3 tw-w-3" />
            )}
            {inProgress ? 'Scanning…' : 'Run Scan'}
          </button>
        )}
      </div>
      {!hasRun && !inProgress && (
        <div className="tw-text-xs tw-text-muted">No scan has been run yet.</div>
      )}
      {inProgress && (
        <div className="tw-text-xs tw-text-accent tw-mb-2">Scan in progress…</div>
      )}
      {summary && (
        <div className="tw-space-y-1">
          <div className="tw-text-xs tw-text-muted tw-uppercase tw-mb-1">Summary</div>
          <div className="tw-grid tw-grid-cols-2 tw-gap-x-4">
            <div>
              <SummaryRow label="Zones scanned" value={summary.zones_scanned} />
              <SummaryRow label="Skipped (active)" value={summary.zones_skipped_active} />
              <SummaryRow label="Skipped (compacting)" value={summary.zones_skipped_compacting} />
            </div>
            <div>
              <SummaryRow label="Ghost busy" value={summary.ghost_busy} color="tw-text-amber-500" />
              <SummaryRow label="Ghost free" value={summary.ghost_free} color="tw-text-amber-500" />
              <SummaryRow label="Uncompacted lag" value={summary.uncompacted_lag} color="tw-text-amber-500" />
            </div>
          </div>
          <div className="tw-border-t tw-border-border tw-mt-2 tw-pt-2">
            <div className="tw-text-xs tw-text-muted tw-uppercase tw-mb-1">Integrity</div>
            <div className="tw-grid tw-grid-cols-2 tw-gap-x-4">
              <SummaryRow label="Corrupt snapshots" value={summary.corrupt_snapshots} color={summary.corrupt_snapshots > 0 ? 'tw-text-failed' : undefined} />
              <SummaryRow label="Corrupt records" value={summary.corrupt_records} color={summary.corrupt_records > 0 ? 'tw-text-failed' : undefined} />
              <SummaryRow label="Owner mismatches" value={summary.owner_mismatches} color={summary.owner_mismatches > 0 ? 'tw-text-failed' : undefined} />
              <SummaryRow label="Leak status" value={0} color="tw-text-text" />
            </div>
            <div className="tw-text-xs tw-text-muted tw-mt-1">Leak: {summary.leak_status}</div>
          </div>
        </div>
      )}
    </div>
  );
}
