// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState } from 'react';
import { RotateCw, Loader2 } from 'lucide-react';
import { recalcDiskdbUsage, rebuildDiskdbZoneBitmap } from '../api';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import type { RecalcResultResponse, ZoneRecalcResultDto } from '../types';

interface RecalcPanelProps {
  dgId: number;
  readonly?: boolean;
}

export function RecalcPanel({ dgId, readonly }: RecalcPanelProps) {
  const [result, setResult] = useState<RecalcResultResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [rebuildLoading, setRebuildLoading] = useState<string | null>(null);
  const { success, error } = useToast();
  const { log } = useActivity();

  const handleRecalc = async () => {
    setLoading(true);
    try {
      const r = await recalcDiskdbUsage(dgId);
      setResult(r);
      success(`Recalc completed for DG-${dgId}`);
      log({ action: 'Recalc Usage', target: `DG-${dgId}`, status: 'Success' });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      error(`Recalc failed: ${msg}`);
      log({ action: 'Recalc Usage', target: `DG-${dgId}`, status: 'Failed', message: msg });
    } finally {
      setLoading(false);
    }
  };

  const handleRebuild = async (zone: ZoneRecalcResultDto) => {
    setRebuildLoading(`${zone.disk_id}-${zone.zone_index}`);
    try {
      await rebuildDiskdbZoneBitmap(zone.disk_id, [zone.zone_index]);
      success(`Rebuild completed for zone ${zone.zone_index} on ${zone.disk_id.slice(0, 8)}…`);
      log({ action: 'Rebuild Bitmap', target: `${zone.disk_id.slice(0, 8)}…/z${zone.zone_index}`, status: 'Success' });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      error(`Rebuild failed: ${msg}`);
      log({ action: 'Rebuild Bitmap', target: `${zone.disk_id.slice(0, 8)}…/z${zone.zone_index}`, status: 'Failed', message: msg });
    } finally {
      setRebuildLoading(null);
    }
  };

  // Collect all drifted zones across disk-groups.
  const driftedZones: ZoneRecalcResultDto[] = [];
  for (const dg of result?.results || []) {
    for (const z of dg.zones) {
      if (z.drift_detected) driftedZones.push(z);
    }
  }

  return (
    <div className="tw-bg-panel tw-rounded-lg tw-p-4">
      <div className="tw-flex tw-items-center tw-justify-between tw-mb-3">
        <h3 className="tw-text-sm tw-font-semibold tw-text-text">Recalc (DG-{dgId})</h3>
        {!readonly && (
          <button
            onClick={handleRecalc}
            disabled={loading}
            className="tw-flex tw-items-center tw-gap-1 tw-px-3 tw-py-1 tw-text-xs tw-bg-accent tw-text-white tw-rounded-md disabled:tw-opacity-50"
          >
            {loading ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <RotateCw className="tw-h-3 tw-w-3" />}
            Run Recalc
          </button>
        )}
      </div>
      {!result && !loading && (
        <div className="tw-text-xs tw-text-muted">Click "Run Recalc" to check for usage drift.</div>
      )}
      {result && driftedZones.length === 0 && (
        <div className="tw-text-xs tw-text-green-500">No drift detected. All zones match.</div>
      )}
      {driftedZones.length > 0 && (
        <div className="tw-space-y-1">
          <div className="tw-text-xs tw-text-muted tw-uppercase tw-mb-1">
            Drifted zones ({driftedZones.length})
          </div>
          <div className="tw-max-h-48 tw-overflow-auto">
            <table className="tw-w-full tw-text-xs">
              <thead>
                <tr className="tw-text-muted tw-border-b tw-border-border">
                  <th className="tw-text-left tw-py-1">Disk</th>
                  <th className="tw-text-right tw-py-1">Zone</th>
                  <th className="tw-text-right tw-py-1">Live</th>
                  <th className="tw-text-right tw-py-1">Replayed</th>
                  <th className="tw-text-right tw-py-1">Action</th>
                </tr>
              </thead>
              <tbody>
                {driftedZones.map((z) => {
                  const key = `${z.disk_id}-${z.zone_index}`;
                  return (
                    <tr key={key} className="tw-border-b tw-border-border/50 tw-bg-amber-500/5">
                      <td className="tw-py-1 tw-text-text tw-font-mono">{z.disk_id.slice(0, 8)}…</td>
                      <td className="tw-py-1 tw-text-right tw-text-text">{z.zone_index}</td>
                      <td className="tw-py-1 tw-text-right tw-text-text">{z.live_busy_blocks}</td>
                      <td className="tw-py-1 tw-text-right tw-text-text">{z.replayed_busy_blocks}</td>
                      <td className="tw-py-1 tw-text-right">
                        {!readonly && (
                          <button
                            onClick={() => handleRebuild(z)}
                            disabled={rebuildLoading === key}
                            className="tw-px-2 tw-py-0.5 tw-text-xs tw-bg-accent tw-text-white tw-rounded disabled:tw-opacity-50"
                          >
                            {rebuildLoading === key ? '…' : 'Rebuild'}
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
