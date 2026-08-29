// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useMemo, useCallback } from 'react';
import { HardDrive, Activity, RotateCw, Loader2 } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
  DiskInfoDto,
} from '../../types';
import {
  triggerDiskdbScan,
  recalcDiskdbUsage,
  compactDiskdbZones,
  rebuildDiskdbZoneBitmap,
  setDiskStatus,
} from '../../api';
import { useToast } from '../../contexts/ToastContext';
import { useActivity } from '../../contexts/ActivityContext';
import { useZoneBitmap } from '../../data/useZoneBitmap';
import { ZoneGrid } from '../ZoneGrid';
import { ZoneBitmap } from '../ZoneBitmap';
import { RecalcPanel } from '../RecalcPanel';
import { busyPct, formatBytes, diskTypeLabel } from '../../utils/capacity';
import { hwStatusLabel as sharedHwStatusLabel } from '../../utils/entityDisplay';

interface DiskViewProps {
  dgId: number;
  diskId: string;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  readonly?: boolean;
  onRefresh?: () => Promise<void>;
}

export function DiskView({
  dgId,
  diskId,
  usage,
  hardwareCapacity,
  readonly,
  onRefresh,
}: DiskViewProps) {
  const { success, error } = useToast();
  const { log } = useActivity();
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [selectedZoneIndex, setSelectedZoneIndex] = useState<number | null>(null);

  const disk = useMemo<DiskInfoDto | null>(() => {
    const usageDg = usage?.disk_groups.find((g) => g.disk_group_id === dgId);
    const usageDisk = usageDg?.disks.find((d) => d.disk_id === diskId);
    if (usageDisk) return usageDisk;
    const hwDg = hardwareCapacity?.disk_groups.find((g) => g.disk_group_id === dgId);
    const hwDisk = hwDg?.disks.find((d) => d.disk_id === diskId);
    if (!hwDg || !hwDisk) return null;
    return {
      rack_id: hwDg.rack_id,
      node_id: hwDg.node_id,
      disk_group_id: dgId,
      disk_id: hwDisk.disk_id,
      disk_type: hwDisk.disk_type,
      capacity_units: 0,
      zone_size_units: 0,
      unit_size_bytes: hwDisk.unit_size_bytes,
      zone_count: hwDisk.zone_count,
      status: hwDisk.status,
      busy_units: 0,
      free_units: 0,
      capacity_bytes: hwDisk.capacity_bytes,
      busy_bytes: 0,
      free_bytes: hwDisk.capacity_bytes,
      active_zone_count: 0,
      zone_usages: [],
    };
  }, [dgId, diskId, usage, hardwareCapacity]);

  const { zone: bitmapZone, loading: bitmapLoading, error: bitmapError, refresh: refreshBitmap } = useZoneBitmap(
    dgId,
    diskId,
    selectedZoneIndex,
  );

  const runAction = useCallback(async (
    actionId: string,
    actionName: string,
    target: string,
    fn: () => Promise<unknown>,
  ) => {
    if (readonly) return;
    setActionLoading(actionId);
    try {
      await fn();
      success(`${actionName} succeeded for ${target}`);
      log({ action: actionName, target, status: 'Success' });
      await onRefresh?.();
      if (actionId.startsWith('scan-') || actionId.startsWith('recalc-')) {
        await refreshBitmap();
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      error(`${actionName} failed: ${msg}`);
      log({ action: actionName, target, status: 'Failed', message: msg });
    } finally {
      setActionLoading(null);
    }
  }, [readonly, success, error, log, onRefresh, refreshBitmap]);

  const handleScan = useCallback(() => {
    runAction(`scan-${dgId}`, 'Trigger Scan', `DG-${dgId}`, () => triggerDiskdbScan(dgId));
  }, [runAction, dgId]);

  const handleRecalc = useCallback(() => {
    runAction(`recalc-${dgId}`, 'Recalc Usage', `DG-${dgId}`, () => recalcDiskdbUsage(dgId));
  }, [runAction, dgId]);

  const handleCompact = useCallback(() => {
    runAction(`compact-${diskId}`, 'Compact Zones', diskId, () => compactDiskdbZones(diskId));
  }, [runAction, diskId]);

  const handleRebuild = useCallback(() => {
    runAction(`rebuild-${diskId}`, 'Rebuild Bitmap', diskId, () => rebuildDiskdbZoneBitmap(diskId));
  }, [runAction, diskId]);

  const handleSetStatus = useCallback((status: string) => {
    runAction(`setstatus-${diskId}-${status}`, `Set Disk ${status}`, diskId, () => setDiskStatus(diskId, status));
  }, [runAction, diskId]);

  if (!disk) {
    return <div className="tw-text-sm tw-text-muted">Disk {diskId.slice(0, 12)}… not found in DG-{dgId}.</div>;
  }

  const pct = busyPct(disk.capacity_bytes, disk.busy_bytes);
  const zoneCount = disk.zone_usages.length;

  return (
    <div className="tw-space-y-4">
      {/* Disk header */}
      <div className="tw-bg-panel tw-rounded-lg tw-p-4">
        <div className="tw-flex tw-items-center tw-gap-2 tw-mb-2">
          <HardDrive className="tw-h-5 tw-w-5 tw-text-muted" />
          <div className="tw-text-sm tw-font-mono tw-text-text">{disk.disk_id}</div>
        </div>
        <div className="tw-text-xs tw-text-muted tw-mb-3">
          {diskTypeLabel(disk.disk_type)} · {sharedHwStatusLabel(disk.status)} · {disk.zone_count} zones · {formatBytes(disk.capacity_bytes)} · {pct}% busy
        </div>
        {!readonly && (
          <div className="tw-flex tw-gap-2 tw-flex-wrap">
            <button
              onClick={handleScan}
              disabled={actionLoading === `scan-${dgId}`}
              className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-text-xs tw-bg-accent tw-text-white tw-rounded disabled:tw-opacity-50"
            >
              {actionLoading === `scan-${dgId}` ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Activity className="tw-h-3 tw-w-3" />}
              Scan
            </button>
            <button
              onClick={handleRecalc}
              disabled={actionLoading === `recalc-${dgId}`}
              className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-text-xs tw-bg-accent tw-text-white tw-rounded disabled:tw-opacity-50"
            >
              {actionLoading === `recalc-${dgId}` ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <RotateCw className="tw-h-3 tw-w-3" />}
              Recalc
            </button>
            <button
              onClick={handleCompact}
              disabled={actionLoading?.startsWith(`compact-${diskId}`)}
              className="tw-px-2 tw-py-1 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
            >
              {actionLoading?.startsWith(`compact-${diskId}`) ? '…' : 'Compact'}
            </button>
            <button
              onClick={handleRebuild}
              disabled={actionLoading?.startsWith(`rebuild-${diskId}`)}
              className="tw-px-2 tw-py-1 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
            >
              {actionLoading?.startsWith(`rebuild-${diskId}`) ? '…' : 'Rebuild'}
            </button>
            <button
              onClick={() => handleSetStatus('Down')}
              disabled={actionLoading?.startsWith(`setstatus-${diskId}`)}
              className="tw-px-2 tw-py-1 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
            >
              Down
            </button>
            <button
              onClick={() => handleSetStatus('Up')}
              disabled={actionLoading?.startsWith(`setstatus-${diskId}`)}
              className="tw-px-2 tw-py-1 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
            >
              Up
            </button>
          </div>
        )}
      </div>

      {/* RecalcPanel (per-DG, scoped to this disk's parent DG) */}
      <RecalcPanel dgId={dgId} readonly={readonly} />

      {/* Zone grid */}
      {zoneCount > 0 ? (
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-mb-2">Zone grid ({zoneCount} zones)</div>
          <ZoneGrid
            zones={disk.zone_usages}
            onZoneClick={(z) => setSelectedZoneIndex(z.zone_index)}
          />
        </div>
      ) : (
        <div className="tw-bg-panel tw-rounded-lg tw-p-4 tw-text-xs tw-text-muted">
          No zone usage data available.
        </div>
      )}

      {/* Zone bitmap (on-demand) */}
      {selectedZoneIndex !== null && (
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-mb-2">
            Zone {selectedZoneIndex} bitmap
            {bitmapLoading && ' · loading…'}
            {bitmapError && ` · error: ${bitmapError.message}`}
            {bitmapZone && ` · ${bitmapZone.busy_block_count} busy / ${bitmapZone.free_block_count} free blocks`}
          </div>
          {bitmapZone && (
            <ZoneBitmap
              usageBitmap={bitmapZone.usage_bitmap}
              totalUnits={bitmapZone.busy_block_count + bitmapZone.free_block_count}
            />
          )}
          {!bitmapLoading && !bitmapZone && !bitmapError && (
            <div className="tw-text-xs tw-text-muted">No bitmap data.</div>
          )}
        </div>
      )}
    </div>
  );
}
