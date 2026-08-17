// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { Server, HardDrive, Boxes, Activity, RotateCw, Loader2, RefreshCw } from 'lucide-react';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import {
  triggerDiskdbScan,
  recalcDiskdbUsage,
  compactDiskdbZones,
  rebuildDiskdbZoneBitmap,
  setDiskStatus,
} from '../api';
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
  DiskGroupInfoDto,
  DiskInfoDto,
  ZoneUsageDto,
} from '../types';
import { ZoneGrid } from './ZoneGrid';
import { ZoneBitmap } from './ZoneBitmap';
import { ScannerPanel } from './ScannerPanel';
import { RecalcPanel } from './RecalcPanel';

interface CapacityPanelProps {
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  scanStatus: ScanStatusResponse | null;
  loading?: boolean;
  readonly?: boolean;
  onRefresh?: () => Promise<void>;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function busyPct(capacity: number, busy: number): number {
  if (capacity <= 0) return 0;
  return Math.round((busy / capacity) * 100);
}

function diskTypeLabel(t: number): string {
  switch (t) {
    case 0: return 'BlockHdd';
    case 1: return 'BlockSsd';
    case 2: return 'ZoneSsd';
    case 3: return 'SmrHdd';
    default: return `type:${t}`;
  }
}

function hwStatusLabel(s: number): string {
  switch (s) {
    case 0: return 'Unknown';
    case 1: return 'Up';
    case 2: return 'Down';
    case 3: return 'Offline';
    default: return `status:${s}`;
  }
}

export function CapacityPanel({
  instances,
  usage,
  scanStatus,
  loading,
  readonly,
  onRefresh,
}: CapacityPanelProps) {
  const { success, error } = useToast();
  const { log } = useActivity();
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [expandedDg, setExpandedDg] = useState<number | null>(null);
  const refreshRef = useRef(onRefresh);

  // Keep ref in sync without re-triggering the poll effect.
  useEffect(() => { refreshRef.current = onRefresh; }, [onRefresh]);

  // 3s poll for the focused view data (5.7). Retains previous data
  // until new data arrives — no flicker because React only re-renders
  // when the parent passes new props.
  useEffect(() => {
    if (loading) return;
    const id = setInterval(() => { void refreshRef.current?.(); }, 3000);
    return () => clearInterval(id);
  }, [loading]);

  const usageByDgId = useMemo(() => {
    const m = new Map<number, DiskGroupInfoDto>();
    for (const g of usage?.disk_groups || []) {
      m.set(g.disk_group_id, g);
    }
    return m;
  }, [usage]);

  const totalCapacity = useMemo(() => {
    return (usage?.disk_groups || []).reduce((sum, g) => sum + g.capacity_bytes, 0);
  }, [usage]);

  const totalBusy = useMemo(() => {
    return (usage?.disk_groups || []).reduce((sum, g) => sum + g.busy_bytes, 0);
  }, [usage]);

  const totalFree = useMemo(() => {
    return (usage?.disk_groups || []).reduce((sum, g) => sum + g.free_bytes, 0);
  }, [usage]);

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
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      error(`${actionName} failed: ${msg}`);
      log({ action: actionName, target, status: 'Failed', message: msg });
    } finally {
      setActionLoading(null);
    }
  }, [readonly, success, error, log, onRefresh]);

  const handleScan = useCallback((dgId: number | undefined) => {
    runAction(
      `scan-${dgId ?? 'all'}`,
      'Trigger Scan',
      dgId ? `DG-${dgId}` : 'all',
      () => triggerDiskdbScan(dgId),
    );
  }, [runAction]);

  const handleRecalc = useCallback((dgId: number | undefined) => {
    runAction(
      `recalc-${dgId ?? 'all'}`,
      'Recalc Usage',
      dgId ? `DG-${dgId}` : 'all',
      () => recalcDiskdbUsage(dgId),
    );
  }, [runAction]);

  const handleCompact = useCallback((diskId: string) => {
    runAction(
      `compact-${diskId}`,
      'Compact Zones',
      diskId,
      () => compactDiskdbZones(diskId),
    );
  }, [runAction]);

  const handleRebuild = useCallback((diskId: string) => {
    runAction(
      `rebuild-${diskId}`,
      'Rebuild Bitmap',
      diskId,
      () => rebuildDiskdbZoneBitmap(diskId),
    );
  }, [runAction]);

  const handleSetStatus = useCallback((diskId: string, status: string) => {
    runAction(
      `setstatus-${diskId}-${status}`,
      `Set Disk ${status}`,
      diskId,
      () => setDiskStatus(diskId, status),
    );
  }, [runAction]);

  if (loading && instances.length === 0) {
    return (
      <div className="tw-flex tw-items-center tw-justify-center tw-h-full">
        <Loader2 className="tw-h-6 tw-w-6 tw-animate-spin tw-text-muted" />
      </div>
    );
  }

  if (instances.length === 0) {
    return (
      <div className="tw-flex tw-flex-col tw-items-center tw-justify-center tw-h-full tw-text-muted tw-gap-3">
        <Server className="tw-h-12 tw-w-12 tw-opacity-40" />
        <div className="tw-text-lg">No diskdb instances registered</div>
        <div className="tw-text-sm">Deploy a diskdb instance to see capacity data.</div>
      </div>
    );
  }

  return (
    <div className="tw-h-full tw-overflow-auto tw-p-6 tw-space-y-6">
      {/* Summary header */}
      <div className="tw-flex tw-items-center tw-justify-between">
        <div>
          <h2 className="tw-text-xl tw-font-semibold tw-text-text">Capacity Overview</h2>
          <p className="tw-text-sm tw-text-muted tw-mt-1">
            {instances.length} instance(s) · {usage?.disk_groups.length || 0} disk-group(s)
          </p>
        </div>
        <button
          onClick={() => onRefresh?.()}
          className="tw-p-2 tw-rounded-md hover:tw-bg-panel tw-text-muted"
          aria-label="Refresh"
        >
          <RefreshCw className="tw-h-4 tw-w-4" />
        </button>
      </div>

      {/* Cluster-wide totals */}
      <div className="tw-grid tw-grid-cols-3 tw-gap-4">
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-uppercase">Total Capacity</div>
          <div className="tw-text-2xl tw-font-bold tw-text-text tw-mt-1">{formatBytes(totalCapacity)}</div>
        </div>
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-uppercase">Busy</div>
          <div className="tw-text-2xl tw-font-bold tw-text-text tw-mt-1">{formatBytes(totalBusy)}</div>
          <div className="tw-text-xs tw-text-muted tw-mt-1">{busyPct(totalCapacity, totalBusy)}% used</div>
        </div>
        <div className="tw-bg-panel tw-rounded-lg tw-p-4">
          <div className="tw-text-xs tw-text-muted tw-uppercase">Free</div>
          <div className="tw-text-2xl tw-font-bold tw-text-text tw-mt-1">{formatBytes(totalFree)}</div>
          <div className="tw-text-xs tw-text-muted tw-mt-1">{100 - busyPct(totalCapacity, totalBusy)}% free</div>
        </div>
      </div>

      {/* Scanner panel (5.8) */}
      <ScannerPanel
        scanStatus={scanStatus}
        readonly={readonly}
        actionLoading={actionLoading}
        onScan={() => handleScan(undefined)}
      />

      {/* Per-instance detail */}
      {instances.map((inst) => (
        <div key={inst.instance_id} className="tw-bg-panel tw-rounded-lg tw-p-4 tw-space-y-3">
          <div className="tw-flex tw-items-center tw-gap-2">
            <Server className="tw-h-5 tw-w-5 tw-text-muted" />
            <h3 className="tw-text-sm tw-font-semibold tw-text-text">
              diskdb-{inst.instance_id}
            </h3>
            <span className="tw-text-xs tw-text-muted">{inst.grpc_endpoint}</span>
          </div>

          {(inst.owned_dg_ids || []).map((dgId) => {
            const dgUsage = usageByDgId.get(dgId);
            const isExpanded = expandedDg === dgId;
            return (
              <DiskGroupRow
                key={dgId}
                dgId={dgId}
                dgUsage={dgUsage}
                isExpanded={isExpanded}
                onToggle={() => setExpandedDg(isExpanded ? null : dgId)}
                readonly={readonly}
                actionLoading={actionLoading}
                onScan={() => handleScan(dgId)}
                onRecalc={() => handleRecalc(dgId)}
                onCompact={handleCompact}
                onRebuild={handleRebuild}
                onSetStatus={handleSetStatus}
              />
            );
          })}
        </div>
      ))}
    </div>
  );
}

interface DiskGroupRowProps {
  dgId: number;
  dgUsage?: DiskGroupInfoDto;
  isExpanded: boolean;
  onToggle: () => void;
  readonly?: boolean;
  actionLoading: string | null;
  onScan: () => void;
  onRecalc: () => void;
  onCompact: (diskId: string) => void;
  onRebuild: (diskId: string) => void;
  onSetStatus: (diskId: string, status: string) => void;
}

function DiskGroupRow({
  dgId,
  dgUsage,
  isExpanded,
  onToggle,
  readonly,
  actionLoading,
  onScan,
  onRecalc,
  onCompact,
  onRebuild,
  onSetStatus,
}: DiskGroupRowProps) {
  const cap = dgUsage?.capacity_bytes ?? 0;
  const busy = dgUsage?.busy_bytes ?? 0;
  const pct = busyPct(cap, busy);
  const [expandedDisk, setExpandedDisk] = useState<string | null>(null);
  const [selectedZone, setSelectedZone] = useState<ZoneUsageDto | null>(null);

  return (
    <div className="tw-border tw-border-border tw-rounded-md tw-overflow-hidden">
      <div
        className="tw-flex tw-items-center tw-justify-between tw-p-3 tw-cursor-pointer hover:tw-bg-bg/50"
        onClick={onToggle}
      >
        <div className="tw-flex tw-items-center tw-gap-2">
          <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />
          <span className="tw-text-sm tw-font-medium tw-text-text">DG-{dgId}</span>
          <span className="tw-text-xs tw-text-muted">
            {dgUsage?.disks.length || 0} disk(s) · {formatBytes(cap)}
          </span>
        </div>
        <div className="tw-flex tw-items-center tw-gap-3">
          <div className="tw-w-24 tw-h-2 tw-bg-bg tw-rounded-full tw-overflow-hidden">
            <div
              className="tw-h-full tw-bg-accent tw-transition-all"
              style={{ width: `${pct}%` }}
            />
          </div>
          <span className="tw-text-xs tw-text-muted tw-w-12 tw-text-right">{pct}%</span>
        </div>
      </div>

      {isExpanded && (
        <div className="tw-border-t tw-border-border tw-p-3 tw-space-y-3">
          {/* Per-disk colored boxes (5.4) */}
          {(dgUsage?.disks || []).length > 0 && (
            <div className="tw-flex tw-gap-1 tw-flex-wrap">
              {(dgUsage?.disks || []).map((d) => {
                const dpct = busyPct(d.capacity_bytes, d.busy_bytes);
                const color = dpct < 30 ? '#22c55e' : dpct < 60 ? '#eab308' : dpct < 85 ? '#f97316' : '#ef4444';
                return (
                  <div
                    key={d.disk_id}
                    className="tw-w-8 tw-h-8 tw-rounded tw-flex tw-items-center tw-justify-center tw-text-xs tw-text-white tw-font-medium"
                    style={{ backgroundColor: color }}
                    title={`${d.disk_id.slice(0, 12)}… · ${dpct}% busy`}
                  >
                    {dpct}
                  </div>
                );
              })}
            </div>
          )}
          {/* Actions */}
          {!readonly && (
            <div className="tw-flex tw-gap-2">
              <button
                onClick={onScan}
                disabled={actionLoading === `scan-${dgId}`}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-text-xs tw-bg-accent tw-text-white tw-rounded disabled:tw-opacity-50"
              >
                {actionLoading === `scan-${dgId}` ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <Activity className="tw-h-3 tw-w-3" />}
                Scan
              </button>
              <button
                onClick={onRecalc}
                disabled={actionLoading === `recalc-${dgId}`}
                className="tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-text-xs tw-bg-accent tw-text-white tw-rounded disabled:tw-opacity-50"
              >
                {actionLoading === `recalc-${dgId}` ? <Loader2 className="tw-h-3 tw-w-3 tw-animate-spin" /> : <RotateCw className="tw-h-3 tw-w-3" />}
                Recalc
              </button>
            </div>
          )}
          {/* RecalcPanel (5.9) */}
          <RecalcPanel dgId={dgId} readonly={readonly} />
          {/* Per-disk rows with zone grid expansion */}
          {(dgUsage?.disks || []).map((d) => (
            <DiskRow
              key={d.disk_id}
              disk={d}
              readonly={readonly}
              actionLoading={actionLoading}
              onCompact={onCompact}
              onRebuild={onRebuild}
              onSetStatus={onSetStatus}
              isExpanded={expandedDisk === d.disk_id}
              onToggle={() => {
                setExpandedDisk(expandedDisk === d.disk_id ? null : d.disk_id);
                setSelectedZone(null);
              }}
              selectedZone={selectedZone}
              onZoneClick={setSelectedZone}
            />
          ))}
          {(!dgUsage?.disks || dgUsage.disks.length === 0) && (
            <div className="tw-text-xs tw-text-muted tw-py-2">No disks in this group.</div>
          )}
        </div>
      )}
    </div>
  );
}

interface DiskRowProps {
  disk: DiskInfoDto;
  readonly?: boolean;
  actionLoading: string | null;
  onCompact: (diskId: string) => void;
  onRebuild: (diskId: string) => void;
  onSetStatus: (diskId: string, status: string) => void;
  isExpanded: boolean;
  onToggle: () => void;
  selectedZone: ZoneUsageDto | null;
  onZoneClick: (zone: ZoneUsageDto) => void;
}

function DiskRow({
  disk,
  readonly,
  actionLoading,
  onCompact,
  onRebuild,
  onSetStatus,
  isExpanded,
  onToggle,
  selectedZone,
  onZoneClick,
}: DiskRowProps) {
  const pct = busyPct(disk.capacity_bytes, disk.busy_bytes);
  return (
    <div className="tw-border tw-border-border/50 tw-rounded">
      <div
        className="tw-flex tw-items-center tw-justify-between tw-py-2 tw-px-2 tw-rounded hover:tw-bg-bg/30 tw-cursor-pointer"
        onClick={onToggle}
      >
        <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
          <HardDrive className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
          <div className="tw-min-w-0">
            <div className="tw-text-sm tw-text-text tw-truncate">{disk.disk_id}</div>
            <div className="tw-text-xs tw-text-muted">
              {diskTypeLabel(disk.disk_type)} · {hwStatusLabel(disk.status)} · {disk.zone_count} zones · {formatBytes(disk.capacity_bytes)}
            </div>
          </div>
        </div>
        <div className="tw-flex tw-items-center tw-gap-2 tw-shrink-0" onClick={(e) => e.stopPropagation()}>
          <span className="tw-text-xs tw-text-muted tw-w-10 tw-text-right">{pct}%</span>
          {!readonly && (
            <div className="tw-flex tw-gap-1">
              <button
                onClick={() => onCompact(disk.disk_id)}
                disabled={actionLoading?.startsWith(`compact-${disk.disk_id}`)}
                className="tw-px-2 tw-py-0.5 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
                title="Compact zones"
              >
                Compact
              </button>
              <button
                onClick={() => onRebuild(disk.disk_id)}
                disabled={actionLoading?.startsWith(`rebuild-${disk.disk_id}`)}
                className="tw-px-2 tw-py-0.5 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
                title="Rebuild zone bitmap"
              >
                Rebuild
              </button>
              <button
                onClick={() => onSetStatus(disk.disk_id, 'Down')}
                disabled={actionLoading?.startsWith(`setstatus-${disk.disk_id}`)}
                className="tw-px-2 tw-py-0.5 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
                title="Set disk down"
              >
                Down
              </button>
              <button
                onClick={() => onSetStatus(disk.disk_id, 'Up')}
                disabled={actionLoading?.startsWith(`setstatus-${disk.disk_id}`)}
                className="tw-px-2 tw-py-0.5 tw-text-xs tw-bg-panel tw-border tw-border-border tw-rounded hover:tw-bg-bg disabled:tw-opacity-50"
                title="Set disk up"
              >
                Up
              </button>
            </div>
          )}
        </div>
      </div>
      {isExpanded && (
        <div className="tw-border-t tw-border-border/50 tw-p-3 tw-space-y-3">
          {/* Zone grid (5.5) */}
          {disk.zone_usages.length > 0 ? (
            <div>
              <div className="tw-text-xs tw-text-muted tw-mb-1">Zone grid ({disk.zone_usages.length} zones)</div>
              <ZoneGrid zones={disk.zone_usages} onZoneClick={onZoneClick} />
            </div>
          ) : (
            <div className="tw-text-xs tw-text-muted">No zone usage data available.</div>
          )}
          {/* Zone bitmap (5.6) */}
          {selectedZone && (
            <div>
              <div className="tw-text-xs tw-text-muted tw-mb-1">
                Zone {selectedZone.zone_index} bitmap ({selectedZone.busy_block_count} busy / {selectedZone.free_block_count} free blocks)
              </div>
              <ZoneBitmap
                usageBitmap={selectedZone.usage_bitmap}
                totalUnits={selectedZone.busy_block_count + selectedZone.free_block_count}
              />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
