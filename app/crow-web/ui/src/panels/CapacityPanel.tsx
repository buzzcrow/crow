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
  HardwareCapacitySummary,
  ScanStatusResponse,
  DiskGroupInfoDto,
  DiskInfoDto,
  ZoneUsageDto,
} from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { ZoneGrid } from './ZoneGrid';
import { ZoneBitmap } from './ZoneBitmap';
import { hwStatusLabel as sharedHwStatusLabel } from '../utils/entityDisplay';
import { ScannerPanel } from './ScannerPanel';
import { RecalcPanel } from './RecalcPanel';

interface CapacityPanelProps {
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  hardwareCapacity?: HardwareCapacitySummary | null;
  scanStatus: ScanStatusResponse | null;
  loading?: boolean;
  readonly?: boolean;
  onRefresh?: () => Promise<void>;
  selectedEntity?: SelectedEntity | null;
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
  return sharedHwStatusLabel(s);
}

export function CapacityPanel({
  instances,
  usage,
  hardwareCapacity,
  scanStatus,
  loading,
  readonly,
  onRefresh,
  selectedEntity,
}: CapacityPanelProps) {
  const { success, error } = useToast();
  const { log } = useActivity();
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [expandedDg, setExpandedDg] = useState<number | null>(null);
  const [expandedDiskId, setExpandedDiskId] = useState<string | null>(null);
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

  // Auto-expand DG and disk when selected from the sidebar.
  useEffect(() => {
    if (!selectedEntity) return;
    if (selectedEntity.type === 'DiskGroup') {
      const dgId = Number(selectedEntity.parentIds?.disk_group_id ?? selectedEntity.id);
      setExpandedDg(dgId);
    } else if (selectedEntity.type === 'Disk') {
      const dgId = Number(selectedEntity.parentIds?.disk_group_id);
      const diskId = String(selectedEntity.parentIds?.disk_id ?? selectedEntity.id);
      if (dgId) setExpandedDg(dgId);
      setExpandedDiskId(diskId);
    }
  }, [selectedEntity]);

  const usageByDgId = useMemo(() => {
    const m = new Map<number, DiskGroupInfoDto>();
    for (const g of usage?.disk_groups || []) {
      m.set(g.disk_group_id, g);
    }
    return m;
  }, [usage]);

  // Filter disk-groups based on the selected layer (rack/node/DG/disk).
  // Uses hardwareCapacity (group-0 sysdata) for the DG list and
  // capacity; falls back to usage (diskdb) if hardwareCapacity is
  // not yet loaded.
  const filteredDgs = useMemo(() => {
    const hwDgs = hardwareCapacity?.disk_groups || [];
    const allDgs = hwDgs.length > 0
      ? hwDgs.map((g) => ({
          rack_id: g.rack_id,
          node_id: g.node_id,
          disk_group_id: g.disk_group_id,
          status: g.status,
          disk_ids: g.disks.map((d) => d.disk_id),
          disks: [],
          capacity_bytes: g.capacity_bytes,
          busy_bytes: 0,
          free_bytes: g.capacity_bytes,
          allocatable_disk_count: 0,
        } as DiskGroupInfoDto))
      : (usage?.disk_groups || []);
    if (!selectedEntity) return allDgs;
    if (selectedEntity.type === 'Rack') {
      const rackId = Number(selectedEntity.id);
      return allDgs.filter((g) => g.rack_id === rackId);
    }
    if (selectedEntity.type === 'Node') {
      const nodeId = Number(selectedEntity.id);
      return allDgs.filter((g) => g.node_id === nodeId);
    }
    if (selectedEntity.type === 'DiskGroup') {
      const dgId = Number(selectedEntity.parentIds?.disk_group_id ?? selectedEntity.id);
      return allDgs.filter((g) => g.disk_group_id === dgId);
    }
    if (selectedEntity.type === 'Disk') {
      const dgId = Number(selectedEntity.parentIds?.disk_group_id);
      return allDgs.filter((g) => g.disk_group_id === dgId);
    }
    return allDgs;
  }, [hardwareCapacity, usage, selectedEntity]);

  // When a single disk is selected, show only that disk's capacity.
  const selectedDiskCap = useMemo(() => {
    if (selectedEntity?.type !== 'Disk') return null;
    const dgId = Number(selectedEntity.parentIds?.disk_group_id);
    const diskId = String(selectedEntity.parentIds?.disk_id ?? selectedEntity.id);
    const hwDg = hardwareCapacity?.disk_groups.find((g) => g.disk_group_id === dgId);
    const hwDisk = hwDg?.disks.find((d) => d.disk_id === diskId);
    if (hwDisk) {
      const usageDg = usage?.disk_groups.find((g) => g.disk_group_id === dgId);
      const usageDisk = usageDg?.disks.find((d) => d.disk_id === diskId);
      return {
        capacity: hwDisk.capacity_bytes,
        busy: usageDisk?.busy_bytes ?? 0,
        free: usageDisk?.free_bytes ?? hwDisk.capacity_bytes,
      };
    }
    // Fall back to diskdb usage if hardware sysdata not loaded.
    const usageDg = usage?.disk_groups.find((g) => g.disk_group_id === dgId);
    const usageDisk = usageDg?.disks.find((d) => d.disk_id === diskId);
    if (usageDisk) {
      return {
        capacity: usageDisk.capacity_bytes,
        busy: usageDisk.busy_bytes,
        free: usageDisk.free_bytes,
      };
    }
    return null;
  }, [selectedEntity, hardwareCapacity, usage]);

  const totalCapacity = useMemo(() => {
    if (selectedDiskCap) return selectedDiskCap.capacity;
    return filteredDgs.reduce((sum, g) => sum + g.capacity_bytes, 0);
  }, [filteredDgs, selectedDiskCap]);

  const totalBusy = useMemo(() => {
    if (selectedDiskCap) return selectedDiskCap.busy;
    const usageDgs = usage?.disk_groups || [];
    return filteredDgs.reduce((sum, g) => {
      const u = usageDgs.find((ud) => ud.disk_group_id === g.disk_group_id);
      return sum + (u?.busy_bytes ?? 0);
    }, 0);
  }, [filteredDgs, usage, selectedDiskCap]);

  const totalFree = useMemo(() => {
    if (selectedDiskCap) return selectedDiskCap.free;
    return Math.max(0, totalCapacity - totalBusy);
  }, [totalCapacity, totalBusy, selectedDiskCap]);

  // Build a summary title based on the selected layer.
  const scopeLabel = useMemo(() => {
    if (!selectedEntity) return 'Cluster';
    switch (selectedEntity.type) {
      case 'Rack': return `Rack ${selectedEntity.id}`;
      case 'Node': return `Node ${selectedEntity.id}`;
      case 'DiskGroup': return `DG-${selectedEntity.parentIds?.disk_group_id ?? selectedEntity.id}`;
      case 'Disk': return `Disk ${String(selectedEntity.parentIds?.disk_id ?? selectedEntity.id).slice(0, 12)}…`;
      default: return 'Cluster';
    }
  }, [selectedEntity]);

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
          <h2 className="tw-text-xl tw-font-semibold tw-text-text">Capacity — {scopeLabel}</h2>
          <p className="tw-text-sm tw-text-muted tw-mt-1">
            {instances.length} instance(s) · {filteredDgs.length} disk-group(s)
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

          {(inst.owned_dg_ids || []).filter((dgId) => filteredDgs.some((g) => g.disk_group_id === dgId)).map((dgId) => {
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
                expandedDiskId={expandedDiskId}
                onDiskExpand={setExpandedDiskId}
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
  expandedDiskId?: string | null;
  onDiskExpand?: (diskId: string | null) => void;
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
  expandedDiskId,
  onDiskExpand,
}: DiskGroupRowProps) {
  const cap = dgUsage?.capacity_bytes ?? 0;
  const busy = dgUsage?.busy_bytes ?? 0;
  const pct = busyPct(cap, busy);
  const [localExpandedDisk, setLocalExpandedDisk] = useState<string | null>(null);
  const [selectedZone, setSelectedZone] = useState<ZoneUsageDto | null>(null);

  // Use controlled expandedDiskId if provided, otherwise local state.
  const expandedDisk = expandedDiskId !== undefined ? expandedDiskId : localExpandedDisk;
  const setExpandedDisk = onDiskExpand ?? setLocalExpandedDisk;

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
