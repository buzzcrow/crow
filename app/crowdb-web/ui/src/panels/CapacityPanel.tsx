// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { Server, Loader2, RefreshCw } from 'lucide-react';
import { useToast } from '../contexts/ToastContext';
import { useActivity } from '../contexts/ActivityContext';
import { useSelection } from '../contexts/SelectionContext';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { triggerDiskdbScan } from '../api';
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  HardwareCapacitySummary,
  ScanStatusResponse,
} from '../types';
import { ViewMode } from '../types';
import { busyPct, formatBytes } from '../utils/capacity';
import { ScannerPanel } from './ScannerPanel';
import { ClusterView } from './capacity/ClusterView';
import { RackView } from './capacity/RackView';
import { NodeView } from './capacity/NodeView';
import { DiskGroupView } from './capacity/DiskGroupView';
import { DiskView } from './capacity/DiskView';

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

type CapacityScope = 'Cluster' | 'Rack' | 'Node' | 'DiskGroup' | 'Disk';

function scopeFromEntity(entity: SelectedEntity | null | undefined): CapacityScope {
  if (!entity) return 'Cluster';
  switch (entity.type) {
    case 'Rack': return 'Rack';
    case 'Node': return 'Node';
    case 'DiskGroup': return 'DiskGroup';
    case 'Disk': return 'Disk';
    default: return 'Cluster';
  }
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
  const { selectEntity } = useSelection();
  const { success, error } = useToast();
  const { log } = useActivity();
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const refreshRef = useRef(onRefresh);

  useEffect(() => { refreshRef.current = onRefresh; }, [onRefresh]);

  // 3s poll for the focused view data. Retains previous data until new
  // data arrives — no flicker because React only re-renders when the
  // parent passes new props.
  useEffect(() => {
    if (loading) return;
    const id = setInterval(() => { void refreshRef.current?.(); }, 3000);
    return () => clearInterval(id);
  }, [loading]);

  const scope = scopeFromEntity(selectedEntity);

  // Resolve IDs from the selected entity.
  const rackId = scope === 'Rack' ? Number(selectedEntity?.id) : undefined;
  const nodeId = scope === 'Node' ? Number(selectedEntity?.id) : undefined;
  const dgId = scope === 'DiskGroup'
    ? Number(selectedEntity?.parentIds?.disk_group_id ?? selectedEntity?.id)
    : scope === 'Disk'
      ? Number(selectedEntity?.parentIds?.disk_group_id)
      : undefined;
  const diskId = scope === 'Disk'
    ? String(selectedEntity?.parentIds?.disk_id ?? selectedEntity?.id)
    : undefined;

  // Totals for the header cards, scoped to the selection.
  const { totalCapacity, totalBusy, totalFree } = useMemo(() => {
    const dgs = usage?.disk_groups || [];
    const hwDgs = hardwareCapacity?.disk_groups || [];
    let cap = 0;
    let busy = 0;
    if (scope === 'Disk' && dgId !== undefined && diskId !== undefined) {
      const usageDg = dgs.find((g) => g.disk_group_id === dgId);
      const usageDisk = usageDg?.disks.find((d) => d.disk_id === diskId);
      if (usageDisk) {
        return { totalCapacity: usageDisk.capacity_bytes, totalBusy: usageDisk.busy_bytes, totalFree: usageDisk.free_bytes };
      }
      const hwDg = hwDgs.find((g) => g.disk_group_id === dgId);
      const hwDisk = hwDg?.disks.find((d) => d.disk_id === diskId);
      if (hwDisk) {
        return { totalCapacity: hwDisk.capacity_bytes, totalBusy: 0, totalFree: hwDisk.capacity_bytes };
      }
      return { totalCapacity: 0, totalBusy: 0, totalFree: 0 };
    }
    // Build the DG list the same way as the old filteredDgs: prefer
    // hardwareCapacity (group-0 sysdata) for the DG list + capacity;
    // fall back to usage (diskdb) when hardwareCapacity is not loaded.
    const scopeMatch = (g: { rack_id: number; node_id: number; disk_group_id: number }) =>
      (scope !== 'Rack' || g.rack_id === rackId) &&
      (scope !== 'Node' || g.node_id === nodeId) &&
      (scope !== 'DiskGroup' || g.disk_group_id === dgId);

    if (hwDgs.length > 0) {
      const filtered = hwDgs.filter(scopeMatch);
      cap = filtered.reduce((s, g) => s + g.capacity_bytes, 0);
      busy = dgs.filter(scopeMatch).reduce((s, g) => s + g.busy_bytes, 0);
    } else {
      const filtered = dgs.filter(scopeMatch);
      cap = filtered.reduce((s, g) => s + g.capacity_bytes, 0);
      busy = filtered.reduce((s, g) => s + g.busy_bytes, 0);
    }
    return { totalCapacity: cap, totalBusy: busy, totalFree: Math.max(0, cap - busy) };
  }, [scope, rackId, nodeId, dgId, diskId, usage, hardwareCapacity]);

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

  const handleClusterScan = useCallback(() => {
    if (readonly) return;
    setActionLoading('scan-all');
    triggerDiskdbScan()
      .then(() => {
        success('Trigger Scan succeeded for all');
        log({ action: 'Trigger Scan', target: 'all', status: 'Success' });
        return onRefresh?.();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : 'Unknown error';
        error(`Trigger Scan failed: ${msg}`);
        log({ action: 'Trigger Scan', target: 'all', status: 'Failed', message: msg });
      })
      .finally(() => setActionLoading(null));
  }, [readonly, success, error, log, onRefresh]);

  const selectRack = useCallback((id: number) => {
    selectEntity({ type: 'Rack', id: String(id), viewMode: ViewMode.Capacity });
  }, [selectEntity]);

  const selectNode = useCallback((id: number) => {
    selectEntity({ type: 'Node', id: String(id), viewMode: ViewMode.Capacity });
  }, [selectEntity]);

  const selectDg = useCallback((id: number) => {
    selectEntity({ type: 'DiskGroup', id: String(id), parentIds: { disk_group_id: id }, viewMode: ViewMode.Capacity });
  }, [selectEntity]);

  const selectDisk = useCallback((dId: string, dgIdVal: number, rackIdVal: number, nodeIdVal: number) => {
    selectEntity({
      type: 'Disk',
      id: dId,
      parentIds: { rack_id: rackIdVal, node_id: nodeIdVal, disk_group_id: dgIdVal, disk_id: dId },
      viewMode: ViewMode.Capacity,
    });
  }, [selectEntity]);

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
            {instances.length} instance(s)
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

      {/* Scope totals */}
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

      {/* Scanner panel — cluster scope only (cluster-wide scan status + trigger) */}
      {scope === 'Cluster' && (
        <ScannerPanel
          scanStatus={scanStatus}
          readonly={readonly}
          actionLoading={actionLoading}
          onScan={handleClusterScan}
        />
      )}

      {/* Scope-specific body */}
      {scope === 'Cluster' && (
        <ClusterView
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectRack={selectRack}
        />
      )}
      {scope === 'Rack' && rackId !== undefined && (
        <RackView
          rackId={rackId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectNode={selectNode}
        />
      )}
      {scope === 'Node' && nodeId !== undefined && (
        <NodeView
          nodeId={nodeId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectDg={selectDg}
        />
      )}
      {scope === 'DiskGroup' && dgId !== undefined && (
        <DiskGroupView
          dgId={dgId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          onSelectDisk={selectDisk}
        />
      )}
      {scope === 'Disk' && dgId !== undefined && diskId !== undefined && (
        <DiskView
          dgId={dgId}
          diskId={diskId}
          usage={usage}
          hardwareCapacity={hardwareCapacity ?? null}
          readonly={readonly}
          onRefresh={onRefresh}
        />
      )}
    </div>
  );
}
