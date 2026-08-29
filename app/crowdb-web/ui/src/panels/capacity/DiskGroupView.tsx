// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
  DiskInfoDto,
} from '../../types';
import { busyPct, busyColor, formatBytes, diskTypeLabel } from '../../utils/capacity';
import { hwStatusLabel as sharedHwStatusLabel } from '../../utils/entityDisplay';

interface DiskGroupViewProps {
  dgId: number;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectDisk: (diskId: string, dgId: number, rackId: number, nodeId: number) => void;
}

export function DiskGroupView({ dgId, usage, hardwareCapacity, onSelectDisk }: DiskGroupViewProps) {
  const disks = useMemo<DiskInfoDto[]>(() => {
    const usageDg = usage?.disk_groups.find((g) => g.disk_group_id === dgId);
    if (usageDg) return usageDg.disks;
    const hwDg = hardwareCapacity?.disk_groups.find((g) => g.disk_group_id === dgId);
    if (!hwDg) return [];
    // No usage data — synthesize brief disk entries from hardware sysdata.
    return hwDg.disks.map((d) => ({
      rack_id: hwDg.rack_id,
      node_id: hwDg.node_id,
      disk_group_id: dgId,
      disk_id: d.disk_id,
      disk_type: d.disk_type,
      capacity_units: 0,
      zone_size_units: 0,
      unit_size_bytes: d.unit_size_bytes,
      zone_count: d.zone_count,
      status: d.status,
      busy_units: 0,
      free_units: 0,
      capacity_bytes: d.capacity_bytes,
      busy_bytes: 0,
      free_bytes: d.capacity_bytes,
      active_zone_count: 0,
      zone_usages: [],
    }));
  }, [dgId, usage, hardwareCapacity]);

  if (disks.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No disks in DG-{dgId}.</div>;
  }

  const rackId = disks[0]?.rack_id ?? 0;
  const nodeId = disks[0]?.node_id ?? 0;

  return (
    <div className="tw-space-y-3">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Disks in DG-{dgId} ({disks.length})</div>
      <div className="tw-flex tw-gap-2 tw-flex-wrap">
        {disks.map((d) => {
          const dpct = busyPct(d.capacity_bytes, d.busy_bytes);
          const color = busyColor(dpct);
          return (
            <button
              key={d.disk_id}
              onClick={() => onSelectDisk(d.disk_id, dgId, rackId, nodeId)}
              className="tw-flex tw-flex-col tw-items-center tw-gap-1 tw-p-2 tw-rounded-lg hover:tw-bg-bg/50"
              title={`${d.disk_id.slice(0, 12)}… · ${diskTypeLabel(d.disk_type)} · ${dpct}% busy · ${formatBytes(d.capacity_bytes)}`}
            >
              <div
                className="tw-w-12 tw-h-12 tw-rounded tw-flex tw-items-center tw-justify-center tw-text-sm tw-text-white tw-font-medium"
                style={{ backgroundColor: color }}
              >
                {dpct}
              </div>
              <div className="tw-text-xs tw-text-muted tw-font-mono">{d.disk_id.slice(0, 8)}…</div>
              <div className="tw-text-xs tw-text-muted">{hwStatusLabel(d.status)}</div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function hwStatusLabel(s: number): string {
  return sharedHwStatusLabel(s);
}
