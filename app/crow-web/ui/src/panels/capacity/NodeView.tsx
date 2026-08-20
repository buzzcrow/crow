// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';

interface NodeViewProps {
  nodeId: number;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectDg: (dgId: number) => void;
}

interface DgAgg {
  dgId: number;
  diskCount: number;
  capacity: number;
  busy: number;
}

export function NodeView({ nodeId, usage, hardwareCapacity, onSelectDg }: NodeViewProps) {
  const dgs = useMemo<DgAgg[]>(() => {
    const hwDgs = (hardwareCapacity?.disk_groups || []).filter((g) => g.node_id === nodeId);
    const usageDgs = (usage?.disk_groups || []).filter((g) => g.node_id === nodeId);
    const byDg = new Map<number, DgAgg>();
    for (const g of hwDgs) {
      byDg.set(g.disk_group_id, {
        dgId: g.disk_group_id,
        diskCount: g.disks.length,
        capacity: g.capacity_bytes,
        busy: 0,
      });
    }
    for (const g of usageDgs) {
      let agg = byDg.get(g.disk_group_id);
      if (!agg) {
        agg = { dgId: g.disk_group_id, diskCount: g.disks.length, capacity: 0, busy: 0 };
        byDg.set(g.disk_group_id, agg);
      }
      agg.busy += g.busy_bytes;
      if (agg.capacity === 0) agg.capacity += g.capacity_bytes;
      if (agg.diskCount === 0) agg.diskCount = g.disks.length;
    }
    return Array.from(byDg.values()).sort((a, b) => a.dgId - b.dgId);
  }, [nodeId, usage, hardwareCapacity]);

  if (dgs.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No disk-groups on node {nodeId}.</div>;
  }

  return (
    <div className="tw-space-y-2">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Disk-groups on N-{nodeId} ({dgs.length})</div>
      {dgs.map((d) => (
        <button
          key={d.dgId}
          onClick={() => onSelectDg(d.dgId)}
          className="tw-w-full tw-flex tw-items-center tw-justify-between tw-p-3 tw-bg-panel tw-rounded-lg hover:tw-bg-bg/50 tw-text-left"
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <Boxes className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">DG-{d.dgId}</div>
              <div className="tw-text-xs tw-text-muted">{d.diskCount} disk(s)</div>
            </div>
          </div>
          <CapacityBar capacity={d.capacity} busy={d.busy} barWidth="tw-w-32" />
        </button>
      ))}
    </div>
  );
}
