// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { FolderTree, Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';

interface ClusterViewProps {
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectRack: (rackId: number) => void;
}

interface RackAgg {
  rackId: number;
  nodeCount: number;
  dgCount: number;
  capacity: number;
  busy: number;
}

export function ClusterView({ usage, hardwareCapacity, onSelectRack }: ClusterViewProps) {
  const racks = useMemo<RackAgg[]>(() => {
    const hwRacks = hardwareCapacity?.racks || [];
    const usageDgs = usage?.disk_groups || [];
    const byRack = new Map<number, RackAgg>();
    for (const r of hwRacks) {
      byRack.set(r.rack_id, {
        rackId: r.rack_id,
        nodeCount: r.node_count,
        dgCount: 0,
        capacity: r.capacity_bytes,
        busy: 0,
      });
    }
    for (const dg of usageDgs) {
      let agg = byRack.get(dg.rack_id);
      if (!agg) {
        agg = { rackId: dg.rack_id, nodeCount: 0, dgCount: 0, capacity: 0, busy: 0 };
        byRack.set(dg.rack_id, agg);
      }
      agg.dgCount += 1;
      agg.busy += dg.busy_bytes;
      if (agg.capacity === 0) agg.capacity += dg.capacity_bytes;
    }
    return Array.from(byRack.values()).sort((a, b) => a.rackId - b.rackId);
  }, [usage, hardwareCapacity]);

  if (racks.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No racks with capacity data.</div>;
  }

  return (
    <div className="tw-space-y-2">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Racks ({racks.length})</div>
      {racks.map((r) => (
        <button
          key={r.rackId}
          onClick={() => onSelectRack(r.rackId)}
          className="tw-w-full tw-flex tw-items-center tw-justify-between tw-p-3 tw-bg-panel tw-rounded-lg hover:tw-bg-bg/50 tw-text-left"
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <FolderTree className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">R-{r.rackId}</div>
              <div className="tw-text-xs tw-text-muted tw-flex tw-items-center tw-gap-1">
                <Boxes className="tw-h-3 tw-w-3" />
                {r.dgCount} DG(s) · {r.nodeCount} node(s)
              </div>
            </div>
          </div>
          <CapacityBar capacity={r.capacity} busy={r.busy} barWidth="tw-w-32" />
        </button>
      ))}
    </div>
  );
}
