// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo } from 'react';
import { Server, Boxes } from 'lucide-react';
import type {
  CapacityUsageResponse,
  HardwareCapacitySummary,
} from '../../types';
import { CapacityBar } from './CapacityBar';

interface RackViewProps {
  rackId: number;
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  onSelectNode: (nodeId: number) => void;
}

interface NodeAgg {
  nodeId: number;
  dgCount: number;
  capacity: number;
  busy: number;
}

export function RackView({ rackId, usage, hardwareCapacity, onSelectNode }: RackViewProps) {
  const nodes = useMemo<NodeAgg[]>(() => {
    const hwNodes = (hardwareCapacity?.nodes || []).filter((n) => n.rack_id === rackId);
    const usageDgs = (usage?.disk_groups || []).filter((g) => g.rack_id === rackId);
    const byNode = new Map<number, NodeAgg>();
    for (const n of hwNodes) {
      byNode.set(n.node_id, {
        nodeId: n.node_id,
        dgCount: n.disk_group_count,
        capacity: n.capacity_bytes,
        busy: 0,
      });
    }
    for (const dg of usageDgs) {
      let agg = byNode.get(dg.node_id);
      if (!agg) {
        agg = { nodeId: dg.node_id, dgCount: 0, capacity: 0, busy: 0 };
        byNode.set(dg.node_id, agg);
      }
      agg.dgCount += 1;
      agg.busy += dg.busy_bytes;
      if (agg.capacity === 0) agg.capacity += dg.capacity_bytes;
    }
    return Array.from(byNode.values()).sort((a, b) => a.nodeId - b.nodeId);
  }, [rackId, usage, hardwareCapacity]);

  if (nodes.length === 0) {
    return <div className="tw-text-sm tw-text-muted">No nodes in rack {rackId}.</div>;
  }

  return (
    <div className="tw-space-y-2">
      <div className="tw-text-xs tw-text-muted tw-uppercase">Nodes in R-{rackId} ({nodes.length})</div>
      {nodes.map((n) => (
        <button
          key={n.nodeId}
          onClick={() => onSelectNode(n.nodeId)}
          className="tw-w-full tw-flex tw-items-center tw-justify-between tw-p-3 tw-bg-panel tw-rounded-lg hover:tw-bg-bg/50 tw-text-left"
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-flex-1 tw-min-w-0">
            <Server className="tw-h-4 tw-w-4 tw-text-muted tw-shrink-0" />
            <div className="tw-min-w-0">
              <div className="tw-text-sm tw-text-text">N-{n.nodeId}</div>
              <div className="tw-text-xs tw-text-muted tw-flex tw-items-center tw-gap-1">
                <Boxes className="tw-h-3 tw-w-3" />
                {n.dgCount} DG(s)
              </div>
            </div>
          </div>
          <CapacityBar capacity={n.capacity} busy={n.busy} barWidth="tw-w-32" />
        </button>
      ))}
    </div>
  );
}
