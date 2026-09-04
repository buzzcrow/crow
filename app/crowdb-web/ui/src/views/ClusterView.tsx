// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense } from 'react';
import type { Rack, Node, CrowdbKVServerView, EnrichedStoreView, NodeStore, NodeHealth, DiskdbInstanceInfo } from '../types';
import type { MenuTarget } from '../topology/TopologyCanvas';
import type { NodeDiskGroups } from '../data/useClusterTree';

const TopologyCanvas = lazy(() => import('../topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })));

export interface ClusterViewProps {
  racks: Rack[];
  nodes: Node[];
  servers: CrowdbKVServerView[];
  stores: EnrichedStoreView[];
  nodeStores: Record<string, NodeStore[]>;
  nodeHealthById: Record<string, NodeHealth>;
  diskdbNodeIds: Set<number>;
  diskdbInstances: DiskdbInstanceInfo[];
  diskdbInstanceIdByNodeId: Map<number, string>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  refreshToken: number;
  focusRequest: { targetId: string; subtree: boolean; nonce: number } | null;
  onEntityContextMenu: (target: MenuTarget, event: React.MouseEvent) => void;
}

export function ClusterView(props: ClusterViewProps) {
  return (
    <Suspense fallback={<ViewFallback />}>
      <TopologyCanvas {...props} />
    </Suspense>
  );
}

function ViewFallback() {
  return <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">Loading…</div>;
}
