// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { lazy, Suspense } from 'react';
import type { Node as ClusterNode, Rack, CrowdbKVServerView, EnrichedStoreView, NodeStore, NodeHealth, DiskdbInstanceInfo, CapacityUsageResponse, HardwareCapacitySummary, ScanStatusResponse } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';
import type { NodeDiskGroups } from '../data/useClusterTree';
import type { CenterPanelMode } from '../shell/Header';

const CapacityPanel = lazy(() => import('../panels/CapacityPanel').then((m) => ({ default: m.CapacityPanel })));
const TopologyCanvas = lazy(() => import('../topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })));

export interface ChunkViewProps {
  centerPanel: CenterPanelMode;
  onCenterPanelChange: (panel: CenterPanelMode) => void;
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  hardwareCapacity: HardwareCapacitySummary | null;
  scanStatus: ScanStatusResponse | null;
  loading: boolean;
  readonly: boolean;
  onRefresh: () => Promise<void>;
  selectedEntity: SelectedEntity | null;
  racks: Rack[];
  nodes: ClusterNode[];
  servers: CrowdbKVServerView[];
  stores: EnrichedStoreView[];
  nodeStores: Record<string, NodeStore[]>;
  nodeHealthById: Record<string, NodeHealth>;
  diskdbNodeIds: Set<number>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  refreshToken: number;
  focusRequest: { targetId: string; subtree: boolean; nonce: number } | null;
  onEntityContextMenu: (target: any, event: React.MouseEvent) => void;
}

export function ChunkView({ centerPanel, onCenterPanelChange, ...props }: ChunkViewProps) {
  return (
    <>
      <div className="tw-flex tw-items-center tw-gap-1 tw-px-4 tw-py-1.5 tw-border-b tw-border-border tw-bg-panel">
        <button
          data-testid="chunk-tab-capacity"
          onClick={() => onCenterPanelChange('capacity')}
          className={`tw-px-3 tw-py-1 tw-text-xs tw-rounded-md tw-transition-colors ${centerPanel === 'capacity' ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg'}`}
          aria-pressed={centerPanel === 'capacity'}
        >
          Capacity
        </button>
        <button
          data-testid="chunk-tab-chunk"
          onClick={() => onCenterPanelChange('chunk')}
          className={`tw-px-3 tw-py-1 tw-text-xs tw-rounded-md tw-transition-colors ${centerPanel === 'chunk' ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg'}`}
          aria-pressed={centerPanel === 'chunk'}
        >
          Chunk
        </button>
      </div>
      <Suspense fallback={<ViewFallback />}>
      {centerPanel === 'capacity' ? (
        <CapacityPanel
          instances={props.instances}
          usage={props.usage}
          hardwareCapacity={props.hardwareCapacity}
          scanStatus={props.scanStatus}
          loading={props.loading}
          readonly={props.readonly}
          onRefresh={props.onRefresh}
          selectedEntity={props.selectedEntity}
        />
      ) : (
        <TopologyCanvas
          racks={props.racks}
          nodes={props.nodes}
          servers={props.servers}
          stores={props.stores}
          nodeStores={props.nodeStores}
          nodeHealthById={props.nodeHealthById}
          diskdbNodeIds={props.diskdbNodeIds}
          diskdbInstances={props.instances}
          nodeDiskGroups={props.nodeDiskGroups}
          refreshToken={props.refreshToken}
          focusRequest={props.focusRequest}
          onEntityContextMenu={props.onEntityContextMenu}
        />
      )}
      </Suspense>
    </>
  );
}

function ViewFallback() {
  return <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">Loading…</div>;
}
