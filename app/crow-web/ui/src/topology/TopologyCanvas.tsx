// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useMemo, useCallback, useEffect, useRef } from 'react';
import ReactFlow, {
  Background,
  ReactFlowProvider,
  Node,
  NodeMouseHandler,
  Viewport,
  Edge,
  useReactFlow,
  useNodesInitialized,
} from 'reactflow';
import { ScanSearch } from 'lucide-react';
import 'reactflow/dist/style.css';
import { useViewMode } from '../contexts/ViewModeContext';
import { useSelection, SelectedEntity } from '../contexts/SelectionContext';
import { Rack, Node as NodeEntity, StoreView, NodeStore, ViewMode, CrowKVServerView, NodeHealth } from '../types';
import { buildFlowForViewMode, FlowNodeData } from './buildFlow';
import { layoutTree } from './layout';
import { CrowKVNode } from './CrowKVNode';
import { Button } from '../components/ui/Button';

export interface MenuTarget {
  type: SelectedEntity['type'];
  id: string;
  rawId?: string | number;
  parentIds?: Record<string, string | number>;
  label?: string;
}

interface TopologyCanvasProps {
  racks: Rack[];
  nodes: NodeEntity[];
  servers: CrowKVServerView[];
  stores: StoreView[];
  nodeStores?: Record<string, NodeStore[]>;
  nodeHealthById?: Record<string, NodeHealth>;
  refreshToken?: number;
  focusRequest?: { targetId: string; subtree: boolean; nonce: number } | null;
  /** Right-click on a canvas node. */
  onEntityContextMenu?: (target: MenuTarget, event: React.MouseEvent) => void;
}

const NODE_TYPES = { crowKv: CrowKVNode };

function descendantIds(rootId: string, edges: Edge[]): Set<string> {
  const childrenByParent = new Map<string, string[]>();
  for (const edge of edges) {
    const children = childrenByParent.get(edge.source) ?? [];
    children.push(edge.target);
    childrenByParent.set(edge.source, children);
  }

  const ids = new Set<string>();
  const visit = (nodeId: string) => {
    if (ids.has(nodeId)) return;
    ids.add(nodeId);
    for (const childId of childrenByParent.get(nodeId) ?? []) visit(childId);
  };
  visit(rootId);
  return ids;
}

export function TopologyCanvas(props: TopologyCanvasProps) {
  return (
    <ReactFlowProvider>
      <TopologyCanvasInner {...props} />
    </ReactFlowProvider>
  );
}

/** Mirror the node-id scheme used in buildFlow so the current selection can
 * be highlighted on the canvas. */
function selectedNodeId(entity: SelectedEntity): string | null {
  const p = entity.parentIds || {};
  if (entity.viewMode === ViewMode.Physical) {
    switch (entity.type) {
      case 'Rack': return `R-${entity.id}`;
      case 'Node': return `N-${entity.id}`;
      case 'Server': return p.node_id ? `KV-${p.node_id}` : null;
      case 'Store': return p.node_id ? `S-${p.node_id}-${entity.id}` : null;
      case 'Group':
        return p.node_id && p.store_id ? `G-${p.node_id}-${p.store_id}-${entity.id}` : null;
      case 'Replica':
        if (p.remote_on && p.store_id && p.group_id) return `RR-${p.remote_on}-${p.store_id}-${p.group_id}-${entity.id}`;
        return p.node_id && p.store_id && p.group_id ? `LR-${p.node_id}-${p.store_id}-${p.group_id}-${entity.id}` : null;
      default: return null;
    }
  }
  switch (entity.type) {
    case 'Store': return `S-${entity.id}`;
    case 'Group': return p.store_id ? `G-${p.store_id}-${entity.id}` : null;
    case 'Replica':
      return p.store_id && p.group_id ? `LR-${p.store_id}-${p.group_id}-${entity.id}` : null;
    default: return null;
  }
}

function TopologyCanvasInner({ racks, nodes, servers, stores, nodeStores, nodeHealthById, refreshToken, focusRequest, onEntityContextMenu }: TopologyCanvasProps) {
  const { viewMode } = useViewMode();
  const { selectedEntity, selectEntity } = useSelection();
  const { fitView, setViewport, setCenter, getZoom, getNodes } = useReactFlow();
  const nodesInitialized = useNodesInitialized();
  const viewportsRef = useRef<Partial<Record<ViewMode, Viewport>>>({});
  const fittedOnceRef = useRef<Partial<Record<ViewMode, boolean>>>({});
  const lastRefreshTokenRef = useRef<number | undefined>(refreshToken);
  const lastFocusNonceRef = useRef<number | undefined>(undefined);
  const nodeIdsKeyRef = useRef<Partial<Record<ViewMode, string>>>({});
  // Tracks the last (viewMode, nodeIds, refreshToken) triple that triggered
  // a fit/restore. Polls return new array references for the same data, which
  // would otherwise re-run the effect every cycle and fight user panning.
  const lastActionKeyRef = useRef<string | undefined>(undefined);

  const { nodes: rawNodes, edges } = useMemo(
    () => buildFlowForViewMode(viewMode, racks, nodes, servers, stores, nodeStores, nodeHealthById),
    [viewMode, racks, nodes, servers, stores, nodeStores, nodeHealthById],
  );

  const positioned = useMemo(() => layoutTree(rawNodes, edges), [rawNodes, edges]);

  useEffect(() => {
    if (refreshToken !== lastRefreshTokenRef.current) {
      lastRefreshTokenRef.current = refreshToken;
      viewportsRef.current = {};
      fittedOnceRef.current = {};
    }
  }, [refreshToken]);

  useEffect(() => {
    if (positioned.nodes.length === 0 || !nodesInitialized) {
      fittedOnceRef.current[viewMode] = false;
      return;
    }
    // Re-fit when the set of node IDs changes (nodes added/removed),
    // whether from a mutation or a poll update — not just on manual refresh.
    const nodeIdsKey = positioned.nodes.map((n) => n.id).sort().join(',');
    // Only act on meaningful changes: view-mode switch, node-set change, or
    // manual refresh. Polls return new array references for the same data;
    // without this guard the effect re-runs every cycle and the
    // setViewport(savedViewport) call below fights an in-progress pan drag.
    const actionKey = `${viewMode}:${nodeIdsKey}:${refreshToken ?? ''}`;
    if (actionKey === lastActionKeyRef.current) return;
    lastActionKeyRef.current = actionKey;
    if (nodeIdsKey !== nodeIdsKeyRef.current[viewMode]) {
      nodeIdsKeyRef.current[viewMode] = nodeIdsKey;
      viewportsRef.current[viewMode] = undefined;
      fittedOnceRef.current[viewMode] = false;
    }
    // Retry fit across frames until ReactFlow has measured every node's
    // dimensions — newly added nodes lack width/height on the first frame,
    // so a single rAF fitView would ignore them and never re-fit.
    let rafId: number;
    const tryFit = () => {
      const savedViewport = viewportsRef.current[viewMode];
      if (savedViewport) {
        void setViewport(savedViewport, { duration: 250 });
        return;
      }
      if (!fittedOnceRef.current[viewMode]) {
        const storeNodes = getNodes();
        if (storeNodes.some((n) => !n.width || !n.height)) {
          rafId = requestAnimationFrame(tryFit);
          return;
        }
        void fitView({ padding: 0.1, duration: 250, includeHiddenNodes: true });
        fittedOnceRef.current[viewMode] = true;
      }
    };
    rafId = requestAnimationFrame(tryFit);
    return () => cancelAnimationFrame(rafId);
  }, [fitView, getNodes, nodesInitialized, positioned.nodes, setViewport, viewMode, refreshToken]);

  const selId = selectedEntity ? selectedNodeId(selectedEntity) : null;
  const decoratedNodes: Node[] = useMemo(
    () =>
      positioned.nodes.map((n) => ({
        ...n,
        data: { ...(n.data as FlowNodeData), isSelected: n.id === selId },
      })),
    [positioned.nodes, selId],
  );

  useEffect(() => {
    if (!focusRequest || !nodesInitialized) return;
    if (focusRequest.nonce === lastFocusNonceRef.current) return;

    const target = positioned.nodes.find((node) => node.id === focusRequest.targetId);
    if (!target) return;

    lastFocusNonceRef.current = focusRequest.nonce;
    const targetIds = focusRequest.subtree ? descendantIds(focusRequest.targetId, positioned.edges) : new Set([focusRequest.targetId]);
    const focusNodes = positioned.nodes.filter((node) => targetIds.has(node.id));
    if (focusNodes.length === 0) return;

    // Drop any stale saved viewport for this mode. Programmatic setCenter
    // does not reliably fire onMoveEnd with the new viewport, so the saved
    // value still points at the pre-focus position; if a poll lands right
    // after the focus pan, the node-change effect would otherwise restore it
    // and snap the view back to where it was before the click.
    viewportsRef.current[viewMode] = undefined;

    const frame = requestAnimationFrame(() => {
      // Pan only — keep the current zoom level, just center the focused
      // node(s) in the viewport. Scaling on every click is jarring.
      const xs = focusNodes.map((n) => n.position.x + (n.width ?? 0) / 2);
      const ys = focusNodes.map((n) => n.position.y + (n.height ?? 0) / 2);
      const cx = xs.reduce((a, b) => a + b, 0) / xs.length;
      const cy = ys.reduce((a, b) => a + b, 0) / ys.length;
      void setCenter(cx, cy, { zoom: getZoom(), duration: 250 });
      fittedOnceRef.current[viewMode] = true;
    });
    return () => cancelAnimationFrame(frame);
  }, [setCenter, getZoom, focusRequest, nodesInitialized, positioned.edges, positioned.nodes, viewMode]);

  const handleFitAll = useCallback(() => {
    viewportsRef.current[viewMode] = undefined;
    void fitView({ padding: 0.1, duration: 250, includeHiddenNodes: true });
    fittedOnceRef.current[viewMode] = true;
  }, [fitView, viewMode]);

  const onNodeClick: NodeMouseHandler = useCallback(
    (_e, node) => {
      const entity = (node.data as FlowNodeData).entity;
      if (entity) selectEntity({ ...entity, viewMode });
    },
    [selectEntity, viewMode],
  );

  const onNodeContextMenu = useCallback(
    (e: React.MouseEvent, node: Node) => {
      e.preventDefault();
      const data = node.data as FlowNodeData;
      if (!data.entity) return;
      selectEntity({ ...data.entity, viewMode });
      onEntityContextMenu?.(
        { type: data.entity.type, id: data.entity.id, parentIds: data.entity.parentIds, label: data.label },
        e,
      );
    },
    [onEntityContextMenu, selectEntity, viewMode],
  );

  if (decoratedNodes.length === 0) {
    return (
      <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm tw-bg-bg">
        {viewMode === ViewMode.Physical
          ? 'No racks registered. Add a rack to get started.'
          : 'No stores yet. Switch to a deployed node and add a store.'}
      </div>
    );
  }

  return (
    <div className="tw-relative tw-w-full tw-h-full tw-bg-bg tw-overflow-hidden">
      <div className="tw-absolute tw-top-3 tw-right-3 tw-z-10">
        <Button
          variant="secondary"
          size="sm"
          leftIcon={<ScanSearch className="tw-h-3.5 tw-w-3.5" />}
          onClick={handleFitAll}
        >
          Fit All
        </Button>
      </div>
      <ReactFlow
        nodes={decoratedNodes}
        edges={positioned.edges}
        nodeTypes={NODE_TYPES}
        onNodeClick={onNodeClick}
        onNodeContextMenu={onNodeContextMenu}
        onMoveEnd={(_event, viewport) => {
          viewportsRef.current[viewMode] = viewport;
          fittedOnceRef.current[viewMode] = true;
        }}
        nodesDraggable={false}
        minZoom={0.02}
        maxZoom={2.5}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={24} color="#2e3440" />
      </ReactFlow>
    </div>
  );
}
