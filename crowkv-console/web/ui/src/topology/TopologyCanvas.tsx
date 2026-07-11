import { useEffect, useMemo, useState, useCallback, useRef } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  useReactFlow,
  Node,
  Edge,
  NodeMouseHandler,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Search, Focus, Tag, ChevronLeft, ChevronRight, X } from 'lucide-react';
import { useViewMode } from '../contexts/ViewModeContext';
import { useSelection } from '../contexts/SelectionContext';
import { Rack, Node as NodeEntity, StoreView } from '../types';
import { localStorage } from '../utils/localStorage';
import { exportAsSVG, exportAsPNG } from '../utils/exportUtils';
import { cn } from '../utils/cn';
import { useDebouncedValue } from '../hooks/useDebouncedValue';
import { buildFlowForViewMode } from './buildFlow';
import { applyLayout, LayoutKind } from './layout';
import { CrowKVNode } from './CrowKVNode';
import { ExportDropdown } from '../components/ExportDropdown';

interface TopologyCanvasProps {
  racks: Rack[];
  nodes: NodeEntity[];
  stores: StoreView[];
}

const NODE_TYPES = { crowkv: CrowKVNode };

const LAYOUT_LABELS: Record<LayoutKind, string> = {
  hierarchical: 'Hierarchical',
  grid: 'Grid',
  force: 'Force-directed',
};

export function TopologyCanvas(props: TopologyCanvasProps) {
  return (
    <ReactFlowProvider>
      <TopologyCanvasInner {...props} />
    </ReactFlowProvider>
  );
}

function TopologyCanvasInner({ racks, nodes, stores }: TopologyCanvasProps) {
  const { viewMode } = useViewMode();
  const { selectedEntity, selectEntity } = useSelection();
  const flow = useReactFlow();

  // Per-view-mode persisted toolbar state.
  const [layout, setLayout] = useState<LayoutKind>(() =>
    localStorage.get<LayoutKind>('topologyLayout', 'hierarchical'),
  );
  const [showEdgeLabels, setShowEdgeLabels] = useState<boolean>(() =>
    localStorage.get<boolean>('showEdgeLabels', false),
  );
  const [focusMode, setFocusMode] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [matchIndex, setMatchIndex] = useState(0);

  useEffect(() => {
    localStorage.set('topologyLayout', layout);
  }, [layout]);
  useEffect(() => {
    localStorage.set('showEdgeLabels', showEdgeLabels);
  }, [showEdgeLabels]);

  // Build raw nodes+edges from current data; re-layout when layout or
  // view-mode changes.
  const { nodes: rawNodes, edges: rawEdges } = useMemo(
    () => buildFlowForViewMode(viewMode, racks, nodes, stores),
    [viewMode, racks, nodes, stores],
  );

  const positioned = useMemo(() => {
    return applyLayout(rawNodes, rawEdges, { kind: layout });
  }, [rawNodes, rawEdges, layout]);

  // Compute search matches over the laid-out node list.
  // Debounce the query so we don't re-scan on every keystroke for large
  // clusters; 150 ms feels instantaneous but cuts work on 1k-node trees.
  const debouncedQuery = useDebouncedValue(searchQuery, 150);
  const matches = useMemo(() => {
    const q = debouncedQuery.trim().toLowerCase();
    if (!q) return [] as string[];
    return positioned.nodes
      .filter((n) => {
        const label = (n.data as { label?: string }).label?.toLowerCase() || '';
        const sub = (n.data as { sublabel?: string }).sublabel?.toLowerCase() || '';
        return label.includes(q) || sub.includes(q) || n.id.toLowerCase().includes(q);
      })
      .map((n) => n.id);
  }, [debouncedQuery, positioned.nodes]);

  useEffect(() => {
    if (matchIndex >= matches.length) setMatchIndex(0);
  }, [matches.length, matchIndex]);

  // Focus-mode neighbour set: selected entity + direct neighbours in the
  // edge graph.
  const focusNeighbours = useMemo(() => {
    if (!focusMode || !selectedEntity) return null;
    const selId = selectionToNodeId(selectedEntity, viewMode);
    if (!selId) return null;
    const keep = new Set<string>([selId]);
    for (const e of positioned.edges) {
      if (e.source === selId) keep.add(e.target);
      if (e.target === selId) keep.add(e.source);
    }
    return keep;
  }, [focusMode, selectedEntity, viewMode, positioned.edges]);

  // Decorate nodes with selection/highlight/dim state and edges with
  // optional metric labels.
  const decoratedNodes: Node[] = useMemo(() => {
    const highlightSet = new Set(matches);
    const activeMatch = matches[matchIndex];
    return positioned.nodes.map((n) => ({
      ...n,
      data: {
        ...n.data,
        isHighlighted: highlightSet.has(n.id),
        isDimmed: focusNeighbours ? !focusNeighbours.has(n.id) : false,
        isSelected:
          !!selectedEntity && selectionToNodeId(selectedEntity, viewMode) === n.id,
      },
      selected: n.id === activeMatch,
    }));
  }, [positioned.nodes, matches, matchIndex, focusNeighbours, selectedEntity, viewMode]);

  const decoratedEdges: Edge[] = useMemo(() => {
    if (!showEdgeLabels && !focusNeighbours) return positioned.edges;
    return positioned.edges.map((e) => ({
      ...e,
      label: showEdgeLabels ? formatEdgeMetrics(e) : undefined,
      labelBgPadding: showEdgeLabels ? ([4, 2] as [number, number]) : undefined,
      labelBgBorderRadius: 4,
      labelStyle: showEdgeLabels
        ? { fill: '#d8dee9', fontSize: 10 }
        : undefined,
      labelBgStyle: showEdgeLabels
        ? { fill: '#161a1f', stroke: '#2e3440' }
        : undefined,
      style: focusNeighbours && !(focusNeighbours.has(e.source) && focusNeighbours.has(e.target))
        ? { opacity: 0.15 }
        : undefined,
    }));
  }, [positioned.edges, showEdgeLabels, focusNeighbours]);

  // Center on active search match.
  useEffect(() => {
    const id = matches[matchIndex];
    if (!id) return;
    const node = decoratedNodes.find((n) => n.id === id);
    if (!node) return;
    flow.setCenter(node.position.x + 80, node.position.y + 30, { zoom: 1.2, duration: 300 });
  }, [matchIndex, matches, decoratedNodes, flow]);

  // Click → select entity.
  const onNodeClick: NodeMouseHandler = useCallback(
    (_e, node) => {
      const entity = (node.data as { entity?: import('../contexts/SelectionContext').SelectedEntity })
        .entity;
      if (entity) {
        selectEntity({ ...entity, viewMode });
      }
    },
    [selectEntity, viewMode],
  );

  const canvasRef = useRef<HTMLDivElement>(null);

  const exportOptions = useMemo(
    () => [
      {
        id: 'svg',
        label: 'Export as SVG',
        hint: 'Vector, editable',
        onSelect: async () => {
          await exportAsSVG(decoratedNodes, decoratedEdges, 1200, 800, `topology-${viewMode}.svg`);
        },
      },
      {
        id: 'png',
        label: 'Export as PNG',
        hint: 'High-resolution raster',
        onSelect: async () => {
          await exportAsPNG(decoratedNodes, decoratedEdges, 1200, 800, `topology-${viewMode}.png`);
        },
      },
    ],
    [decoratedNodes, decoratedEdges, viewMode],
  );

  return (
    <div ref={canvasRef} className="tw-relative tw-w-full tw-h-full tw-bg-bg">
      <ReactFlow
        nodes={decoratedNodes}
        edges={decoratedEdges}
        nodeTypes={NODE_TYPES}
        onNodeClick={onNodeClick}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={24} color="#2e3440" />
        <Controls className="!tw-bg-panel !tw-border-border" />
        <MiniMap className="!tw-bg-panel !tw-border-border" pannable zoomable />
      </ReactFlow>

      {/* Floating toolbar */}
      <div className="tw-absolute tw-top-3 tw-left-3 tw-flex tw-items-center tw-gap-2 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-px-2 tw-py-1.5 tw-shadow-lg">
        {/* Layout selector */}
        <label className="tw-flex tw-items-center tw-gap-1 tw-text-xs tw-text-muted">
          Layout
          <select
            value={layout}
            onChange={(e) => setLayout(e.target.value as LayoutKind)}
            className="tw-bg-bg tw-border tw-border-border tw-rounded tw-px-1.5 tw-py-0.5 tw-text-xs tw-text-text"
          >
            {(Object.keys(LAYOUT_LABELS) as LayoutKind[]).map((k) => (
              <option key={k} value={k}>
                {LAYOUT_LABELS[k]}
              </option>
            ))}
          </select>
        </label>

        {/* Search */}
        <div className="tw-flex tw-items-center tw-gap-1 tw-bg-bg tw-border tw-border-border tw-rounded tw-px-1.5">
          <Search className="tw-h-3 tw-w-3 tw-text-muted" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value);
              setMatchIndex(0);
            }}
            placeholder="Find..."
            className="tw-bg-transparent tw-text-xs tw-text-text tw-w-28 tw-py-1 tw-outline-none tw-placeholder-muted"
            aria-label="Search topology"
          />
          {searchQuery && (
            <>
              <span className="tw-text-[10px] tw-text-muted">
                {matches.length > 0 ? `${matchIndex + 1}/${matches.length}` : '0/0'}
              </span>
              <button
                onClick={() => setMatchIndex((i) => (i - 1 + matches.length) % matches.length)}
                disabled={matches.length === 0}
                className="tw-p-0.5 tw-text-muted hover:tw-text-text disabled:tw-opacity-30"
                aria-label="Previous match"
              >
                <ChevronLeft className="tw-h-3 tw-w-3" />
              </button>
              <button
                onClick={() => setMatchIndex((i) => (i + 1) % matches.length)}
                disabled={matches.length === 0}
                className="tw-p-0.5 tw-text-muted hover:tw-text-text disabled:tw-opacity-30"
                aria-label="Next match"
              >
                <ChevronRight className="tw-h-3 tw-w-3" />
              </button>
              <button
                onClick={() => {
                  setSearchQuery('');
                  setMatchIndex(0);
                }}
                className="tw-p-0.5 tw-text-muted hover:tw-text-text"
                aria-label="Clear search"
              >
                <X className="tw-h-3 tw-w-3" />
              </button>
            </>
          )}
        </div>

        {/* Focus mode */}
        <button
          onClick={() => setFocusMode((v) => !v)}
          className={cn(
            'tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-rounded tw-text-xs tw-border tw-transition-colors',
            focusMode
              ? 'tw-bg-accent/10 tw-text-accent tw-border-accent/30'
              : 'tw-bg-bg tw-text-text tw-border-border hover:tw-bg-panel',
          )}
          title={
            focusMode
              ? 'Disable focus mode'
              : selectedEntity
                ? 'Show only selected entity and direct peers'
                : 'Select an entity first'
          }
          disabled={!selectedEntity && !focusMode}
          aria-pressed={focusMode}
        >
          <Focus className="tw-h-3 tw-w-3" />
          Focus
        </button>

        {/* Edge labels */}
        <button
          onClick={() => setShowEdgeLabels((v) => !v)}
          className={cn(
            'tw-flex tw-items-center tw-gap-1 tw-px-2 tw-py-1 tw-rounded tw-text-xs tw-border tw-transition-colors',
            showEdgeLabels
              ? 'tw-bg-accent/10 tw-text-accent tw-border-accent/30'
              : 'tw-bg-bg tw-text-text tw-border-border hover:tw-bg-panel',
          )}
          title="Toggle replication metric labels on edges"
          aria-pressed={showEdgeLabels}
        >
          <Tag className="tw-h-3 tw-w-3" />
          Labels
        </button>

        {/* Export */}
        <ExportDropdown options={exportOptions} buttonLabel="Export" />
      </div>
    </div>
  );
}

/**
 * Map a SelectedEntity to the matching React Flow node id. View-mode-aware
 * because the same entity can live in different trees.
 */
function selectionToNodeId(
  entity: import('../contexts/SelectionContext').SelectedEntity,
  viewMode: import('../types').ViewMode,
): string | null {
  if (viewMode === 'Physical') {
    switch (entity.type) {
      case 'Rack':
        return `rack-${entity.id}`;
      case 'Node':
        return `node-${entity.id}`;
      default:
        return null;
    }
  }
  // Logical
  switch (entity.type) {
    case 'Store':
      return `store-${entity.id}`;
    case 'Group':
      return entity.parentIds?.storeId
        ? `group-${entity.parentIds.storeId}-${entity.id}`
        : null;
    default:
      return null;
  }
}

/** Format edge metrics for the label overlay. */
function formatEdgeMetrics(edge: Edge): string {
  const data = (edge.data || {}) as { replicationLagMs?: number; throughput?: number };
  const parts: string[] = [];
  if (typeof data.replicationLagMs === 'number') parts.push(`${data.replicationLagMs}ms`);
  if (typeof data.throughput === 'number') parts.push(`${data.throughput}/s`);
  return parts.length === 0 ? '—' : parts.join(' · ');
}
