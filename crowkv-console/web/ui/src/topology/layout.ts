import { Node, Edge } from 'reactflow';

export type LayoutKind = 'hierarchical' | 'grid' | 'force';

export interface LayoutOptions {
  kind: LayoutKind;
  /** Horizontal spacing between nodes. */
  hSpacing?: number;
  /** Vertical spacing between layers. */
  vSpacing?: number;
}

/** A node may carry an explicit `layer` (depth from root) in its data. */
interface LayoutableNodeData {
  layer?: number;
  groupKey?: string;
}

/**
 * Top-level entrypoint. Dispatches by LayoutKind and returns a new node list
 * with computed positions; edges are returned unchanged but available so
 * future layouts can use connectivity.
 */
export function applyLayout(
  nodes: Node[],
  edges: Edge[],
  opts: LayoutOptions,
): { nodes: Node[]; edges: Edge[] } {
  const hSpacing = opts.hSpacing ?? 220;
  const vSpacing = opts.vSpacing ?? 140;
  switch (opts.kind) {
    case 'hierarchical':
      return { nodes: hierarchicalLayout(nodes, edges, hSpacing, vSpacing), edges };
    case 'grid':
      return { nodes: gridLayout(nodes, hSpacing, vSpacing), edges };
    case 'force':
      return { nodes: forceLayout(nodes, edges, hSpacing, vSpacing), edges };
  }
}

/**
 * Hierarchical layout: groups nodes by `data.layer`, lays each layer in a
 * horizontal row, and centers rows horizontally. Falls back to BFS from
 * source-less nodes if layers aren't pre-tagged.
 */
function hierarchicalLayout(
  nodes: Node[],
  edges: Edge[],
  hSpacing: number,
  vSpacing: number,
): Node[] {
  const layers = computeLayers(nodes, edges);
  const byLayer = new Map<number, Node[]>();
  for (const node of nodes) {
    const layer = layers.get(node.id) ?? 0;
    const list = byLayer.get(layer) || [];
    list.push(node);
    byLayer.set(layer, list);
  }
  const sortedLayers = [...byLayer.keys()].sort((a, b) => a - b);
  const widestRow = Math.max(...[...byLayer.values()].map((row) => row.length));
  const totalWidth = widestRow * hSpacing;

  const positioned: Node[] = [];
  for (const layer of sortedLayers) {
    const row = byLayer.get(layer)!;
    const rowWidth = row.length * hSpacing;
    const offsetX = (totalWidth - rowWidth) / 2;
    row.forEach((node, idx) => {
      positioned.push({
        ...node,
        position: { x: offsetX + idx * hSpacing, y: layer * vSpacing },
      });
    });
  }
  return positioned;
}

/**
 * Grid layout: arranges nodes into a square-ish grid, optionally grouping
 * by `data.groupKey` so same-type nodes cluster together.
 */
function gridLayout(nodes: Node[], hSpacing: number, vSpacing: number): Node[] {
  const groups = new Map<string, Node[]>();
  for (const node of nodes) {
    const key = (node.data as LayoutableNodeData | undefined)?.groupKey ?? '_default';
    const list = groups.get(key) || [];
    list.push(node);
    groups.set(key, list);
  }

  const positioned: Node[] = [];
  let yOffset = 0;
  for (const [, groupNodes] of groups) {
    const cols = Math.ceil(Math.sqrt(groupNodes.length));
    groupNodes.forEach((node, idx) => {
      const row = Math.floor(idx / cols);
      const col = idx % cols;
      positioned.push({
        ...node,
        position: { x: col * hSpacing, y: yOffset + row * vSpacing },
      });
    });
    const rows = Math.ceil(groupNodes.length / cols);
    yOffset += rows * vSpacing + vSpacing; // padding between groups
  }
  return positioned;
}

/**
 * Force-directed layout: simple spring-embedder. Each iteration applies a
 * repulsive force between every pair of nodes and an attractive force
 * along each edge. Suitable for clusters up to ~200 nodes.
 */
function forceLayout(
  nodes: Node[],
  edges: Edge[],
  hSpacing: number,
  _vSpacing: number,
): Node[] {
  const k = hSpacing; // ideal edge length
  const iterations = 80;
  const width = Math.max(800, Math.sqrt(nodes.length) * hSpacing);

  // Seed positions on a circle so we don't all start at the origin.
  const positions = new Map<string, { x: number; y: number }>();
  nodes.forEach((node, i) => {
    const angle = (i / Math.max(1, nodes.length)) * Math.PI * 2;
    positions.set(node.id, {
      x: width / 2 + Math.cos(angle) * width * 0.3,
      y: width / 2 + Math.sin(angle) * width * 0.3,
    });
  });

  for (let iter = 0; iter < iterations; iter++) {
    const disp = new Map<string, { x: number; y: number }>();
    nodes.forEach((n) => disp.set(n.id, { x: 0, y: 0 }));

    // Repulsion between every pair.
    for (let i = 0; i < nodes.length; i++) {
      for (let j = i + 1; j < nodes.length; j++) {
        const a = positions.get(nodes[i].id)!;
        const b = positions.get(nodes[j].id)!;
        let dx = a.x - b.x;
        let dy = a.y - b.y;
        let dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
        const force = (k * k) / dist;
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;
        const da = disp.get(nodes[i].id)!;
        const db = disp.get(nodes[j].id)!;
        da.x += fx;
        da.y += fy;
        db.x -= fx;
        db.y -= fy;
      }
    }

    // Attraction along each edge.
    for (const edge of edges) {
      const a = positions.get(edge.source);
      const b = positions.get(edge.target);
      if (!a || !b) continue;
      const dx = a.x - b.x;
      const dy = a.y - b.y;
      const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
      const force = (dist * dist) / k;
      const fx = (dx / dist) * force;
      const fy = (dy / dist) * force;
      const da = disp.get(edge.source)!;
      const db = disp.get(edge.target)!;
      da.x -= fx;
      da.y -= fy;
      db.x += fx;
      db.y += fy;
    }

    // Cooling factor.
    const temperature = width * (1 - iter / iterations) * 0.05;
    for (const node of nodes) {
      const d = disp.get(node.id)!;
      const dlen = Math.sqrt(d.x * d.x + d.y * d.y) || 0.01;
      const p = positions.get(node.id)!;
      p.x += (d.x / dlen) * Math.min(dlen, temperature);
      p.y += (d.y / dlen) * Math.min(dlen, temperature);
    }
  }

  return nodes.map((node) => ({
    ...node,
    position: positions.get(node.id) || { x: 0, y: 0 },
  }));
}

/**
 * Compute a layer index per node. Prefers explicit `data.layer`; otherwise
 * runs BFS from each node with no inbound edges.
 */
function computeLayers(nodes: Node[], edges: Edge[]): Map<string, number> {
  const layers = new Map<string, number>();
  const explicit = nodes.every((n) => (n.data as LayoutableNodeData | undefined)?.layer !== undefined);
  if (explicit) {
    for (const n of nodes) {
      layers.set(n.id, (n.data as LayoutableNodeData).layer!);
    }
    return layers;
  }
  const incoming = new Map<string, number>();
  for (const n of nodes) incoming.set(n.id, 0);
  for (const e of edges) {
    incoming.set(e.target, (incoming.get(e.target) || 0) + 1);
  }
  const queue: string[] = [];
  for (const n of nodes) {
    if ((incoming.get(n.id) || 0) === 0) {
      layers.set(n.id, 0);
      queue.push(n.id);
    }
  }
  const adj = new Map<string, string[]>();
  for (const e of edges) {
    if (!adj.has(e.source)) adj.set(e.source, []);
    adj.get(e.source)!.push(e.target);
  }
  while (queue.length > 0) {
    const id = queue.shift()!;
    const layer = layers.get(id) || 0;
    for (const next of adj.get(id) || []) {
      const existing = layers.get(next);
      if (existing === undefined || existing < layer + 1) {
        layers.set(next, layer + 1);
        queue.push(next);
      }
    }
  }
  // Anything unreached: drop on layer 0.
  for (const n of nodes) {
    if (!layers.has(n.id)) layers.set(n.id, 0);
  }
  return layers;
}
