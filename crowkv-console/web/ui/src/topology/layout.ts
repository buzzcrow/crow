import { Node, Edge } from 'reactflow';

/**
 * Deterministic hierarchical layout. Nodes carry an explicit `data.layer`
 * (depth from root); each layer is laid out as a horizontal row and rows are
 * centered. No force simulation, no user-selectable layouts (v1 lean).
 */
const H_SPACING = 220;
const V_SPACING = 130;
const ROOT_GAP = 180;

interface LayerData {
  layer?: number;
}

export function layoutTree(nodes: Node[], edges: Edge[]): { nodes: Node[]; edges: Edge[] } {
  const nodeMap = new Map(nodes.map((node) => [node.id, node]));
  const layerOf = (node: Node) => (node.data as LayerData | undefined)?.layer ?? 0;
  const containmentEdges = edges.filter((edge) => {
    const source = nodeMap.get(edge.source);
    const target = nodeMap.get(edge.target);
    if (!source || !target) return false;
    return layerOf(target) === layerOf(source) + 1;
  });

  const childrenByParent = new Map<string, string[]>();
  const parentByChild = new Map<string, string>();
  for (const edge of containmentEdges) {
    const children = childrenByParent.get(edge.source) ?? [];
    children.push(edge.target);
    childrenByParent.set(edge.source, children);
    if (!parentByChild.has(edge.target)) parentByChild.set(edge.target, edge.source);
  }

  const roots = nodes.filter((node) => !parentByChild.has(node.id));
  const xById = new Map<string, number>();
  const yById = new Map<string, number>();
  let nextRootY = 0;

  const collectSubtree = (rootId: string): string[] => {
    const ordered: string[] = [];
    const visit = (nodeId: string) => {
      ordered.push(nodeId);
      for (const childId of childrenByParent.get(nodeId) ?? []) visit(childId);
    };
    visit(rootId);
    return ordered;
  };

  for (const root of roots) {
    let nextLeafX = 0;
    const localXById = new Map<string, number>();

    const place = (nodeId: string): number => {
      const children = childrenByParent.get(nodeId) ?? [];
      if (children.length === 0) {
        const x = nextLeafX * H_SPACING;
        nextLeafX += 1;
        localXById.set(nodeId, x);
        return x;
      }

      const childXs = children.map((childId) => place(childId));
      const x = (childXs[0] + childXs[childXs.length - 1]) / 2;
      localXById.set(nodeId, x);
      return x;
    };

    place(root.id);

    const subtreeIds = collectSubtree(root.id);
    const xs = subtreeIds.map((id) => localXById.get(id) ?? 0);
    const minX = xs.length ? Math.min(...xs) : 0;
    const maxLayer = subtreeIds.reduce((max, id) => {
      const node = nodeMap.get(id);
      return node ? Math.max(max, layerOf(node)) : max;
    }, 0);

    for (const id of subtreeIds) {
      const node = nodeMap.get(id);
      if (!node) continue;
      xById.set(id, (localXById.get(id) ?? 0) - minX);
      yById.set(id, nextRootY + layerOf(node) * V_SPACING);
    }

    nextRootY += (maxLayer + 1) * V_SPACING + ROOT_GAP;
  }

  const positioned = nodes.map((node) => ({
    ...node,
    position: {
      x: xById.get(node.id) ?? 0,
      y: yById.get(node.id) ?? layerOf(node) * V_SPACING,
    },
  }));

  return { nodes: positioned, edges };
}
