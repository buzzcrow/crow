import { Node, Edge, MarkerType } from 'reactflow';
import { Rack, Node as NodeEntity, StoreView, ViewMode } from '../types';

/**
 * Build React Flow nodes+edges from the physical tree
 * (Rack -> Node -> Server placeholder). Each node carries `data.layer` so
 * the hierarchical layout can place it; `data.groupKey` so the grid layout
 * can cluster same-type entities.
 */
export function buildPhysicalFlow(racks: Rack[], nodes: NodeEntity[]): {
  nodes: Node[];
  edges: Edge[];
} {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];

  for (const rack of racks) {
    flowNodes.push({
      id: `rack-${rack.id}`,
      type: 'crowkv',
      position: { x: 0, y: 0 },
      data: {
        kind: 'Rack',
        label: rack.name || rack.id,
        sublabel: `${rack.nodes.length} node${rack.nodes.length === 1 ? '' : 's'}`,
        layer: 0,
        groupKey: 'rack',
        entity: { type: 'Rack', id: rack.id, name: rack.name },
      },
    });
  }

  for (const node of nodes) {
    flowNodes.push({
      id: `node-${node.id}`,
      type: 'crowkv',
      position: { x: 0, y: 0 },
      data: {
        kind: 'Node',
        label: node.id,
        sublabel: node.host,
        health: node.server?.health,
        layer: 1,
        groupKey: 'node',
        entity: {
          type: 'Node',
          id: node.id,
          parentIds: { rackId: node.rack_id },
        },
      },
    });
    flowEdges.push({
      id: `e-rack-${node.rack_id}-node-${node.id}`,
      source: `rack-${node.rack_id}`,
      target: `node-${node.id}`,
      type: 'smoothstep',
    });
  }

  return { nodes: flowNodes, edges: flowEdges };
}

/**
 * Build React Flow nodes+edges from the logical tree
 * (Store -> Group). Replica-level expansion is omitted for now since the
 * physical view already exposes peer wiring.
 */
export function buildLogicalFlow(stores: StoreView[]): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];

  for (const store of stores) {
    flowNodes.push({
      id: `store-${store.store_id}`,
      type: 'crowkv',
      position: { x: 0, y: 0 },
      data: {
        kind: 'Store',
        label: store.name || store.store_id,
        sublabel: `${store.groups?.length ?? 0} group(s)`,
        layer: 0,
        groupKey: 'store',
        entity: { type: 'Store', id: store.store_id, name: store.name },
      },
    });
    for (const group of store.groups || []) {
      flowNodes.push({
        id: `group-${store.store_id}-${group.group_id}`,
        type: 'crowkv',
        position: { x: 0, y: 0 },
        data: {
          kind: 'Group',
          label: group.group_id,
          sublabel: `${group.replica_count} replicas`,
          health: group.health,
          layer: 1,
          groupKey: 'group',
          entity: {
            type: 'Group',
            id: group.group_id,
            parentIds: { storeId: store.store_id },
          },
        },
      });
      flowEdges.push({
        id: `e-store-${store.store_id}-group-${group.group_id}`,
        source: `store-${store.store_id}`,
        target: `group-${store.store_id}-${group.group_id}`,
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed },
        data: {
          // Placeholder replication metrics; live values will be filled in
          // by a future polling integration.
          replicationLagMs: undefined as number | undefined,
          throughput: undefined as number | undefined,
        },
      });
    }
  }

  return { nodes: flowNodes, edges: flowEdges };
}

export function buildFlowForViewMode(
  viewMode: ViewMode,
  racks: Rack[],
  nodes: NodeEntity[],
  stores: StoreView[],
): { nodes: Node[]; edges: Edge[] } {
  if (viewMode === ViewMode.Physical) return buildPhysicalFlow(racks, nodes);
  return buildLogicalFlow(stores);
}
