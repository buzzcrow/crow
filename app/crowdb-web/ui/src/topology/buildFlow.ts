// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Node, Edge, MarkerType } from 'reactflow';
import { Rack, Node as NodeEntity, EnrichedStoreView, Domain, CrowdbKVServerView, NodeHealth, DiskGroupEntry, DiskEntry } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { crowdbKvServerByNodeId } from '../data/crowdbKvServers';
import { DEFAULT_DC_ID, DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, serverLabel, storeLabel, toDisplayState, toUiReplicaRole } from '../utils/entityDisplay';

export interface NodeDiskGroups {
  diskGroups: DiskGroupEntry[];
  disksByDg: Record<number, DiskEntry[]>;
}

/**
 * React Flow node data shared by both views. `entity` is the selectable
 * identity (without `domain`, which the canvas stamps on click). `layer`
 * drives the deterministic layout in `layout.ts`.
 */
export interface FlowNodeData {
  kind: 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'LocalReplica' | 'RemoteReplica' | 'DiskGroup' | 'Disk';
  label: string;
  sublabel?: string;
  health?: string;
  role?: string;
  /** Remote-replica reachability (physical view); false renders a warning. */
  reachable?: boolean;
  /** Whether this replica is the group leader. */
  leader?: boolean;
  layer: number;
  entity?: Omit<SelectedEntity, 'domain'>;
  isSelected?: boolean;
}

function mkNode(id: string, data: FlowNodeData): Node {
  return { id, type: 'crowdbKv', position: { x: 0, y: 0 }, data };
}

/** Fixed UI-only datacenter root node id shared by all three builders. */
const DC_NODE_ID = `DC-${DEFAULT_DC_ID}`;

/** Push the fixed datacenter root at layer 0; returns the node + edge list seed. */
function pushDatacenterRoot(flowNodes: Node[], sublabel: string): { dcId: string } {
  flowNodes.push(
    mkNode(DC_NODE_ID, {
      kind: 'Datacenter',
      label: DEFAULT_DC_NAME,
      sublabel,
      layer: 0,
      entity: { type: 'Datacenter', id: DEFAULT_DC_ID, name: DEFAULT_DC_NAME },
    }),
  );
  return { dcId: DC_NODE_ID };
}

const LEADER_EDGE = {
  type: 'smoothstep',
  animated: true,
  style: { stroke: 'var(--color-accent, #88c0d0)' },
  markerEnd: { type: MarkerType.ArrowClosed },
};

/**
 * Cluster physical view: Rack -> Node -> KV server and physical
 * DiskGroup -> Disk siblings.
 */
export function buildPhysicalFlow(
  racks: Rack[],
  nodes: NodeEntity[],
  servers: CrowdbKVServerView[],
  _nodeStores: Record<string, unknown> = {},
  nodeHealthById: Record<string, NodeHealth> = {},
  diskdbNodeIds: Set<number> = new Set(),
  _diskdbInstances: { instance_id: number; owned_dg_ids: number[] }[] = [],
  nodeDiskGroups: Record<number, NodeDiskGroups> = {},
): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];
  const serverByNodeId = crowdbKvServerByNodeId(servers);
  const dcId = racks.length > 0 ? pushDatacenterRoot(flowNodes, `${racks.length} rack(s)`).dcId : null;

  for (const rack of racks) {
    flowNodes.push(
      mkNode(`R-${rack.id}`, {
        kind: 'Rack',
        label: rackLabel(String(rack.id)),
        sublabel: `${rack.nodes?.length ?? 0} node(s)`,
        layer: 1,
        entity: { type: 'Rack', id: String(rack.id), name: rack.name },
      }),
    );
    if (dcId) flowEdges.push({ id: `e-${dcId}-R-${rack.id}`, source: dcId, target: `R-${rack.id}`, type: 'smoothstep' });
  }

  for (const node of nodes) {
    const server = serverByNodeId.get(node.id);
    flowNodes.push(
      mkNode(`N-${node.id}`, {
        kind: 'Node',
        label: nodeLabel(String(node.id)),
        sublabel: node.host,
        health: nodeHealthById[node.id],
        layer: 2,
        entity: { type: 'Node', id: String(node.id), parentIds: { rack_id: node.rack_id } },
      }),
    );
    flowEdges.push({ id: `e-R-${node.rack_id}-N-${node.id}`, source: `R-${node.rack_id}`, target: `N-${node.id}`, type: 'smoothstep' });

    if (server) {
      const serverNodeId = `KV-${node.id}`;
      flowNodes.push(
        mkNode(serverNodeId, {
          kind: 'Server',
          label: serverLabel(String(node.id)),
          sublabel: toDisplayState(server.process.state),
          health: server.process.health,
          layer: 3,
          entity: { type: 'Server', id: server.id, parentIds: { rack_id: node.rack_id, node_id: node.id }, serviceType: 'kv' },
        }),
      );
      flowEdges.push({ id: `e-N-${node.id}-KV`, source: `N-${node.id}`, target: serverNodeId, type: 'smoothstep' });
    }

    // Cluster physical disk hierarchy is a sibling of the KV service.
    const clusterDiskGroups = nodeDiskGroups[node.id];
    for (const dg of clusterDiskGroups?.diskGroups || []) {
      const dgNodeId = `CL-DG-${node.id}-${dg.id}`;
      const disks = clusterDiskGroups?.disksByDg[dg.id] || [];
      flowNodes.push(mkNode(dgNodeId, {
        kind: 'DiskGroup',
        label: dg.name ? `${dg.name} (DG-${dg.id})` : `DG-${dg.id}`,
        sublabel: `${disks.length} disk(s)`,
        layer: 3,
        entity: { type: 'DiskGroup', id: String(dg.id), parentIds: { rack_id: node.rack_id, node_id: node.id, disk_group_id: dg.id } },
      }));
      flowEdges.push({ id: `e-N-${node.id}-${dgNodeId}`, source: `N-${node.id}`, target: dgNodeId, type: 'smoothstep' });
      for (const disk of disks) {
        const diskNodeId = `CL-D-${node.id}-${dg.id}-${disk.disk_id}`;
        flowNodes.push(mkNode(diskNodeId, {
          kind: 'Disk', label: disk.disk_id.slice(0, 12) + '…', layer: 4,
          entity: { type: 'Disk', id: disk.disk_id, parentIds: { rack_id: node.rack_id, node_id: node.id, disk_group_id: dg.id, disk_id: disk.disk_id } },
        }));
        flowEdges.push({ id: `e-${dgNodeId}-${diskNodeId}`, source: dgNodeId, target: diskNodeId, type: 'smoothstep' });
      }
    }

    // DiskDB server node (its owned disk-groups are not nested beneath it).
    if (diskdbNodeIds.has(node.id)) {
      const ddbNodeId = `DDB-${node.id}`;
      flowNodes.push(
        mkNode(ddbNodeId, {
          kind: 'Server',
          label: `DDB-${node.id}`,
          sublabel: 'DiskDB',
          layer: 3,
          entity: { type: 'Server', id: `DDB-${node.id}`, parentIds: { rack_id: node.rack_id, node_id: node.id }, serviceType: 'diskdb' },
        }),
      );
      flowEdges.push({ id: `e-N-${node.id}-DDB`, source: `N-${node.id}`, target: ddbNodeId, type: 'smoothstep' });

    }
  }

  return { nodes: flowNodes, edges: flowEdges };
}

/**
 * Logical view: Cluster -> Store -> Group -> Replica. The leader radiates
 * accent edges to followers; replicas are badged by `node_id`.
 */
export function buildLogicalFlow(stores: EnrichedStoreView[]): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];
  const dcId = stores.length > 0 ? pushDatacenterRoot(flowNodes, `${stores.length} store(s)`).dcId : null;

  for (const store of stores) {
    const sid = String(store.store_id);
    const storeNodeId = `S-${sid}`;
    flowNodes.push(
      mkNode(storeNodeId, {
        kind: 'Store',
        label: store.name ? `${storeLabel(sid)} (${store.name})` : storeLabel(sid),
        sublabel: `${store.groups?.length ?? 0} group(s)`,
        layer: 1,
        entity: { type: 'Store', id: sid, name: store.name },
      }),
    );
    if (dcId) flowEdges.push({ id: `e-${dcId}-${storeNodeId}`, source: dcId, target: storeNodeId, type: 'smoothstep' });

    for (const group of store.groups || []) {
      const gid = String(group.group_id);
      const replicas = group.replicas;
      const leader = group.leader;
      flowNodes.push(
        mkNode(`G-${sid}-${gid}`, {
          kind: 'Group',
          label: groupLabel(gid),
          sublabel: leader ? `leader ${leader}` : `${replicas.length} replica(s)`,
          health: group.state,
          layer: 2,
          entity: { type: 'Group', id: gid, parentIds: { store_id: sid } },
        }),
      );
      flowEdges.push({
        id: `e-S-${sid}-G-${gid}`,
        source: `S-${sid}`,
        target: `G-${sid}-${gid}`,
        type: 'smoothstep',
      });

      const leaderNodeId = leader != null ? `LR-${sid}-${gid}-${leader}` : null;
      for (const r of replicas) {
        const rid = String(r.replica_id);
        const nid = `LR-${sid}-${gid}-${rid}`;
        flowNodes.push(
          mkNode(nid, {
            kind: 'Replica',
            label: localReplicaLabel(rid),
            sublabel: r.node_id ? nodeLabel(String(r.node_id)) : undefined,
            health: r.state,
            role: toUiReplicaRole(String(r.role), String(r.state)),
            layer: 3,
            entity: {
              type: 'Replica',
              id: rid,
              parentIds: { store_id: sid, group_id: gid, node_id: String(r.node_id ?? '') },
            },
          }),
        );
        flowEdges.push({
          id: `e-G-${sid}-${gid}-LR-${rid}`,
          source: `G-${sid}-${gid}`,
          target: nid,
          type: 'smoothstep',
        });
        // Leader -> follower accent edge in addition to the containment edge.
        if (leaderNodeId && leaderNodeId !== nid) {
          flowEdges.push({ id: `e-leader-${gid}-${rid}`, source: leaderNodeId, target: nid, ...LEADER_EDGE });
        }
      }
    }
  }

  return { nodes: flowNodes, edges: flowEdges };
}

/**
 * Capacity view: Rack -> Node -> DiskGroup -> Disk. Shows the physical
 * hierarchy for disk-management operations. Disk-groups and disks are
 * rendered as children of the node that owns them.
 */
export function buildCapacityFlow(
  racks: Rack[],
  nodes: NodeEntity[],
  diskdbNodeIds: Set<number> = new Set(),
  nodeHealthById: Record<string, NodeHealth> = {},
  nodeDiskGroups: Record<number, NodeDiskGroups> = {},
): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];
  const dcId = racks.length > 0 ? pushDatacenterRoot(flowNodes, `${racks.length} rack(s)`).dcId : null;

  for (const rack of racks) {
    flowNodes.push(
      mkNode(`R-${rack.id}`, {
        kind: 'Rack',
        label: rackLabel(String(rack.id)),
        sublabel: `${rack.nodes?.length ?? 0} node(s)`,
        layer: 1,
        entity: { type: 'Rack', id: String(rack.id), name: rack.name },
      }),
    );
    if (dcId) flowEdges.push({ id: `e-${dcId}-R-${rack.id}`, source: dcId, target: `R-${rack.id}`, type: 'smoothstep' });
  }

  for (const node of nodes) {
    const hasDiskdb = diskdbNodeIds.has(node.id);
    flowNodes.push(
      mkNode(`N-${node.id}`, {
        kind: 'Node',
        label: nodeLabel(String(node.id)),
        sublabel: hasDiskdb ? 'DiskDB active' : node.host,
        health: nodeHealthById[node.id],
        layer: 2,
        entity: { type: 'Node', id: String(node.id), parentIds: { rack_id: node.rack_id } },
      }),
    );
    flowEdges.push({ id: `e-R-${node.rack_id}-N-${node.id}`, source: `R-${node.rack_id}`, target: `N-${node.id}`, type: 'smoothstep' });

    // Disk-group → disk children.
    const ndg = nodeDiskGroups[node.id];
    const diskGroups = ndg?.diskGroups || [];
    for (const dg of diskGroups) {
      const dgNodeId = `CDG-${node.id}-${dg.id}`;
      const disks = ndg?.disksByDg[dg.id] || [];
      flowNodes.push(
        mkNode(dgNodeId, {
          kind: 'DiskGroup',
          label: dg.name ? `${dg.name} (DG-${dg.id})` : `DG-${dg.id}`,
          sublabel: disks.length > 0 ? `${disks.length} disk(s)` : '',
          layer: 3,
          entity: { type: 'DiskGroup', id: String(dg.id), parentIds: { rack_id: node.rack_id, node_id: node.id, disk_group_id: dg.id } },
        }),
      );
      flowEdges.push({ id: `e-N-${node.id}-${dgNodeId}`, source: `N-${node.id}`, target: dgNodeId, type: 'smoothstep' });

      for (const disk of disks) {
        const diskNodeId = `CD-${node.id}-${dg.id}-${disk.disk_id}`;
        flowNodes.push(
          mkNode(diskNodeId, {
            kind: 'Disk',
            label: disk.disk_id.slice(0, 12) + '…',
            sublabel: '',
            layer: 4,
            entity: {
              type: 'Disk',
              id: disk.disk_id,
              parentIds: { rack_id: node.rack_id, node_id: node.id, disk_group_id: dg.id, disk_id: disk.disk_id },
            },
          }),
        );
        flowEdges.push({ id: `e-${dgNodeId}-${diskNodeId}`, source: dgNodeId, target: diskNodeId, type: 'smoothstep' });
      }
    }

    // DiskDB is a service sibling of the physical disk hierarchy. Its owned
    // disk-groups are intentionally not rendered beneath it.
    if (hasDiskdb) {
      const ddbNodeId = `DDB-${node.id}`;
      flowNodes.push(
        mkNode(ddbNodeId, {
          kind: 'Server',
          label: `DDB-${node.id}`,
          sublabel: 'DiskDB',
          layer: 3,
          entity: { type: 'Server', id: ddbNodeId, parentIds: { rack_id: node.rack_id, node_id: node.id }, serviceType: 'diskdb' },
        }),
      );
      flowEdges.push({ id: `e-N-${node.id}-DDB`, source: `N-${node.id}`, target: ddbNodeId, type: 'smoothstep' });
    }
  }

  return { nodes: flowNodes, edges: flowEdges };
}

export function buildFlowForDomain(
  domain: Domain,
  racks: Rack[],
  nodes: NodeEntity[],
  servers: CrowdbKVServerView[],
  stores: EnrichedStoreView[],
  _nodeStores: Record<string, unknown> = {},
  nodeHealthById: Record<string, NodeHealth> = {},
  diskdbNodeIds: Set<number> = new Set(),
  _diskdbInstances: { instance_id: number; owned_dg_ids: number[] }[] = [],
  nodeDiskGroups: Record<number, NodeDiskGroups> = {},
): { nodes: Node[]; edges: Edge[] } {
  switch (domain) {
    case Domain.Cluster:
      return buildPhysicalFlow(racks, nodes, servers, _nodeStores, nodeHealthById, diskdbNodeIds, _diskdbInstances, nodeDiskGroups);
    case Domain.Chunk:
      return buildCapacityFlow(racks, nodes, diskdbNodeIds, nodeHealthById, nodeDiskGroups);
    default:
      return buildLogicalFlow(stores);
  }
}
