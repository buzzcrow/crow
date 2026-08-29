// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { Node, Edge, MarkerType } from 'reactflow';
import { Rack, Node as NodeEntity, EnrichedStoreView, NodeStore, ViewMode, CrowdbKVServerView, NodeHealth, ReplicaState } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { crowdbKvServerByNodeId } from '../data/crowdbKvServers';
import { DEFAULT_DC_ID, DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, remoteReplicaLabel, serverLabel, storeLabel, toDisplayState, toUiReplicaRole } from '../utils/entityDisplay';

/**
 * React Flow node data shared by both views. `entity` is the selectable
 * identity (without `viewMode`, which the canvas stamps on click). `layer`
 * drives the deterministic layout in `layout.ts`.
 */
export interface FlowNodeData {
  kind: 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'LocalReplica' | 'RemoteReplica' | 'DiskGroup';
  label: string;
  sublabel?: string;
  health?: string;
  role?: string;
  /** Remote-replica reachability (physical view); false renders a warning. */
  reachable?: boolean;
  /** Whether this replica is the group leader. */
  leader?: boolean;
  layer: number;
  entity?: Omit<SelectedEntity, 'viewMode'>;
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

function physicalGroupHealth(group: NodeStore['groups'][number]): string {
  const state = String(group.local.state || '').toLowerCase();
  if (state === ReplicaState.Failed || state === ReplicaState.Draining) {
    return 'unavailable';
  }
  if (group.leader_hint == null) {
    return 'degraded';
  }
  if (state === ReplicaState.Running) {
    return 'healthy';
  }
  return 'unknown';
}

const LEADER_EDGE = {
  type: 'smoothstep',
  animated: true,
  style: { stroke: 'var(--color-accent, #88c0d0)' },
  markerEnd: { type: MarkerType.ArrowClosed },
};

const REMOTE_EDGE = {
  type: 'smoothstep',
  style: { stroke: 'var(--color-remote, #8b5cf6)', strokeDasharray: '4 3' },
  markerEnd: { type: MarkerType.ArrowClosed, color: '#8b5cf6' },
};

/** Local-replica node id for the physical view. */
function localId(nodeId: string, storeId: string | number, groupId: string | number, replicaId: string | number) {
  return `LR-${nodeId}-${storeId}-${groupId}-${replicaId}`;
}

/**
 * Physical view: Rack -> Node -> Server -> PxStore -> PxGroup ->
 * { LocalReplica, RemoteReplica }. Remote replicas draw a dashed edge to the
 * matching LocalReplica on the peer node — a missing edge is exactly the
 * mis-wiring this view exists to surface. Source is the per-node store detail
 * (`nodeStores`), which carries each node's true local + remotes list.
 */
export function buildPhysicalFlow(
  racks: Rack[],
  nodes: NodeEntity[],
  servers: CrowdbKVServerView[],
  nodeStores: Record<string, NodeStore[]> = {},
  nodeHealthById: Record<string, NodeHealth> = {},
  diskdbNodeIds: Set<number> = new Set(),
  diskdbInstances: { instance_id: number; owned_dg_ids: number[] }[] = [],
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

    if (!server) continue;

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

    for (const store of nodeStores[node.id] || []) {
      const sid = String(store.store_id);
      const storeNodeId = `S-${node.id}-${sid}`;
      flowNodes.push(
        mkNode(storeNodeId, {
          kind: 'Store',
          label: storeLabel(sid),
          sublabel: `${store.groups?.length ?? 0} group(s)`,
          layer: 4,
          entity: { type: 'Store', id: sid, parentIds: { rack_id: node.rack_id, node_id: node.id } },
        }),
      );
      flowEdges.push({ id: `e-${serverNodeId}-${storeNodeId}`, source: serverNodeId, target: storeNodeId, type: 'smoothstep' });

      for (const group of store.groups || []) {
        const gid = String(group.group_id);
        const groupNodeId = `G-${node.id}-${sid}-${gid}`;
        const leaderRid = group.leader_hint != null ? String(group.leader_hint) : null;
        flowNodes.push(
          mkNode(groupNodeId, {
            kind: 'Group',
            label: groupLabel(gid),
            sublabel: leaderRid ? `leader ${leaderRid}` : `${group.remotes?.length ?? 0} peer(s)`,
            health: physicalGroupHealth(group),
            layer: 5,
            entity: { type: 'Group', id: gid, parentIds: { rack_id: node.rack_id, node_id: node.id, store_id: sid } },
          }),
        );
        flowEdges.push({ id: `e-${storeNodeId}-${groupNodeId}`, source: storeNodeId, target: groupNodeId, type: 'smoothstep' });

        // Local replica.
        const local = group.local;
        const localNodeId = localId(String(node.id), sid, gid, local.replica_id);
        flowNodes.push(
          mkNode(localNodeId, {
            kind: 'LocalReplica',
            label: localReplicaLabel(local.replica_id),
            sublabel: toDisplayState(String(local.role)),
            health: local.state,
            role: local.role,
            leader: leaderRid === String(local.replica_id),
            layer: 6,
            entity: {
              type: 'Replica',
              id: String(local.replica_id),
              parentIds: { rack_id: node.rack_id, node_id: node.id, store_id: sid, group_id: gid, role: local.role },
            },
          }),
        );
        flowEdges.push({ id: `e-${groupNodeId}-${localNodeId}`, source: groupNodeId, target: localNodeId, type: 'smoothstep' });

        // Remote replicas (peer proxies). Dashed edge to the peer's local glyph.
        for (const remote of group.remotes || []) {
          const remoteNodeId = `RR-${node.id}-${sid}-${gid}-${remote.replica_id}`;
          flowNodes.push(
            mkNode(remoteNodeId, {
              kind: 'RemoteReplica',
              label: remoteReplicaLabel(remote.replica_id),
              sublabel: nodeLabel(String(remote.node_id)),
              reachable: remote.reachable,
              layer: 6,
              entity: {
                type: 'Replica',
                id: String(remote.replica_id),
                parentIds: {
                  rack_id: node.rack_id,
                  node_id: remote.node_id,
                  store_id: sid,
                  group_id: gid,
                  remote_on: node.id,
                  reachable: String(remote.reachable),
                },
              },
            }),
          );
          flowEdges.push({ id: `e-${groupNodeId}-${remoteNodeId}`, source: groupNodeId, target: remoteNodeId, type: 'smoothstep' });
          // Peer-wiring edge to the matching LocalReplica on the peer node.
          flowEdges.push({
            id: `e-peer-${remoteNodeId}`,
            source: remoteNodeId,
            target: localId(String(remote.node_id), sid, gid, remote.replica_id),
            ...REMOTE_EDGE,
          });
        }
      }
    }

    // DiskDB server node + owned disk-group children.
    if (diskdbNodeIds.has(node.id)) {
      const ddbNodeId = `DDB-${node.id}`;
      const ddbInstance = diskdbInstances.find((i) => i.instance_id === node.id);
      const ownedDgIds = ddbInstance?.owned_dg_ids || [];
      flowNodes.push(
        mkNode(ddbNodeId, {
          kind: 'Server',
          label: `DDB-${node.id}`,
          sublabel: ownedDgIds.length > 0 ? `${ownedDgIds.length} DG(s)` : 'no DGs',
          layer: 3,
          entity: { type: 'Server', id: `DDB-${node.id}`, parentIds: { rack_id: node.rack_id, node_id: node.id }, serviceType: 'diskdb' },
        }),
      );
      flowEdges.push({ id: `e-N-${node.id}-DDB`, source: `N-${node.id}`, target: ddbNodeId, type: 'smoothstep' });

      for (const dgId of ownedDgIds) {
        const dgNodeId = `DDBG-${node.id}-${dgId}`;
        flowNodes.push(
          mkNode(dgNodeId, {
            kind: 'DiskGroup',
            label: `DG-${dgId}`,
            sublabel: '',
            layer: 4,
            entity: { type: 'DiskGroup', id: String(dgId), parentIds: { rack_id: node.rack_id, node_id: node.id, disk_group_id: dgId } },
          }),
        );
        flowEdges.push({ id: `e-${ddbNodeId}-${dgNodeId}`, source: ddbNodeId, target: dgNodeId, type: 'smoothstep' });
      }
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
 * Capacity view: Rack -> Node. Shows the physical hierarchy for
 * disk-management operations. Node sublabel indicates whether a
 * DiskDB instance is deployed.
 */
export function buildCapacityFlow(
  racks: Rack[],
  nodes: NodeEntity[],
  diskdbNodeIds: Set<number> = new Set(),
  nodeHealthById: Record<string, NodeHealth> = {},
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
  }

  return { nodes: flowNodes, edges: flowEdges };
}

export function buildFlowForViewMode(
  viewMode: ViewMode,
  racks: Rack[],
  nodes: NodeEntity[],
  servers: CrowdbKVServerView[],
  stores: EnrichedStoreView[],
  nodeStores: Record<string, NodeStore[]> = {},
  nodeHealthById: Record<string, NodeHealth> = {},
  diskdbNodeIds: Set<number> = new Set(),
  diskdbInstances: { instance_id: number; owned_dg_ids: number[] }[] = [],
): { nodes: Node[]; edges: Edge[] } {
  switch (viewMode) {
    case ViewMode.Physical:
      return buildPhysicalFlow(racks, nodes, servers, nodeStores, nodeHealthById, diskdbNodeIds, diskdbInstances);
    case ViewMode.Capacity:
      return buildCapacityFlow(racks, nodes, diskdbNodeIds, nodeHealthById);
    default:
      return buildLogicalFlow(stores);
  }
}
