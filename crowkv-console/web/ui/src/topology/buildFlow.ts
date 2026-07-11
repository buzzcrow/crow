import { Node, Edge, MarkerType } from 'reactflow';
import { Rack, Node as NodeEntity, StoreView, NodeStore, ViewMode, CrowKVServerView, NodeHealth, ReplicaState } from '../types';
import type { SelectedEntity } from '../contexts/SelectionContext';
import { crowkvServerByNodeId } from '../data/crowkvServers';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, remoteReplicaLabel, serverLabel, storeLabel, toDisplayState, toUiReplicaRole } from '../utils/entityDisplay';

/**
 * React Flow node data shared by both views. `entity` is the selectable
 * identity (without `viewMode`, which the canvas stamps on click). `layer`
 * drives the deterministic layout in `layout.ts`.
 */
export interface FlowNodeData {
  kind: 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'LocalReplica' | 'RemoteReplica';
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
  return { id, type: 'crowkv', position: { x: 0, y: 0 }, data };
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
  servers: CrowKVServerView[],
  nodeStores: Record<string, NodeStore[]> = {},
  nodeHealthById: Record<string, NodeHealth> = {},
): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];
  const serverByNodeId = crowkvServerByNodeId(servers);

  for (const rack of racks) {
    flowNodes.push(
      mkNode(`R-${rack.id}`, {
        kind: 'Rack',
        label: rackLabel(rack.id),
        sublabel: `${rack.nodes?.length ?? 0} node(s)`,
        layer: 0,
        entity: { type: 'Rack', id: rack.id, name: rack.name },
      }),
    );
  }

  for (const node of nodes) {
    const server = serverByNodeId.get(node.id);
    flowNodes.push(
      mkNode(`N-${node.id}`, {
        kind: 'Node',
        label: nodeLabel(node.id),
        sublabel: node.host,
        health: nodeHealthById[node.id],
        layer: 1,
        entity: { type: 'Node', id: node.id, parentIds: { rack_id: node.rack_id } },
      }),
    );
    flowEdges.push({ id: `e-R-${node.rack_id}-N-${node.id}`, source: `R-${node.rack_id}`, target: `N-${node.id}`, type: 'smoothstep' });

    if (!server) continue;

    const serverNodeId = `KV-${node.id}`;
    flowNodes.push(
      mkNode(serverNodeId, {
        kind: 'Server',
        label: serverLabel(node.id),
        sublabel: toDisplayState(server.process.state),
        health: server.process.health,
        layer: 2,
        entity: { type: 'Server', id: server.id, parentIds: { rack_id: node.rack_id, node_id: node.id } },
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
          layer: 3,
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
            layer: 4,
            entity: { type: 'Group', id: gid, parentIds: { rack_id: node.rack_id, node_id: node.id, store_id: sid } },
          }),
        );
        flowEdges.push({ id: `e-${storeNodeId}-${groupNodeId}`, source: storeNodeId, target: groupNodeId, type: 'smoothstep' });

        // Local replica.
        const local = group.local;
        const localNodeId = localId(node.id, sid, gid, local.replica_id);
        flowNodes.push(
          mkNode(localNodeId, {
            kind: 'LocalReplica',
            label: localReplicaLabel(local.replica_id),
            sublabel: toDisplayState(String(local.role)),
            health: local.state,
            role: local.role,
            leader: leaderRid === String(local.replica_id),
            layer: 5,
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
              sublabel: nodeLabel(remote.node_id),
              reachable: remote.reachable,
              layer: 5,
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
            target: localId(remote.node_id, sid, gid, remote.replica_id),
            ...REMOTE_EDGE,
          });
        }
      }
    }
  }

  return { nodes: flowNodes, edges: flowEdges };
}

/**
 * Logical view: Cluster -> Store -> Group -> Replica. The leader radiates
 * accent edges to followers; replicas are badged by `node_id`.
 */
export function buildLogicalFlow(stores: StoreView[]): { nodes: Node[]; edges: Edge[] } {
  const flowNodes: Node[] = [];
  const flowEdges: Edge[] = [];

  for (const store of stores) {
    const sid = String(store.store_id);
    flowNodes.push(
      mkNode(`S-${sid}`, {
        kind: 'Store',
        label: store.name ? `${storeLabel(sid)} (${store.name})` : storeLabel(sid),
        sublabel: `${store.groups?.length ?? 0} group(s)`,
        layer: 0,
        entity: { type: 'Store', id: sid, name: store.name },
      }),
    );

    for (const group of store.groups || []) {
      const gid = String(group.group_id);
      const replicas: any[] = 'replicas' in group && Array.isArray((group as any).replicas)
        ? (group as any).replicas
        : [];
      const leader = (group as any).leader ?? (group as any).leader_id;
      flowNodes.push(
        mkNode(`G-${sid}-${gid}`, {
          kind: 'Group',
          label: groupLabel(gid),
          sublabel: leader ? `leader ${leader}` : `${replicas.length} replica(s)`,
          health: (group as any).health || (group as any).state,
          layer: 1,
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
            layer: 2,
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

export function buildFlowForViewMode(
  viewMode: ViewMode,
  racks: Rack[],
  nodes: NodeEntity[],
  servers: CrowKVServerView[],
  stores: StoreView[],
  nodeStores: Record<string, NodeStore[]> = {},
  nodeHealthById: Record<string, NodeHealth> = {},
): { nodes: Node[]; edges: Edge[] } {
  return viewMode === ViewMode.Physical
    ? buildPhysicalFlow(racks, nodes, servers, nodeStores, nodeHealthById)
    : buildLogicalFlow(stores);
}
