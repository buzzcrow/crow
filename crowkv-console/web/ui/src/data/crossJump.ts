import type { NodeGroup, ReplicaView, NodeStore, GroupView } from '../types';
import { ViewMode } from '../types'; // Import as value since we use it in comparisons

/**
 * Find the corresponding physical LocalReplica for a logical ReplicaView
 * @param logicalReplica The logical replica to find
 * @param physicalTree The physical tree data (nodes with their stores and groups)
 * @returns The physical LocalReplica and its parent entities, or undefined if not found
 */
export function findPhysicalReplicaForLogicalReplica(
  logicalReplica: ReplicaView,
  physicalNodes: Array<{ id: string; stores?: NodeStore[] }>
): {
  node: { id: string };
  store: NodeStore;
  group: NodeGroup;
  localReplica: { replica_id: string; role: any; state: any };
} | undefined {
  // Find the node that hosts this replica
  const node = physicalNodes.find(n => n.id === logicalReplica.node_id);
  if (!node?.stores) return undefined;

  // Find the store on this node
  const store = node.stores.find(s => s.store_id === logicalReplica.store_id);
  if (!store?.groups) return undefined;

  // Find the group on this store
  const group = store.groups.find(g => g.group_id === logicalReplica.group_id);
  if (!group?.local) return undefined;

  // Find the local replica
  const localReplica = group.local;
  if (localReplica.replica_id !== logicalReplica.replica_id) return undefined;

  return {
    node: { id: node.id },
    store,
    group,
    localReplica,
  };
}

/**
 * Find the corresponding logical ReplicaView for a physical LocalReplica
 * @param nodeId The node ID where the local replica resides
 * @param storeId The store ID of the replica
 * @param groupId The group ID of the replica
 * @param localReplicaId The local replica ID
 * @param logicalTree The logical tree data (stores with their groups and replicas)
 * @returns The logical ReplicaView and its parent entities, or undefined if not found
 */
export function findLogicalReplicaForPhysicalReplica(
  nodeId: string,
  storeId: string,
  groupId: string,
  localReplicaId: string,
  logicalStores: Array<{ store_id: string; groups?: Array<{ group_id: string; replicas?: ReplicaView[] }> }>
): {
  store: { store_id: string };
  group: { group_id: string };
  replica: ReplicaView;
} | undefined {
  // Find the store
  const store = logicalStores.find(s => s.store_id === storeId);
  if (!store?.groups) return undefined;

  // Find the group
  const group = store.groups.find(g => g.group_id === groupId);
  if (!group?.replicas) return undefined;

  // Find the replica that is on this node with the matching ID
  const replica = group.replicas.find(r => r.node_id === nodeId && r.replica_id === localReplicaId);
  if (!replica) return undefined;

  return {
    store: { store_id: store.store_id },
    group: { group_id: group.group_id },
    replica,
  };
}

/**
 * Find the corresponding logical GroupView for a physical NodeGroup
 * @param storeId The store ID of the group
 * @param groupId The group ID
 * @param logicalStores The logical tree data
 * @returns The logical GroupView, or undefined if not found
 */
export function findLogicalGroupForPhysicalGroup(
  storeId: string,
  groupId: string,
  logicalStores: Array<{ store_id: string; groups?: Array<{ group_id: string }> }>
): GroupView | undefined {
  const store = logicalStores.find(s => s.store_id === storeId);
  if (!store?.groups) return undefined;
  return store.groups.find(g => g.group_id === groupId) as GroupView | undefined;
}

/**
 * Find all corresponding physical NodeGroups for a logical GroupView
 * @param storeId The store ID of the group
 * @param groupId The group ID
 * @param physicalNodes The physical tree data
 * @returns Array of NodeGroups from all nodes that host this group
 */
export function findPhysicalGroupsForLogicalGroup(
  storeId: string,
  groupId: string,
  physicalNodes: Array<{ id: string; stores?: NodeStore[] }>
): Array<{ nodeId: string; group: NodeGroup }> {
  const physicalGroups: Array<{ nodeId: string; group: NodeGroup }> = [];

  for (const node of physicalNodes) {
    if (!node.stores) continue;
    const store = node.stores.find(s => s.store_id === storeId);
    if (!store?.groups) continue;
    const group = store.groups.find(g => g.group_id === groupId);
    if (group) {
      physicalGroups.push({ nodeId: node.id, group });
    }
  }

  return physicalGroups;
}

/**
 * Get the cross-jump target for an entity when switching view modes
 * @param currentViewMode The current view mode
 * @param entityType The type of entity
 * @param entity The entity data
 * @returns Information about where to jump in the other view mode
 */
export function getCrossJumpTarget(
  currentViewMode: ViewMode,
  entityType: 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica',
  entity: any
): {
  targetViewMode: ViewMode;
  targetEntityType: 'Store' | 'Group' | 'Replica' | 'Node' | 'Server';
  targetIds: Record<string, string>;
} | undefined {
  if (currentViewMode === ViewMode.Physical) {
    // Jumping from physical to logical
    switch (entityType) {
      case 'Store':
        return {
          targetViewMode: ViewMode.Logical,
          targetEntityType: 'Store',
          targetIds: { storeId: entity.store_id },
        };
      case 'Group':
        return {
          targetViewMode: ViewMode.Logical,
          targetEntityType: 'Group',
          targetIds: { storeId: entity.store_id, groupId: entity.group_id },
        };
      case 'Replica':
        return {
          targetViewMode: ViewMode.Logical,
          targetEntityType: 'Replica',
          targetIds: {
            storeId: entity.store_id,
            groupId: entity.group_id,
            replicaId: entity.replica_id,
          },
        };
      default:
        // Racks, Nodes, Servers don't have direct logical equivalents
        return undefined;
    }
  } else {
    // Jumping from logical to physical
    switch (entityType) {
      case 'Replica':
        return {
          targetViewMode: ViewMode.Physical,
          targetEntityType: 'Server',
          targetIds: { nodeId: entity.node_id },
        };
      case 'Group':
      case 'Store':
        // Groups and stores span multiple nodes, so we can't jump to a single physical entity
        return undefined;
      default:
        return undefined;
    }
  }
}
