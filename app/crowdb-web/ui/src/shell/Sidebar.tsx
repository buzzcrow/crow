// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useMemo } from 'react';
import { Search, FolderTree, Monitor, Database, Boxes, HardDrive, Cog, Plus, Rocket, Building2 } from 'lucide-react';
import { useDomain } from '../contexts/DomainContext';
import { Tree, TreeNode } from '../components/Tree';
import { Button } from '../components/ui/Button';
import { Domain, Rack, EnrichedStoreView, NodeStore, CrowdbKVServerView, NodeHealth, DiskdbInstanceInfo, CapacityUsageResponse, HardwareCapacitySummary } from '../types';
import { crowdbKvServerByNodeId } from '../data/crowdbKvServers';
import { DEFAULT_DC_ID, DEFAULT_DC_NAME } from '../data/defaultDatacenter';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, serverLabel, storeLabel, toUiHealth, toUiReplicaRole, toUiRole } from '../utils/entityDisplay';
import type { NodeDiskGroups } from '../data/useCapacityTree';

/** Fixed UI-only datacenter root wrapping the rack/store children. */
function datacenterRoot(children: TreeNode[]): TreeNode {
  return {
    id: `DC-${DEFAULT_DC_ID}`,
    rawId: DEFAULT_DC_ID,
    label: DEFAULT_DC_NAME,
    type: 'Datacenter',
    icon: <Building2 className="tw-h-4 tw-w-4 tw-text-muted" />,
    children,
  };
}

interface SidebarProps {
  racks?: Rack[];
  servers?: CrowdbKVServerView[];
  stores?: EnrichedStoreView[];
  nodeStores?: Record<string, NodeStore[]>;
  nodeHealthById?: Record<string, NodeHealth>;
  loading?: boolean;
  readonly?: boolean;
  width?: number;
  clusterInitialized?: boolean;
  onNodeClick?: (node: TreeNode) => void;
  onNodeContextMenu?: (node: TreeNode, event: React.MouseEvent) => void;
  onAdd?: () => void;
  // Capacity view props (R77)
  diskdbInstances?: DiskdbInstanceInfo[];
  capacityUsage?: CapacityUsageResponse | null;
  hardwareCapacity?: HardwareCapacitySummary | null;
  nodeDiskGroups?: Record<number, NodeDiskGroups>;
  diskdbNodeIds?: Set<number>;
  diskdbHealthById?: Map<number, string>;
  diskdbInstanceIdByNodeId?: Map<number, string>;
}

export function Sidebar({
  racks = [],
  servers = [],
  stores = [],
  nodeStores = {},
  nodeHealthById = {},
  loading,
  readonly,
  width = 280,
  clusterInitialized = true,
  onNodeClick,
  onNodeContextMenu,
  onAdd,
  diskdbInstances = [],
  capacityUsage = null,
  hardwareCapacity = null,
  nodeDiskGroups = {},
  diskdbNodeIds,
  diskdbHealthById,
  diskdbInstanceIdByNodeId = new Map(),
}: SidebarProps) {
  const { domain } = useDomain();
  const [filterQuery, setFilterQuery] = useState('');
  const serverByNodeId = useMemo(() => crowdbKvServerByNodeId(servers), [servers]);

  const treeNodes = useMemo<TreeNode[]>(() => {
    if (domain === Domain.Cluster) {
      // Cluster domain: rack → node → services → owned disk groups and disks.
      // No KV stores/groups — those live in the KV domain.
      if (racks.length === 0) return [];

      // Build lookup maps for status badges.
      const dgStatusByKey = new Map<string, number>();
      const diskStatusById = new Map<string, number>();
      if (hardwareCapacity?.disk_groups) {
        for (const dg of hardwareCapacity.disk_groups) {
          dgStatusByKey.set(`${dg.rack_id}:${dg.node_id}:${dg.disk_group_id}`, dg.status);
          for (const disk of dg.disks || []) {
            diskStatusById.set(disk.disk_id, disk.status);
          }
        }
      }
      if (capacityUsage?.disk_groups) {
        for (const dg of capacityUsage.disk_groups) {
          const key = `${dg.rack_id}:${dg.node_id}:${dg.disk_group_id}`;
          if (!dgStatusByKey.has(key)) dgStatusByKey.set(key, dg.status);
          for (const disk of dg.disks || []) {
            if (!diskStatusById.has(disk.disk_id)) diskStatusById.set(disk.disk_id, disk.status);
          }
        }
      }

      return [datacenterRoot(racks.map((rack) => ({
        id: `R-${rack.id}`,
        rawId: rack.id,
        label: rack.name ? `${rackLabel(String(rack.id))} (${rack.name})` : rackLabel(String(rack.id)),
        type: 'Rack' as const,
        icon: <FolderTree className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (rack.nodes || []).map((entry) => {
          const nodeId: number = entry.id;
          const diskdbInstanceId = diskdbInstanceIdByNodeId.get(nodeId);
          const diskdbInstance = diskdbInstances.find((instance) => instance.instance_id === diskdbInstanceId);
          const ownedDgIds = new Set(diskdbInstance?.owned_dg_ids || []);
          const children: TreeNode[] = [];

          // Cluster projects services and DiskDB-owned disk groups.
          const server = serverByNodeId.get(nodeId);
          if (server) {
            // Build Store > Group > Replica children for replicas hosted
            // on this node, so users can see which logical entities each
            // KV server owns.
            const storeChildren: TreeNode[] = [];
            for (const store of stores) {
              const sid = String(store.store_id);
              const groupChildren: TreeNode[] = [];
              for (const group of store.groups || []) {
                const gid = String(group.group_id);
                const replicasOnNode = (group.replicas || []).filter((r) => String(r.node_id) === String(nodeId));
                if (replicasOnNode.length === 0) continue;
                groupChildren.push({
                  id: `G-${nodeId}-${sid}-${gid}`,
                  rawId: gid,
                  label: groupLabel(gid),
                  type: 'Group' as const,
                  icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
                  health: toUiHealth(group.state),
                  parentIds: { node_id: nodeId, store_id: sid },
                  children: replicasOnNode.map((r) => ({
                    id: `LR-${nodeId}-${sid}-${gid}-${r.replica_id}`,
                    rawId: String(r.replica_id),
                    label: localReplicaLabel(String(r.replica_id)),
                    type: 'Replica' as const,
                    icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
                    role: toUiReplicaRole(String(r.role), String(r.state)),
                    health: toUiHealth(String(r.state)),
                    parentIds: { node_id: nodeId, store_id: sid, group_id: gid },
                  })),
                });
              }
              if (groupChildren.length > 0) {
                storeChildren.push({
                  id: `S-${nodeId}-${sid}`,
                  rawId: sid,
                  label: store.name ? `${storeLabel(sid)} (${store.name})` : storeLabel(sid),
                  type: 'Store' as const,
                  icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
                  parentIds: { node_id: nodeId },
                  children: groupChildren,
                });
              }
            }
            children.push({
              id: `KV-${nodeId}`,
              rawId: server.id,
              label: serverLabel(String(nodeId)),
              type: 'Server',
              icon: <Cog className="tw-h-4 tw-w-4 tw-text-muted" />,
              health: toUiHealth(server.process.health),
              serviceType: 'kv',
              parentIds: { rack_id: rack.id, node_id: nodeId },
              children: storeChildren.length > 0 ? storeChildren : undefined,
            });
          }

          if (diskdbNodeIds?.has(nodeId)) {
            const diskGroups = Object.values(nodeDiskGroups).flatMap((entry) =>
              entry.diskGroups
                .filter((dg) => ownedDgIds.has(dg.id))
                .map((dg) => ({ dg, disks: entry.disksByDg[dg.id] || [] })),
            );
            children.push({
              id: `DDB-${nodeId}`,
              rawId: `${nodeId}-ddb`,
              label: `DDB-${nodeId}`,
              type: 'Server',
              icon: <Cog className="tw-h-4 tw-w-4 tw-text-muted" />,
              health: toUiHealth(diskdbHealthById?.get(nodeId)),
              serviceType: 'diskdb',
              parentIds: { rack_id: rack.id, node_id: nodeId },
              children: diskGroups.map(({ dg, disks }) => {
                const dgStatus = dgStatusByKey.get(`${dg.rack_id}:${dg.node_id}:${dg.id}`);
                return {
                  id: `CL-DG-${dg.node_id}-${dg.id}`,
                  rawId: dg.id,
                  label: dg.name ? `${dg.name} (DG-${dg.id})` : `DG-${dg.id}`,
                  type: 'DiskGroup' as const,
                  icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
                  hwStatus: dgStatus ?? undefined,
                  parentIds: { rack_id: dg.rack_id, node_id: dg.node_id, disk_group_id: dg.id },
                  children: disks.map((d) => ({
                    id: `CL-D-${dg.node_id}-${dg.id}-${d.disk_id}`,
                    rawId: d.disk_id,
                    label: d.disk_id.slice(0, 12) + '…',
                    type: 'Disk' as const,
                    icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
                    hwStatus: diskStatusById.get(d.disk_id) ?? undefined,
                    parentIds: { rack_id: dg.rack_id, node_id: dg.node_id, disk_group_id: dg.id, disk_id: d.disk_id },
                  })),
                };
              }),
            });
          }

          return {
            id: `N-${nodeId}`,
            rawId: nodeId,
            label: nodeLabel(String(nodeId)),
            type: 'Node' as const,
            icon: <Monitor className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(nodeHealthById[String(nodeId)]),
            parentIds: { rack_id: rack.id },
            children: children.length ? children : undefined,
          };
        }),
      })))];
    }

    if (domain === Domain.KV) {
      // KV domain is logical only: datacenter → store → group → replica.
      if (stores.length === 0) return [];
      return [datacenterRoot(stores.map((store) => ({
        id: `S-${store.store_id}`,
        rawId: String(store.store_id),
        label: store.name ? `${storeLabel(String(store.store_id))} (${store.name})` : storeLabel(String(store.store_id)),
        type: 'Store' as const,
        icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (store.groups || []).map((group) => ({
          id: `G-${store.store_id}-${group.group_id}`,
          rawId: String(group.group_id),
          label: groupLabel(String(group.group_id)),
          type: 'Group' as const,
          icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
          health: toUiHealth(group.state),
          parentIds: { store_id: String(store.store_id) },
          children: (group.replicas || []).map((replica) => ({
            id: `LR-${store.store_id}-${group.group_id}-${replica.replica_id}`,
            rawId: String(replica.replica_id),
            label: localReplicaLabel(replica.replica_id),
            type: 'Replica' as const,
            icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
            role: toUiRole(String(replica.role)),
            health: toUiHealth(String(replica.state)),
            parentIds: { store_id: String(store.store_id), group_id: String(group.group_id), node_id: String(replica.node_id) },
          })),
        })),
      })))]
    }

    if (domain === Domain.Chunk) {
      // Chunk domain: datacenter → rack → node → physical disk groups/disks
      // plus a separate DiskDB service item.
      if (racks.length === 0) return [];

      const dgStatusByKey = new Map<string, number>();
      const diskStatusById = new Map<string, number>();
      if (hardwareCapacity?.disk_groups) {
        for (const dg of hardwareCapacity.disk_groups) {
          dgStatusByKey.set(`${dg.rack_id}:${dg.node_id}:${dg.disk_group_id}`, dg.status);
          for (const disk of dg.disks || []) {
            diskStatusById.set(disk.disk_id, disk.status);
          }
        }
      }
      if (capacityUsage?.disk_groups) {
        for (const dg of capacityUsage.disk_groups) {
          const key = `${dg.rack_id}:${dg.node_id}:${dg.disk_group_id}`;
          if (!dgStatusByKey.has(key)) dgStatusByKey.set(key, dg.status);
          for (const disk of dg.disks || []) {
            if (!diskStatusById.has(disk.disk_id)) diskStatusById.set(disk.disk_id, disk.status);
          }
        }
      }

      return [datacenterRoot(racks.map((rack) => ({
        id: `R-${rack.id}`,
        rawId: rack.id,
        label: rack.name ? `${rackLabel(String(rack.id))} (${rack.name})` : rackLabel(String(rack.id)),
        type: 'Rack' as const,
        icon: <FolderTree className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (rack.nodes || []).map((entry) => {
          const nodeId: number = entry.id;
          const children: TreeNode[] = [];
          const ndg = nodeDiskGroups[nodeId];
          const allDgs = ndg?.diskGroups || [];

          // Chunk owns the physical disk hierarchy. DiskDB is a separate
          // sibling item and never contains owned disk-group children.
          for (const dg of allDgs) {
            const disks = ndg?.disksByDg[dg.id] || [];
            const dgStatus = dgStatusByKey.get(`${rack.id}:${nodeId}:${dg.id}`);
            children.push({
              id: `CH-DG-${nodeId}-${dg.id}`,
              rawId: dg.id,
              label: dg.name ? `${dg.name} (DG-${dg.id})` : `DG-${dg.id}`,
              type: 'DiskGroup' as const,
              icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
              hwStatus: dgStatus ?? undefined,
              parentIds: { rack_id: rack.id, node_id: nodeId, disk_group_id: dg.id },
              children: disks.map((d) => ({
                id: `CH-D-${nodeId}-${dg.id}-${d.disk_id}`,
                rawId: d.disk_id,
                label: d.disk_id.slice(0, 12) + '…',
                type: 'Disk' as const,
                icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
                hwStatus: diskStatusById.get(d.disk_id) ?? undefined,
                parentIds: { rack_id: rack.id, node_id: nodeId, disk_group_id: dg.id, disk_id: d.disk_id },
              })),
            });
          }

          return {
            id: `N-${nodeId}`,
            rawId: nodeId,
            label: nodeLabel(String(nodeId)),
            type: 'Node' as const,
            icon: <Monitor className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(nodeHealthById[String(nodeId)]),
            parentIds: { rack_id: rack.id },
            children: children.length ? children : undefined,
          };
        }),
      })))];
    }

    // Fallback (uninitialized KV domain): logical store tree.
    if (stores.length === 0) return [];
    return [datacenterRoot(stores.map((store) => {
      const sid = String(store.store_id);
      return {
        id: `S-${sid}`,
        rawId: sid,
        label: store.name ? `${storeLabel(sid)} (${store.name})` : storeLabel(sid),
        type: 'Store',
        icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (store.groups || []).map((group) => {
          const gid = String(group.group_id);
          const replicas = group.replicas;
          return {
            id: `G-${sid}-${gid}`,
            rawId: gid,
            label: groupLabel(gid),
            type: 'Group' as const,
            icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(group.state),
            parentIds: { store_id: sid },
            children: replicas.map((r) => ({
              id: `LR-${sid}-${gid}-${r.replica_id}`,
              rawId: String(r.replica_id),
              label: localReplicaLabel(r.replica_id),
              type: 'Replica' as const,
              icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
              role: toUiReplicaRole(String(r.role), String(r.state)),
              health: toUiHealth(String(r.state)),
              parentIds: { store_id: sid, group_id: gid, node_id: String(r.node_id ?? '') },
            })),
          };
        }),
      };
    }))];
  }, [nodeHealthById, nodeStores, serverByNodeId, stores, domain, racks, diskdbInstances, capacityUsage, hardwareCapacity, nodeDiskGroups, diskdbNodeIds, diskdbHealthById, diskdbInstanceIdByNodeId]);

  const filtered = useMemo(() => {
    if (!filterQuery.trim()) return treeNodes;
    const q = filterQuery.toLowerCase();
    const filterNode = (node: TreeNode): TreeNode | null => {
      const matches = node.label.toLowerCase().includes(q) || node.id.toLowerCase().includes(q);
      const kids = node.children?.map(filterNode).filter(Boolean) as TreeNode[] | undefined;
      if (matches || (kids && kids.length > 0)) return { ...node, children: kids };
      return null;
    };
    return treeNodes.map(filterNode).filter(Boolean) as TreeNode[];
  }, [treeNodes, filterQuery]);

  const expandedIds = useMemo(() => {
    const ids: string[] = [];
    const collect = (ns: TreeNode[]) => {
      for (const n of ns) {
        ids.push(n.id);
        if (n.children) collect(n.children);
      }
    };
    collect(filtered);
    return ids;
  }, [filtered]);

  return (
    <aside aria-label="Cluster tree sidebar" className="tw-h-[calc(100vh-3.5rem)] tw-mt-14 tw-border-r tw-border-border tw-bg-bg tw-flex tw-flex-col tw-overflow-hidden tw-fixed tw-left-0 tw-top-0" style={{ width }}>
      <div className="tw-p-3 tw-border-b tw-border-border">
        <div className="tw-relative">
          <Search className="tw-absolute tw-left-3 tw-top-1/2 tw--translate-y-1/2 tw-h-4 tw-w-4 tw-text-muted" />
          <input
            type="text"
            placeholder="Filter..."
            value={filterQuery}
            onChange={(e) => setFilterQuery(e.target.value)}
            className="tw-w-full tw-pl-9 tw-pr-3 tw-py-2 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-text-sm tw-text-text focus:tw-outline-none focus:tw-ring-2 focus:tw-ring-accent"
          />
        </div>
      </div>

      <div className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-border-b tw-border-border">
        <h3 className="tw-text-xs tw-font-semibold tw-text-muted tw-uppercase tw-tracking-wider">
          {domain === Domain.Cluster ? 'Cluster' : domain === Domain.KV ? 'KV' : 'Capacity'}
        </h3>
        {!readonly && onAdd && domain !== Domain.Chunk && (
          domain === Domain.KV && !clusterInitialized ? (
            <Button
              variant="secondary"
              size="sm"
              onClick={onAdd}
              aria-label="Initialize Cluster"
              className="tw-h-7 tw-px-2 tw-gap-1"
            >
              <Rocket className="tw-h-3.5 tw-w-3.5" />
              <span className="tw-text-xs">Initialize</span>
            </Button>
          ) : (
            <Button
              variant="ghost"
              size="sm"
              onClick={onAdd}
              aria-label={domain === Domain.KV ? 'Add Store' : 'Add Rack'}
              className="tw-h-7 tw-px-2"
            >
              <Plus className="tw-h-3.5 tw-w-3.5" />
            </Button>
          )
        )}
      </div>

      {loading && filtered.length === 0 ? (
        <div className="tw-p-4 tw-animate-pulse tw-space-y-2">
          <div className="tw-h-6 tw-bg-panel tw-rounded-md" />
          <div className="tw-h-6 tw-bg-panel tw-rounded-md tw-w-3/4" />
          <div className="tw-h-6 tw-bg-panel tw-rounded-md tw-w-1/2" />
        </div>
      ) : filtered.length > 0 ? (
        <Tree
          key={domain}
          nodes={filtered}
          defaultExpandedIds={expandedIds}
          onNodeClick={onNodeClick}
          onNodeContextMenu={onNodeContextMenu}
        />
      ) : (
        <div className="tw-flex tw-items-center tw-justify-center tw-flex-1 tw-text-sm tw-text-muted tw-px-4 tw-text-center">
          {filterQuery
            ? 'No matching items'
            : domain === Domain.Cluster
              ? 'No racks registered'
              : domain === Domain.Chunk
                ? 'No racks registered'
                : clusterInitialized
                  ? 'No stores yet'
                  : (
                    <div className="tw-space-y-3">
                      <div>Cluster not initialized.</div>
                      {!readonly && (
                        <Button size="sm" onClick={onAdd} leftIcon={<Rocket className="tw-h-3.5 tw-w-3.5" />}>
                          Initialize Cluster
                        </Button>
                      )}
                    </div>
                  )}
        </div>
      )}
    </aside>
  );
}
