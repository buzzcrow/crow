// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useMemo } from 'react';
import { Search, FolderTree, Monitor, Database, Boxes, HardDrive, RadioTower, Cog, Plus } from 'lucide-react';
import { useViewMode } from '../contexts/ViewModeContext';
import { Tree, TreeNode } from '../components/Tree';
import { Button } from '../components/ui/Button';
import { ViewMode, Rack, StoreView, NodeStore, CrowKVServerView, NodeHealth } from '../types';
import { crowKvServerByNodeId } from '../data/crowKvServers';
import { groupLabel, localReplicaLabel, nodeLabel, rackLabel, remoteReplicaLabel, serverLabel, storeLabel, toUiHealth, toUiReplicaRole, toUiRole } from '../utils/entityDisplay';

interface SidebarProps {
  racks?: Rack[];
  servers?: CrowKVServerView[];
  stores?: StoreView[];
  nodeStores?: Record<string, NodeStore[]>;
  nodeHealthById?: Record<string, NodeHealth>;
  loading?: boolean;
  readonly?: boolean;
  width?: number;
  onNodeClick?: (node: TreeNode) => void;
  onNodeContextMenu?: (node: TreeNode, event: React.MouseEvent) => void;
  onAdd?: () => void;
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
  onNodeClick,
  onNodeContextMenu,
  onAdd,
}: SidebarProps) {
  const { viewMode } = useViewMode();
  const [filterQuery, setFilterQuery] = useState('');
  const serverByNodeId = useMemo(() => crowKvServerByNodeId(servers), [servers]);

  const physicalGroupHealth = (group: NodeStore['groups'][number]) => {
    const state = String(group.local.state || '').toLowerCase();
    if (state === 'failed' || state === 'draining') return 'Failed' as const;
    if (group.leader_hint == null) return 'Degraded' as const;
    if (state === 'running') return 'Healthy' as const;
    return 'Unknown' as const;
  };

  const treeNodes = useMemo<TreeNode[]>(() => {
    if (viewMode === ViewMode.Physical) {
      return racks.map((rack) => ({
        id: `R-${rack.id}`,
        rawId: rack.id,
        label: rack.name ? `${rackLabel(rack.id)} (${rack.name})` : rackLabel(rack.id),
        type: 'Rack' as const,
        icon: <FolderTree className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (rack.nodes || []).map((entry: any) => {
          const nodeId: string = typeof entry === 'string' ? entry : entry.id;
          const server = serverByNodeId.get(nodeId);
          const stores = nodeStores[nodeId] || [];
          const hasServer = !!server || stores.length > 0;
          const children: TreeNode[] = [];
          if (hasServer) {
            children.push({
              id: `KV-${nodeId}`,
              rawId: server?.id || `${nodeId}-kv`,
              label: serverLabel(nodeId),
              type: 'Server',
              icon: <Cog className="tw-h-4 tw-w-4 tw-text-muted" />,
              health: toUiHealth(server?.process.health),
              parentIds: { rack_id: rack.id, node_id: nodeId },
              children: stores.map((ns) => {
                const sid = String(ns.store_id);
                return {
                  id: `S-${nodeId}-${sid}`,
                  rawId: sid,
                  label: storeLabel(sid),
                  type: 'Store' as const,
                  icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
                  parentIds: { rack_id: rack.id, node_id: nodeId },
                  children: (ns.groups || []).map((g) => {
                    const gid = String(g.group_id);
                    const replicaRows: TreeNode[] = [
                      {
                        id: `LR-${nodeId}-${sid}-${gid}-${g.local.replica_id}`,
                        rawId: String(g.local.replica_id),
                        label: localReplicaLabel(g.local.replica_id),
                        type: 'Replica' as const,
                        icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
                        role: toUiRole(String(g.local.role)),
                        parentIds: { rack_id: rack.id, node_id: nodeId, store_id: sid, group_id: gid, role: g.local.role },
                      },
                      ...(g.remotes || []).map((r) => ({
                        id: `RR-${nodeId}-${sid}-${gid}-${r.replica_id}`,
                        rawId: String(r.replica_id),
                        label: remoteReplicaLabel(r.replica_id),
                        type: 'Replica' as const,
                        icon: <RadioTower className="tw-h-4 tw-w-4 tw-text-remote" />,
                        health: r.reachable ? ('Healthy' as const) : ('Failed' as const),
                        parentIds: {
                          rack_id: rack.id,
                          node_id: String(r.node_id),
                          store_id: sid,
                          group_id: gid,
                          remote_on: nodeId,
                          reachable: String(r.reachable),
                        },
                      })),
                    ];
                    return {
                      id: `G-${nodeId}-${sid}-${gid}`,
                      rawId: gid,
                      label: groupLabel(gid),
                      type: 'Group' as const,
                      icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
                      health: physicalGroupHealth(g),
                      parentIds: { rack_id: rack.id, node_id: nodeId, store_id: sid },
                      children: replicaRows,
                    };
                  }),
                };
              }),
            });
          }
          return {
            id: `N-${nodeId}`,
            rawId: nodeId,
            label: nodeLabel(nodeId),
            type: 'Node' as const,
            icon: <Monitor className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(nodeHealthById[nodeId]),
            parentIds: { rack_id: rack.id },
            children: children.length ? children : undefined,
          };
        }),
      }));
    }

    return stores.map((store) => {
      const sid = String(store.store_id);
      return {
        id: `S-${sid}`,
        rawId: sid,
        label: store.name ? `${storeLabel(sid)} (${store.name})` : storeLabel(sid),
        type: 'Store',
        icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
        children: (store.groups || []).map((group: any) => {
          const gid = String(group.group_id);
          const replicas: any[] = Array.isArray(group.replicas) ? group.replicas : [];
          return {
            id: `G-${sid}-${gid}`,
            rawId: gid,
            label: groupLabel(gid),
            type: 'Group' as const,
            icon: <Boxes className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: toUiHealth(group.health || group.state),
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
    });
  }, [nodeHealthById, nodeStores, serverByNodeId, stores, viewMode, racks]);

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
          {viewMode === ViewMode.Physical ? 'Infrastructure' : 'Cluster'}
        </h3>
        {!readonly && onAdd && (
          <Button
            variant="ghost"
            size="sm"
            onClick={onAdd}
            aria-label={viewMode === ViewMode.Physical ? 'Add Rack' : 'Add Store'}
            className="tw-h-7 tw-px-2"
          >
            <Plus className="tw-h-3.5 tw-w-3.5" />
          </Button>
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
          key={viewMode}
          nodes={filtered}
          defaultExpandedIds={expandedIds}
          onNodeClick={onNodeClick}
          onNodeContextMenu={onNodeContextMenu}
        />
      ) : (
        <div className="tw-flex tw-items-center tw-justify-center tw-flex-1 tw-text-sm tw-text-muted tw-px-4 tw-text-center">
          {filterQuery
            ? 'No matching items'
            : viewMode === ViewMode.Physical
              ? 'No racks registered'
              : 'No stores yet'}
        </div>
      )}
    </aside>
  );
}
