import React, { useState, useMemo } from 'react';
import { Search, Star, Clock, ChevronDown, ChevronRight, Server, Database, HardDrive, Users, Plus } from 'lucide-react';
import { useViewMode } from '../contexts/ViewModeContext';
import { useSelection } from '../contexts/SelectionContext';
import { Tree, TreeNode } from '../components/Tree';
import { Button } from '../components/ui/Button';
import { ViewMode, Rack, StoreView } from '../types';

interface SidebarProps {
  /** Physical view data: Racks with nested nodes */
  racks?: Rack[];
  /** Logical view data: Stores with nested groups */
  stores?: StoreView[];
  /** Whether data is loading */
  loading?: boolean;
  /** Callback when a node is clicked */
  onNodeClick?: (node: TreeNode) => void;
  /** Callback when a context menu is invoked on a node */
  onNodeContextMenu?: (node: TreeNode, event: React.MouseEvent) => void;
  /** Callback when add button is clicked */
  onAdd?: () => void;
}

export function Sidebar({ racks = [], stores = [], loading, onNodeClick, onNodeContextMenu, onAdd }: SidebarProps) {
  const { viewMode } = useViewMode();
  const { favorites, recentItems, removeFromFavorites } = useSelection();
  const [filterQuery, setFilterQuery] = useState('');
  const [showFavorites, setShowFavorites] = useState(true);
  const [showRecent, setShowRecent] = useState(true);

  // Build tree nodes based on current view mode
  const treeNodes = useMemo<TreeNode[]>(() => {
    if (viewMode === ViewMode.Physical) {
      // Build physical tree: Rack → Node → Server → Store → Group
      return racks.map(rack => ({
        id: `rack-${rack.id}`,
        rawId: rack.id,
        label: rack.name || rack.id,
        type: 'Rack',
        icon: <Server className="tw-h-4 tw-w-4 tw-text-muted" />,
        health: 'Healthy', // TODO: Calculate aggregate health
        // `rack.nodes` is `NodeId[]` at recursive=0 but switches to
        // `NodeView[]` (object with `id`, `host`, `has_server`, …) at
        // recursive>=1. Normalize to the id string before rendering —
        // otherwise React error #31 fires when a `NodeView` ends up as
        // a `label` (see doc/todo_ui2.md §5.6).
        children: rack.nodes?.map((entry: any) => {
          const nodeId: string = typeof entry === 'string' ? entry : entry.id;
          return {
            id: `node-${nodeId}`,
            rawId: nodeId,
            label: nodeId,
            type: 'Node',
            icon: <HardDrive className="tw-h-4 tw-w-4 tw-text-muted" />,
            health: 'Unknown' as const,
            parentIds: { rackId: rack.id },
            // TODO: Add server, stores and groups once we have the data
          };
        }),
      }));
    } else {
      // Build logical tree: Store → Group → Replica
      return stores.map(store => ({
        id: `store-${store.store_id}`,
        rawId: String(store.store_id),
        label: store.name || String(store.store_id),
        type: 'Store',
        icon: <Database className="tw-h-4 tw-w-4 tw-text-muted" />,
        health: 'Healthy', // TODO: Calculate aggregate health
        children: store.groups?.map(group => ({
          id: `group-${group.group_id}`,
          rawId: String(group.group_id),
          label: String(group.group_id),
          type: 'Group',
          icon: <Users className="tw-h-4 tw-w-4 tw-text-muted" />,
          health: (group.health || 'Unknown') as 'Healthy' | 'Degraded' | 'Failed' | 'Unknown',
          parentIds: { store_id: String(store.store_id) },
          // TODO: Add replicas
        })),
      }));
    }
  }, [viewMode, racks, stores]);

  // Filter tree nodes based on search query
  const filteredTreeNodes = useMemo(() => {
    if (!filterQuery.trim()) return treeNodes;

    const query = filterQuery.toLowerCase();

    function filterNode(node: TreeNode): TreeNode | null {
      // Check if current node matches
      const nodeMatches = node.label.toLowerCase().includes(query) || node.id.toLowerCase().includes(query);

      // Filter children first
      const filteredChildren = node.children?.map(filterNode).filter(Boolean) as TreeNode[];

      // If node matches or has matching children, include it
      if (nodeMatches || (filteredChildren && filteredChildren.length > 0)) {
        return {
          ...node,
          children: filteredChildren,
        };
      }

      return null;
    }

    return treeNodes.map(filterNode).filter(Boolean) as TreeNode[];
  }, [treeNodes, filterQuery]);

  // Convert favorites/recent items to tree nodes for display
  const favoriteNodes = useMemo<TreeNode[]>(() => {
    return favorites.map(item => ({
      id: `fav-${item.id}`,
      label: item.name || item.id,
      type: item.type,
      isFavorite: true,
      parentIds: item.parentIds,
    }));
  }, [favorites]);

  const recentNodes = useMemo<TreeNode[]>(() => {
    return recentItems.map(item => ({
      id: `recent-${item.id}`,
      label: item.name || item.id,
      type: item.type,
      parentIds: item.parentIds,
    }));
  }, [recentItems]);

  if (loading) {
    return (
      <aside className="tw-w-64 tw-h-[calc(100vh-3.5rem)] tw-mt-14 tw-border-r tw-border-border tw-bg-bg tw-flex tw-flex-col">
        <div className="tw-p-4 tw-animate-pulse">
          <div className="tw-h-8 tw-bg-panel tw-rounded-md tw-mb-4" />
          <div className="tw-space-y-2">
            <div className="tw-h-6 tw-bg-panel tw-rounded-md" />
            <div className="tw-h-6 tw-bg-panel tw-rounded-md tw-w-3/4" />
            <div className="tw-h-6 tw-bg-panel tw-rounded-md tw-w-1/2" />
          </div>
        </div>
      </aside>
    );
  }

  return (
    <aside className="tw-w-64 tw-h-[calc(100vh-3.5rem)] tw-mt-14 tw-border-r tw-border-border tw-bg-bg tw-flex tw-flex-col tw-overflow-hidden">
      {/* Search input */}
      <div className="tw-p-3 tw-border-b tw-border-border">
        <div className="tw-relative">
          <Search className="tw-absolute tw-left-3 tw-top-1/2 tw--translate-y-1/2 tw-h-4 tw-w-4 tw-text-muted" />
          <input
            type="text"
            placeholder="Search..."
            value={filterQuery}
            onChange={e => setFilterQuery(e.target.value)}
            className="tw-w-full tw-pl-9 tw-pr-3 tw-py-2 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-text-sm tw-text-text tw-placeholder:text-muted tw-focus:outline-none tw-focus:ring-2 tw-focus:ring-accent"
          />
        </div>
      </div>

      {/* Favorites section */}
      <div className="tw-border-b tw-border-border">
        <div
          className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-cursor-pointer hover:tw-bg-panel"
          onClick={() => setShowFavorites(!showFavorites)}
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-text-sm tw-font-medium tw-text-text">
            <Star className="tw-h-4 tw-w-4 tw-text-yellow-400" />
            Favorites
            {favorites.length > 0 && (
              <span className="tw-text-xs tw-text-muted tw-ml-1">({favorites.length})</span>
            )}
          </div>
          {showFavorites ? (
            <ChevronDown className="tw-h-4 tw-w-4 tw-text-muted" />
          ) : (
            <ChevronRight className="tw-h-4 tw-w-4 tw-text-muted" />
          )}
        </div>

        {showFavorites && favoriteNodes.length > 0 && (
          <div className="tw-pb-1">
            {favoriteNodes.map(node => (
              <div
                key={node.id}
                onClick={() => onNodeClick?.(node)}
                className="tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-text-sm tw-cursor-pointer hover:tw-bg-panel"
              >
                <Star className="tw-h-3.5 tw-w-3.5 tw-text-yellow-400" />
                <span className="tw-flex-1 tw-truncate">{node.label}</span>
                <button
                  onClick={e => {
                    e.stopPropagation();
                    removeFromFavorites(node.id.replace('fav-', ''));
                  }}
                  className="tw-opacity-0 hover:tw-opacity-100 tw-text-muted hover:tw-text-text"
                  aria-label="Remove from favorites"
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
        )}

        {showFavorites && favoriteNodes.length === 0 && (
          <div className="tw-px-3 tw-py-4 tw-text-sm tw-text-muted tw-text-center">
            No favorites yet
            <br />
            Right-click items to add
          </div>
        )}
      </div>

      {/* Recent items section */}
      <div className="tw-border-b tw-border-border">
        <div
          className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-cursor-pointer hover:tw-bg-panel"
          onClick={() => setShowRecent(!showRecent)}
        >
          <div className="tw-flex tw-items-center tw-gap-2 tw-text-sm tw-font-medium tw-text-text">
            <Clock className="tw-h-4 tw-w-4 tw-text-blue-400" />
            Recent
          </div>
          {showRecent ? (
            <ChevronDown className="tw-h-4 tw-w-4 tw-text-muted" />
          ) : (
            <ChevronRight className="tw-h-4 tw-w-4 tw-text-muted" />
          )}
        </div>

        {showRecent && recentNodes.length > 0 && (
          <div className="tw-pb-1">
            {recentNodes.map(node => (
              <div
                key={node.id}
                onClick={() => onNodeClick?.(node)}
                className="tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-text-sm tw-cursor-pointer hover:tw-bg-panel"
              >
                <Clock className="tw-h-3.5 tw-w-3.5 tw-text-muted" />
                <span className="tw-flex-1 tw-truncate">{node.label}</span>
              </div>
            ))}
          </div>
        )}

        {showRecent && recentNodes.length === 0 && (
          <div className="tw-px-3 tw-py-4 tw-text-sm tw-text-muted tw-text-center">
            No recent items
          </div>
        )}
      </div>

      {/* Tree view header */}
      <div className="tw-flex tw-items-center tw-justify-between tw-px-3 tw-py-2 tw-border-b tw-border-border">
        <h3 className="tw-text-xs tw-font-semibold tw-text-muted tw-uppercase tw-tracking-wider">
          {viewMode === ViewMode.Physical ? 'Infrastructure' : 'Cluster'}
        </h3>
        {onAdd && (
          <Button
            variant="ghost"
            size="sm"
            onClick={onAdd}
            className="tw-h-7 tw-px-2"
          >
            <Plus className="tw-h-3.5 tw-w-3.5" />
          </Button>
        )}
      </div>

      {/* Tree view */}
      <div className="tw-flex-1 tw-overflow-hidden">
        {filteredTreeNodes.length > 0 ? (
          <Tree
            nodes={filteredTreeNodes}
            defaultExpandedIds={filteredTreeNodes.map(n => n.id)}
            onNodeClick={onNodeClick}
            onNodeContextMenu={onNodeContextMenu}
          />
        ) : (
          <div className="tw-flex tw-items-center tw-justify-center tw-h-full tw-text-sm tw-text-muted">
            {filterQuery ? 'No matching items' : 'No items available'}
          </div>
        )}
      </div>
    </aside>
  );
}
