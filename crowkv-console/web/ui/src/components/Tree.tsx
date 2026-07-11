import React, { useState, useCallback } from 'react';
import { ChevronRight, ChevronDown, Star, MoreVertical } from 'lucide-react';
import { cn } from '../utils/cn';
import { useSelection } from '../contexts/SelectionContext';
import { useViewMode } from '../contexts/ViewModeContext';
import { HealthBadge, RoleBadge } from './ui/Badge';

export interface TreeNode {
  /**
   * Tree-unique identifier (e.g. `rack-r1`, `node-n1`). Used as the
   * React key and for expand/collapse bookkeeping; NOT the backend id.
   */
  id: string;
  /**
   * The unprefixed backend entity id (e.g. `r1`, `n1`, `7`). API
   * handlers must use this — the prefixed `id` field will produce
   * 404s like `node node-n1 not found`. Optional only for legacy
   * favorite/recent entries built before this field existed.
   */
  rawId?: string;
  label: string;
  type: 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica';
  icon?: React.ReactNode;
  children?: TreeNode[];
  health?: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
  role?: 'Leader' | 'Follower' | 'Remote';
  isFavorite?: boolean;
  parentIds?: Record<string, string>;
  data?: any;
}

interface TreeProps {
  nodes: TreeNode[];
  defaultExpandedIds?: string[];
  onNodeClick?: (node: TreeNode) => void;
  onNodeContextMenu?: (node: TreeNode, event: React.MouseEvent) => void;
  className?: string;
}

interface TreeNodeProps {
  node: TreeNode;
  level: number;
  expandedIds: Set<string>;
  toggleExpanded: (id: string) => void;
  onNodeClick?: (node: TreeNode) => void;
  onNodeContextMenu?: (node: TreeNode, event: React.MouseEvent) => void;
}

function TreeNodeComponent({
  node,
  level,
  expandedIds,
  toggleExpanded,
  onNodeClick,
  onNodeContextMenu,
}: TreeNodeProps) {
  const { isSelected, toggleSelection } = useSelection();
  const { viewMode } = useViewMode();
  const [isContextMenuOpen, setIsContextMenuOpen] = useState(false);
  const hasChildren = node.children && node.children.length > 0;
  const isExpanded = expandedIds.has(node.id);
  const isNodeSelected = isSelected(node.id);

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      const selectedEntity = {
        type: node.type,
        id: node.id,
        name: node.label,
        parentIds: node.parentIds,
        viewMode,
      };

      if (e.ctrlKey || e.metaKey) {
        // Multi-select
        toggleSelection(selectedEntity);
      } else {
        // Single select
        if (hasChildren) {
          toggleExpanded(node.id);
        }
        onNodeClick?.(node);
      }
    },
    [node, hasChildren, toggleExpanded, onNodeClick, toggleSelection, viewMode]
  );

  const handleContextMenu = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      onNodeContextMenu?.(node, e);
      setIsContextMenuOpen(true);
    },
    [node, onNodeContextMenu]
  );

  const handleChevronClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      toggleExpanded(node.id);
    },
    [node.id, toggleExpanded]
  );

  return (
    <div>
      <div
        onClick={handleClick}
        onContextMenu={handleContextMenu}
        className={cn(
          'tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-2 tw-cursor-pointer tw-group tw-transition-colors',
          isNodeSelected ? 'tw-bg-accent/10 tw-text-accent' : 'tw-text-text hover:tw-bg-panel',
          level === 0 ? 'tw-font-medium' : 'tw-text-sm'
        )}
        style={{ paddingLeft: `${level * 16 + 12}px` }}
        role="treeitem"
        aria-selected={isNodeSelected}
        aria-expanded={hasChildren ? isExpanded : undefined}
      >
        {/* Expand/collapse chevron */}
        {hasChildren ? (
          <button
            onClick={handleChevronClick}
            className="tw-p-0.5 tw-rounded hover:tw-bg-border tw-transition-colors tw-flex-shrink-0"
            aria-label={isExpanded ? 'Collapse' : 'Expand'}
          >
            {isExpanded ? (
              <ChevronDown className="tw-h-4 tw-w-4 tw-text-muted" />
            ) : (
              <ChevronRight className="tw-h-4 tw-w-4 tw-text-muted" />
            )}
          </button>
        ) : (
          <span className="tw-w-5 tw-flex-shrink-0" />
        )}

        {/* Node icon */}
        {node.icon && <span className="tw-flex-shrink-0">{node.icon}</span>}

        {/* Node label */}
        <span className="tw-flex-1 tw-truncate">{node.label}</span>

        {/* Status badges */}
        <div className="tw-flex tw-items-center tw-gap-1 tw-flex-shrink-0">
          {node.health && <HealthBadge status={node.health} size="sm" />}
          {node.role && <RoleBadge role={node.role} size="sm" />}
          {node.isFavorite && <Star className="tw-h-3 tw-w-3 tw-text-yellow-400" />}
        </div>

        {/* Context menu button (shows on hover) */}
        <button
          onClick={e => {
            e.stopPropagation();
            setIsContextMenuOpen(!isContextMenuOpen);
          }}
          className="tw-opacity-0 group-hover:tw-opacity-100 tw-transition-opacity tw-p-0.5 tw-rounded hover:tw-bg-border tw-flex-shrink-0"
          aria-label="Open context menu"
        >
          <MoreVertical className="tw-h-3.5 tw-w-3.5 tw-text-muted" />
        </button>
      </div>

      {/* Children */}
      {hasChildren && isExpanded && (
        <div role="group">
          {node.children?.map(child => (
            <TreeNodeComponent
              key={child.id}
              node={child}
              level={level + 1}
              expandedIds={expandedIds}
              toggleExpanded={toggleExpanded}
              onNodeClick={onNodeClick}
              onNodeContextMenu={onNodeContextMenu}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export function Tree({ nodes, defaultExpandedIds = [], onNodeClick, onNodeContextMenu, className }: TreeProps) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(() => new Set(defaultExpandedIds));

  const toggleExpanded = useCallback((id: string) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }, []);

  return (
    <div className={cn('tw-overflow-y-auto tw-flex-1', className)} role="tree">
      {nodes.map(node => (
        <TreeNodeComponent
          key={node.id}
          node={node}
          level={0}
          expandedIds={expandedIds}
          toggleExpanded={toggleExpanded}
          onNodeClick={onNodeClick}
          onNodeContextMenu={onNodeContextMenu}
        />
      ))}
    </div>
  );
}
