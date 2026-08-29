// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import React, { useState, useCallback, useEffect, useRef } from 'react';
import { ChevronRight, ChevronDown } from 'lucide-react';
import { cn } from '../utils/cn';
import { useSelection } from '../contexts/SelectionContext';
import { useViewMode } from '../contexts/ViewModeContext';
import { HealthBadge, RoleBadge, HwStatusBadge } from './ui/Badge';

export interface TreeNode {
  /** Tree-unique id (e.g. `rack-r1`). React key + expand bookkeeping; NOT the backend id. */
  id: string;
  /** Unprefixed backend id (e.g. `r1`, `7`). API calls must use this. */
  rawId?: string | number;
  label: string;
  type: 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'DiskGroup' | 'Disk';
  icon?: React.ReactNode;
  children?: TreeNode[];
  health?: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
  role?: 'Leader' | 'Follower' | 'Remote';
  parentIds?: Record<string, string | number>;
  /** Service flavor for `Server` nodes: KV vs DiskDB. */
  serviceType?: 'kv' | 'diskdb';
  /** HwStatus enum (0-6) for DiskGroup/Disk in Capacity view. */
  hwStatus?: number;
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
  const { isSelected, selectEntity } = useSelection();
  const { viewMode } = useViewMode();
  const hasChildren = !!node.children && node.children.length > 0;
  const isExpanded = expandedIds.has(node.id);
  const entityId = node.rawId ?? node.id;
  const isNodeSelected = isSelected(String(entityId));

  const select = useCallback(() => {
    selectEntity({ type: node.type, id: String(entityId), name: node.label, parentIds: node.parentIds, viewMode, serviceType: node.serviceType });
  }, [node, entityId, selectEntity, viewMode]);

  const handleSelectClick = useCallback(() => {
    select();
    onNodeClick?.(node);
  }, [node, select, onNodeClick]);

  const handleContextMenu = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      select();
      onNodeContextMenu?.(node, e);
    },
    [select, node, onNodeContextMenu],
  );

  const handleChevron = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      toggleExpanded(node.id);
    },
    [node.id, toggleExpanded],
  );

  return (
    <div>
      <div
        onContextMenu={handleContextMenu}
        className={cn(
          'tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-transition-colors',
          isNodeSelected ? 'tw-bg-accent/10 tw-text-accent' : 'tw-text-text hover:tw-bg-panel',
          level === 0 ? 'tw-font-medium' : 'tw-text-sm',
        )}
        style={{ paddingLeft: `${level * 16 + 12}px` }}
        role="treeitem"
        aria-selected={isNodeSelected}
        aria-expanded={hasChildren ? isExpanded : undefined}
      >
        {hasChildren ? (
          <button
            onClick={handleChevron}
            className="tw-p-0.5 tw-rounded hover:tw-bg-border tw-flex-shrink-0"
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

        {node.icon && <span className="tw-flex-shrink-0">{node.icon}</span>}
        <button
          type="button"
          onClick={handleSelectClick}
          className="tw-flex-1 tw-min-w-0 tw-truncate tw-text-left tw-cursor-pointer"
        >
          {node.label}
        </button>

        <div className="tw-flex tw-items-center tw-gap-0.5 tw-flex-shrink-0">
          {node.role && <RoleBadge role={node.role} size="sm" compact />}
          {node.hwStatus != null && <HwStatusBadge status={node.hwStatus} size="sm" compact />}
          {node.health && <HealthBadge status={node.health} size="sm" compact />}
        </div>
      </div>

      {hasChildren && isExpanded && (
        <div role="group">
          {node.children?.map((child) => (
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
  const prevDefaultRef = useRef<Set<string>>(new Set(defaultExpandedIds));

  useEffect(() => {
    const prev = prevDefaultRef.current;
    const next = new Set(defaultExpandedIds);
    setExpandedIds((cur) => {
      const merged = new Set(cur);
      for (const id of next) {
        if (!prev.has(id)) merged.add(id);
      }
      return merged;
    });
    prevDefaultRef.current = next;
  }, [defaultExpandedIds]);

  const toggleExpanded = useCallback((id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  return (
    <div className={cn('tw-overflow-y-auto tw-flex-1', className)} role="tree">
      {nodes.map((node) => (
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
