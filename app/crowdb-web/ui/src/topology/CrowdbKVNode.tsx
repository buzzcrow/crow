// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { memo } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import { FolderTree, Monitor, Database, Boxes, HardDrive, RadioTower, Cog, Crown, AlertTriangle, Building2 } from 'lucide-react';
import { cn } from '../utils/cn';
import { toUiHealth } from '../utils/entityDisplay';

interface CrowdbKVNodeData {
  kind: 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'LocalReplica' | 'RemoteReplica' | 'DiskGroup' | 'Disk';
  label: string;
  sublabel?: string;
  diskLabels?: string[];
  health?: string;
  role?: string;
  /** Remote-replica reachability; false renders a warning glyph. */
  reachable?: boolean;
  /** Whether this replica is the group leader (crown badge). */
  leader?: boolean;
  /** Set when the node is the current selection. */
  isSelected?: boolean;
}

const iconForKind: Record<CrowdbKVNodeData['kind'], typeof FolderTree> = {
  Datacenter: Building2,
  Rack: FolderTree,
  Node: Monitor,
  Server: Cog,
  Store: Database,
  Group: Boxes,
  Replica: HardDrive,
  LocalReplica: HardDrive,
  RemoteReplica: RadioTower,
  DiskGroup: Boxes,
  Disk: HardDrive,
};

const accentForKind: Record<CrowdbKVNodeData['kind'], string> = {
  Datacenter: 'tw-text-accent',
  Rack: 'tw-text-accent',
  Node: 'tw-text-accent2',
  Server: 'tw-text-accent',
  Store: 'tw-text-accent',
  Group: 'tw-text-healthy',
  Replica: 'tw-text-healthy',
  LocalReplica: 'tw-text-healthy',
  RemoteReplica: 'tw-text-remote',
  DiskGroup: 'tw-text-accent',
  Disk: 'tw-text-accent2',
};

const surfaceForKind: Record<CrowdbKVNodeData['kind'], string> = {
  Datacenter: 'tw-bg-panel tw-border-border',
  Rack: 'tw-bg-panel tw-border-border',
  Node: 'tw-bg-panel tw-border-border',
  Server: 'tw-bg-accent2/10 tw-border-accent2/30',
  Store: 'tw-bg-accent/10 tw-border-accent/30',
  Group: 'tw-bg-healthy/10 tw-border-healthy/30',
  Replica: 'tw-bg-healthy/10 tw-border-healthy/30',
  LocalReplica: 'tw-bg-healthy/10 tw-border-healthy/30',
  RemoteReplica: 'tw-bg-remote/10 tw-border-remote/30',
  DiskGroup: 'tw-bg-accent/10 tw-border-accent/30',
  Disk: 'tw-bg-accent2/10 tw-border-accent2/30',
};

/**
 * Single, unified node renderer used by both physical and logical views.
 * Variant (icon/accent) is derived from `data.kind`; visual state
 * (highlighted/dimmed/selected) is set imperatively by TopologyCanvas.
 */
function CrowdbKVNodeBase({ data }: NodeProps<CrowdbKVNodeData>) {
  const Icon = iconForKind[data.kind] || FolderTree;
  const accent = accentForKind[data.kind] || 'tw-text-text';
  const surface = surfaceForKind[data.kind] || 'tw-bg-panel tw-border-border';
  const isRemote = data.kind === 'RemoteReplica';
  const unreachable = isRemote && data.reachable === false;
  const uiHealth = toUiHealth(data.health);

  return (
    <div
      className={cn(
        'tw-border tw-rounded-lg tw-px-3 tw-py-2 tw-min-w-[160px] tw-shadow-sm tw-transition-all',
        surface,
        data.isSelected ? 'tw-ring-2 tw-ring-accent/50 tw-shadow-accent/30' : '',
        isRemote && 'tw-border-dashed tw-border-remote',
        data.leader && 'tw-ring-2 tw-ring-yellow-400/70',
        unreachable && 'tw-border-failed',
      )}
    >
      <Handle type="target" position={Position.Top} className="tw-opacity-0" />
      <div className="tw-flex tw-items-center tw-gap-2">
        <Icon className={cn('tw-h-4 tw-w-4 tw-flex-shrink-0', accent)} />
        <div className="tw-flex-1 tw-min-w-0">
          <div className="tw-flex tw-items-center tw-gap-1">
            <span className="tw-text-sm tw-font-medium tw-text-text tw-truncate">{data.label}</span>
            {data.leader && <Crown className="tw-h-3.5 tw-w-3.5 tw-text-yellow-400 tw-flex-shrink-0" />}
          </div>
          {data.sublabel && (
            <div className="tw-text-[10px] tw-text-muted tw-truncate">{data.sublabel}</div>
          )}
        </div>
        {unreachable && (
          <AlertTriangle className="tw-h-3.5 tw-w-3.5 tw-text-failed tw-flex-shrink-0" aria-label="unreachable" />
        )}
        {data.health && (
          <span
            className={cn(
              'tw-h-2 tw-w-2 tw-rounded-full tw-flex-shrink-0',
              uiHealth === 'Healthy'
                ? 'tw-bg-healthy'
                : uiHealth === 'Degraded'
                  ? 'tw-bg-yellow-400'
                  : uiHealth === 'Failed'
                    ? 'tw-bg-failed'
                    : 'tw-bg-unknown',
            )}
            title={`Health: ${data.health}`}
          />
        )}
      </div>
      {data.diskLabels && data.diskLabels.length > 0 && (
        <div data-testid="compact-disk-stack" className="tw-mt-2 tw-space-y-1">
          {data.diskLabels.map((label) => (
            <div key={label} className="tw-flex tw-items-center tw-gap-1.5 tw-border tw-border-border tw-rounded tw-bg-bg/60 tw-px-2 tw-py-1 tw-text-[10px] tw-text-muted tw-font-mono">
              <HardDrive className="tw-h-3 tw-w-3 tw-flex-shrink-0 tw-text-accent2" />
              {label}
            </div>
          ))}
        </div>
      )}
      <Handle type="source" position={Position.Bottom} className="tw-opacity-0" />
    </div>
  );
}

export const CrowdbKVNode = memo(CrowdbKVNodeBase);
