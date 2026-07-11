import { memo } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import { Server, HardDrive, Database, Users, Cpu, Network } from 'lucide-react';
import { cn } from '../utils/cn';

interface CrowKVNodeData {
  kind: 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'LocalReplica' | 'RemoteReplica';
  label: string;
  sublabel?: string;
  health?: string;
  /** Set by the canvas when the search query matches this node. */
  isHighlighted?: boolean;
  /** Set by the canvas when focus mode dims unrelated nodes. */
  isDimmed?: boolean;
  /** Set when the node is the current selection. */
  isSelected?: boolean;
}

const iconForKind: Record<CrowKVNodeData['kind'], typeof Server> = {
  Rack: Server,
  Node: HardDrive,
  Server: Cpu,
  Store: Database,
  Group: Users,
  Replica: Network,
  LocalReplica: Network,
  RemoteReplica: Network,
};

const accentForKind: Record<CrowKVNodeData['kind'], string> = {
  Rack: 'tw-text-accent',
  Node: 'tw-text-accent2',
  Server: 'tw-text-accent2',
  Store: 'tw-text-accent',
  Group: 'tw-text-healthy',
  Replica: 'tw-text-healthy',
  LocalReplica: 'tw-text-healthy',
  RemoteReplica: 'tw-text-remote',
};

/**
 * Single, unified node renderer used by both physical and logical views.
 * Variant (icon/accent) is derived from `data.kind`; visual state
 * (highlighted/dimmed/selected) is set imperatively by TopologyCanvas.
 */
function CrowKVNodeBase({ data }: NodeProps<CrowKVNodeData>) {
  const Icon = iconForKind[data.kind] || Server;
  const accent = accentForKind[data.kind] || 'tw-text-text';

  return (
    <div
      className={cn(
        'tw-bg-panel tw-border tw-rounded-lg tw-px-3 tw-py-2 tw-min-w-[160px] tw-shadow-sm tw-transition-all',
        data.isSelected
          ? 'tw-border-accent tw-shadow-accent/30'
          : 'tw-border-border',
        data.isHighlighted && 'tw-animate-pulse-slow tw-ring-2 tw-ring-brand-accent',
        data.isDimmed && 'tw-opacity-30',
      )}
    >
      <Handle type="target" position={Position.Top} className="tw-opacity-0" />
      <div className="tw-flex tw-items-center tw-gap-2">
        <Icon className={cn('tw-h-4 tw-w-4 tw-flex-shrink-0', accent)} />
        <div className="tw-flex-1 tw-min-w-0">
          <div className="tw-text-sm tw-font-medium tw-text-text tw-truncate">{data.label}</div>
          {data.sublabel && (
            <div className="tw-text-[10px] tw-text-muted tw-truncate">{data.sublabel}</div>
          )}
        </div>
        {data.health && (
          <span
            className={cn(
              'tw-h-2 tw-w-2 tw-rounded-full tw-flex-shrink-0',
              data.health.toLowerCase().includes('heal') || data.health.toLowerCase() === 'up'
                ? 'tw-bg-healthy'
                : data.health.toLowerCase().includes('unheal') || data.health.toLowerCase() === 'down'
                  ? 'tw-bg-failed'
                  : 'tw-bg-unknown',
            )}
            title={`Health: ${data.health}`}
          />
        )}
      </div>
      <Handle type="source" position={Position.Bottom} className="tw-opacity-0" />
    </div>
  );
}

export const CrowKVNode = memo(CrowKVNodeBase);
