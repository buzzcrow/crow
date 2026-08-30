// Copyright 2026-present Gian <crow.db@outlook.com>

import { RefreshCw, Network, Database, RotateCcw, HardDrive } from 'lucide-react';
import { useDomain } from '../contexts/DomainContext';
import { Domain } from '../types';
import { cn } from '../utils/cn';

export type ClusterHealth = 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';

export type CenterPanelMode = 'topology' | 'kv' | 'capacity' | 'chunk';

interface HeaderProps {
  clusterHealth: ClusterHealth;
  onRefresh: () => void;
  refreshing?: boolean;
  onShowTopology?: () => void;
  onResetCluster?: () => void;
}

const healthPill: Record<ClusterHealth, string> = {
  Healthy: 'tw-bg-healthy/15 tw-text-healthy tw-border-healthy/30',
  Degraded: 'tw-bg-degraded/15 tw-text-degraded tw-border-degraded/30',
  Failed: 'tw-bg-failed/15 tw-text-failed tw-border-failed/30',
  Unknown: 'tw-bg-unknown/15 tw-text-muted tw-border-unknown/30',
};

const healthGlyph: Record<ClusterHealth, string> = {
  Healthy: '✓',
  Degraded: '!',
  Failed: '✕',
  Unknown: '?',
};

export function Header({
  clusterHealth,
  onRefresh,
  refreshing,
  onShowTopology,
  onResetCluster,
}: HeaderProps) {
  const { domain, setDomain } = useDomain();

  return (
    <header className="tw-fixed tw-top-0 tw-left-0 tw-right-0 tw-z-40 tw-h-14 tw-bg-panel tw-border-b tw-border-border tw-flex tw-items-center tw-gap-4 tw-px-4">
      {/* Brand */}
      <div className="tw-flex tw-items-center tw-gap-2 tw-font-semibold tw-text-text">
        <span className="tw-text-accent">◆</span> CrowDB Storage Console
      </div>

      {/* Health pill */}
      <span
        className={cn(
          'tw-inline-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1 tw-rounded-full tw-text-xs tw-font-medium tw-border',
          healthPill[clusterHealth],
        )}
        title={`Cluster health: ${clusterHealth}`}
      >
        <span aria-hidden>{healthGlyph[clusterHealth]}</span>
        {clusterHealth}
      </span>

      {/* Domain toggle */}
      <div className="tw-flex tw-items-center tw-rounded-md tw-border tw-border-border tw-overflow-hidden">
        <button
          data-testid="domain-cluster"
          onClick={() => { setDomain(Domain.Cluster); onShowTopology?.(); }}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors',
            domain === Domain.Cluster ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg',
          )}
          aria-pressed={domain === Domain.Cluster}
        >
          <Network className="tw-h-3.5 tw-w-3.5" /> Cluster
        </button>
        <button
          data-testid="domain-kv"
          onClick={() => { setDomain(Domain.KV); onShowTopology?.(); }}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors',
            domain === Domain.KV ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg',
          )}
          aria-pressed={domain === Domain.KV}
        >
          <Database className="tw-h-3.5 tw-w-3.5" /> KV
        </button>
        <button
          data-testid="domain-chunk"
          onClick={() => { setDomain(Domain.Chunk); onShowTopology?.(); }}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors',
            domain === Domain.Chunk ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg',
          )}
          aria-pressed={domain === Domain.Chunk}
        >
          <HardDrive className="tw-h-3.5 tw-w-3.5" /> Chunk
        </button>
      </div>

      <div className="tw-flex-1" />

      {onResetCluster && (
        <button
          onClick={onResetCluster}
          className="tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-text-xs tw-border tw-border-failed/30 tw-text-failed hover:tw-bg-failed/10 tw-transition-colors"
          title="Reset entire cluster: tear down all stores, groups, servers, nodes, and racks"
        >
          <RotateCcw className="tw-h-3.5 tw-w-3.5" /> Reset
        </button>
      )}

      <button
        onClick={onRefresh}
        className="tw-p-2 tw-rounded-md tw-text-muted hover:tw-text-text hover:tw-bg-bg tw-transition-colors"
        aria-label="Refresh"
        title="Refresh now"
      >
        <RefreshCw className={cn('tw-h-4 tw-w-4', refreshing && 'tw-animate-spin')} />
      </button>
    </header>
  );
}
