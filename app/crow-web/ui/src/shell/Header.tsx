// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { RefreshCw, Layers, Network, FileJson, Database, RotateCcw, HardDrive } from 'lucide-react';
import { useViewMode } from '../contexts/ViewModeContext';
import { ViewMode } from '../types';
import { cn } from '../utils/cn';

export type ClusterHealth = 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';

export type CenterPanelMode = 'topology' | 'swagger' | 'kv';

interface HeaderProps {
  clusterHealth: ClusterHealth;
  onRefresh: () => void;
  refreshing?: boolean;
  apiTargetNodeId: string;
  swaggerActive?: boolean;
  onToggleSwagger?: () => void;
  showSwagger?: boolean;
  showKV?: boolean;
  kvActive?: boolean;
  onToggleKV?: () => void;
  centerPanel?: CenterPanelMode;
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
  apiTargetNodeId,
  swaggerActive,
  onToggleSwagger,
  showSwagger = true,
  showKV = false,
  kvActive = false,
  onToggleKV,
  centerPanel = 'topology',
  onShowTopology,
  onResetCluster,
}: HeaderProps) {
  const { viewMode, setViewMode } = useViewMode();

  return (
    <header className="tw-fixed tw-top-0 tw-left-0 tw-right-0 tw-z-40 tw-h-14 tw-bg-panel tw-border-b tw-border-border tw-flex tw-items-center tw-gap-4 tw-px-4">
      {/* Brand */}
      <div className="tw-flex tw-items-center tw-gap-2 tw-font-semibold tw-text-text">
        <span className="tw-text-accent">◆</span> Crow Storage Console
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

      {/* View-mode toggle */}
      <div className="tw-flex tw-items-center tw-rounded-md tw-border tw-border-border tw-overflow-hidden">
        <button
          onClick={() => { setViewMode(ViewMode.Physical); onShowTopology?.(); }}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors',
            viewMode === ViewMode.Physical && centerPanel === 'topology' ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg',
          )}
          aria-pressed={viewMode === ViewMode.Physical && centerPanel === 'topology'}
        >
          <Network className="tw-h-3.5 tw-w-3.5" /> Physical
        </button>
        <button
          onClick={() => { setViewMode(ViewMode.Logical); onShowTopology?.(); }}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors',
            viewMode === ViewMode.Logical && centerPanel === 'topology' ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg',
          )}
          aria-pressed={viewMode === ViewMode.Logical && centerPanel === 'topology'}
        >
          <Layers className="tw-h-3.5 tw-w-3.5" /> KV Cluster
        </button>
        <button
          onClick={() => { setViewMode(ViewMode.Capacity); onShowTopology?.(); }}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-3 tw-py-1.5 tw-text-xs tw-transition-colors',
            viewMode === ViewMode.Capacity ? 'tw-bg-accent/15 tw-text-accent' : 'tw-text-muted hover:tw-bg-bg',
          )}
          aria-pressed={viewMode === ViewMode.Capacity}
        >
          <HardDrive className="tw-h-3.5 tw-w-3.5" /> Capacity
        </button>
      </div>

      {/* KV operator toggle */}
      {showKV && onToggleKV && (
        <button
          onClick={onToggleKV}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-text-xs tw-border tw-transition-colors',
            kvActive
              ? 'tw-bg-accent/15 tw-text-accent tw-border-accent/30'
              : 'tw-text-muted tw-border-border hover:tw-bg-bg',
          )}
          aria-pressed={kvActive}
          title="KV operator panel"
        >
          <Database className="tw-h-3.5 tw-w-3.5" /> KV
        </button>
      )}

      <div className="tw-flex-1" />

      {/* Swagger / API docs toggle */}
      {showSwagger && onToggleSwagger && (
        <button
          onClick={onToggleSwagger}
          disabled={!apiTargetNodeId}
          className={cn(
            'tw-flex tw-items-center tw-gap-1.5 tw-px-2.5 tw-py-1.5 tw-rounded-md tw-text-xs tw-border tw-transition-colors',
            swaggerActive
              ? 'tw-bg-accent/15 tw-text-accent tw-border-accent/30'
              : 'tw-text-muted tw-border-border hover:tw-bg-bg',
            !apiTargetNodeId && 'tw-opacity-50 tw-cursor-not-allowed hover:tw-bg-transparent',
          )}
          aria-pressed={swaggerActive}
          title={apiTargetNodeId ? `Show API for ${apiTargetNodeId}` : 'No node available for API'}
        >
          <FileJson className="tw-h-3.5 tw-w-3.5" /> API
        </button>
      )}

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
