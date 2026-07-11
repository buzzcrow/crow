import React, { useState, useMemo, useEffect } from 'react';
import {
  LayoutDashboard,
  Server,
  RefreshCw,
  Search,
  Menu,
  Moon,
  Sun,
  Monitor,
  Download,
  FileText,
  Activity,
} from 'lucide-react';
import { useViewMode } from '../contexts/ViewModeContext';
import { useTheme } from '../contexts/ThemeContext';
import { useSelection } from '../contexts/SelectionContext';
import { Button } from '../components/ui/Button';
import { HealthBadge } from '../components/ui/Badge';
import { ThemeMode, ViewMode } from '../types';
import { generateHealthReport } from '../utils/exportUtils';
import { cn } from '../utils/cn';

interface HeaderProps {
  /** Custom brand logo to display instead of default */
  brandLogo?: React.ReactNode;
  /** Cluster health status */
  clusterHealth: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
  /** Timestamp of last data refresh */
  lastRefreshTime: Date;
  /** Callback to manually trigger data refresh */
  onRefresh: () => void;
  /** List of available nodes for the Swagger selector */
  nodes: Array<{ id: string; name?: string; host: string }>;
  /** Currently selected node for Swagger */
  selectedNodeId?: string;
  /** Callback when selected node changes */
  onNodeSelect: (nodeId: string) => void;
  /** Callback to open the command palette */
  onOpenCommandPalette: () => void;
  /** Custom menu items to add to the overflow menu */
  customMenuItems?: Array<{ label: string; icon?: React.ReactNode; onClick: () => void }>;
}

// Type for health history entry
type HealthHistoryEntry = {
  timestamp: number;
  status: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown';
};

export function Header({
  brandLogo,
  clusterHealth,
  lastRefreshTime,
  onRefresh,
  nodes,
  selectedNodeId,
  onNodeSelect,
  onOpenCommandPalette,
  customMenuItems,
}: HeaderProps) {
  const { viewMode, toggleViewMode } = useViewMode();
  const { themeMode, setThemeMode } = useTheme();
  const { selectedEntity } = useSelection();
  const [isOverflowMenuOpen, setIsOverflowMenuOpen] = useState(false);
  const [isHealthTimelineOpen, setIsHealthTimelineOpen] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [healthHistory, setHealthHistory] = useState<HealthHistoryEntry[]>([]);

  // Generate initial health history for the last hour
  useEffect(() => {
    const now = Date.now();
    const history: HealthHistoryEntry[] = [];

    // Generate a data point every 5 minutes for the last hour
    for (let i = 12; i >= 0; i--) {
      const timestamp = now - i * 5 * 60 * 1000;
      // Generate mostly healthy status with occasional degradations
      const random = Math.random();
      let status: 'Healthy' | 'Degraded' | 'Failed' | 'Unknown' = 'Healthy';
      if (random < 0.1) status = 'Failed';
      else if (random < 0.3) status = 'Degraded';

      history.push({ timestamp, status });
    }

    setHealthHistory(history);
  }, []);

  // Update health history when clusterHealth changes
  useEffect(() => {
    if (clusterHealth) {
      setHealthHistory(prev => {
        const newEntry: HealthHistoryEntry = {
          timestamp: Date.now(),
          status: clusterHealth as 'Healthy' | 'Degraded' | 'Failed' | 'Unknown',
        };

        // Keep only the last 13 entries (1 hour of data at 5 minute intervals)
        return [...prev.slice(1), newEntry];
      });
    }
  }, [clusterHealth, lastRefreshTime]);

  // Calculate health status color for timeline bars
  const getHealthColor = (status: string) => {
    switch (status) {
      case 'Healthy': return 'tw-bg-green-500';
      case 'Degraded': return 'tw-bg-yellow-500';
      case 'Failed': return 'tw-bg-red-500';
      default: return 'tw-bg-gray-500';
    }
  };

  // Build breadcrumb trail from selected entity
  const breadcrumbs = useMemo(() => {
    if (!selectedEntity) {
      return [{ label: viewMode === ViewMode.Physical ? 'Infrastructure' : 'Cluster', current: true }];
    }

    const crumbs: Array<{ label: string; current?: boolean }> = [];

    // Add root
    crumbs.push({ label: viewMode === ViewMode.Physical ? 'Infrastructure' : 'Cluster' });

    // Add parent entities based on type
    if (viewMode === ViewMode.Physical) {
      switch (selectedEntity.type) {
        case 'Rack':
          crumbs.push({ label: selectedEntity.name || selectedEntity.id, current: true });
          break;
        case 'Node':
          if (selectedEntity.parentIds?.rackId) {
            crumbs.push({ label: selectedEntity.parentIds.rackId });
          }
          crumbs.push({ label: selectedEntity.name || selectedEntity.id, current: true });
          break;
        case 'Server':
          if (selectedEntity.parentIds?.rackId) {
            crumbs.push({ label: selectedEntity.parentIds.rackId });
          }
          if (selectedEntity.parentIds?.nodeId) {
            crumbs.push({ label: selectedEntity.parentIds.nodeId });
          }
          crumbs.push({ label: 'Server', current: true });
          break;
        case 'Store':
          if (selectedEntity.parentIds?.rackId) {
            crumbs.push({ label: selectedEntity.parentIds.rackId });
          }
          if (selectedEntity.parentIds?.nodeId) {
            crumbs.push({ label: selectedEntity.parentIds.nodeId });
          }
          crumbs.push({ label: selectedEntity.name || selectedEntity.id, current: true });
          break;
        case 'Group':
          if (selectedEntity.parentIds?.rackId) {
            crumbs.push({ label: selectedEntity.parentIds.rackId });
          }
          if (selectedEntity.parentIds?.nodeId) {
            crumbs.push({ label: selectedEntity.parentIds.nodeId });
          }
          if (selectedEntity.parentIds?.storeId) {
            crumbs.push({ label: selectedEntity.parentIds.storeId });
          }
          crumbs.push({ label: selectedEntity.name || selectedEntity.id, current: true });
          break;
        case 'Replica':
          if (selectedEntity.parentIds?.rackId) {
            crumbs.push({ label: selectedEntity.parentIds.rackId });
          }
          if (selectedEntity.parentIds?.nodeId) {
            crumbs.push({ label: selectedEntity.parentIds.nodeId });
          }
          if (selectedEntity.parentIds?.storeId) {
            crumbs.push({ label: selectedEntity.parentIds.storeId });
          }
          if (selectedEntity.parentIds?.groupId) {
            crumbs.push({ label: selectedEntity.parentIds.groupId });
          }
          crumbs.push({ label: 'Replica', current: true });
          break;
      }
    } else {
      // Logical view
      switch (selectedEntity.type) {
        case 'Store':
          crumbs.push({ label: selectedEntity.name || selectedEntity.id, current: true });
          break;
        case 'Group':
          if (selectedEntity.parentIds?.storeId || selectedEntity.parentIds?.store_id) {
            crumbs.push({ label: selectedEntity.parentIds.storeId || selectedEntity.parentIds.store_id });
          }
          crumbs.push({ label: selectedEntity.name || selectedEntity.id, current: true });
          break;
        case 'Replica':
          if (selectedEntity.parentIds?.storeId || selectedEntity.parentIds?.store_id) {
            crumbs.push({ label: selectedEntity.parentIds.storeId || selectedEntity.parentIds.store_id });
          }
          if (selectedEntity.parentIds?.groupId || selectedEntity.parentIds?.group_id) {
            crumbs.push({ label: selectedEntity.parentIds.groupId || selectedEntity.parentIds.group_id });
          }
          crumbs.push({ label: 'Replica', current: true });
          break;
      }
    }

    return crumbs;
  }, [selectedEntity, viewMode]);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      await onRefresh();
    } finally {
      setIsRefreshing(false);
    }
  };

  const handleExportHealthReport = async () => {
    setIsOverflowMenuOpen(false);
    try {
      await generateHealthReport('CrowKV Cluster', clusterHealth, [], []);
    } catch (err) {
      console.error('Failed to generate health report:', err);
    }
  };

  return (
    <header className="tw-fixed tw-top-0 tw-left-0 tw-right-0 tw-h-14 tw-bg-panel tw-border-b tw-border-border tw-z-40 tw-px-4 tw-flex tw-items-center tw-justify-between tw-gap-4">
      {/* Left section: Brand + View toggle + Health */}
      <div className="tw-flex tw-items-center tw-gap-4 tw-flex-shrink-0">
        {/* Brand logo */}
        <div className="tw-flex tw-items-center tw-gap-2">
          {brandLogo || (
            <>
              <Server className="tw-h-6 tw-w-6 tw-text-accent" />
              <span className="tw-font-bold tw-text-text tw-hidden sm:tw-inline-block">CrowKV</span>
            </>
          )}
        </div>

        {/* View mode toggle */}
        <div className="tw-flex tw-items-center tw-bg-bg tw-rounded-md tw-p-1 tw-border tw-border-border">
          <button
            onClick={toggleViewMode}
            className={cn(
              'tw-px-3 tw-py-1 tw-rounded tw-text-sm tw-font-medium tw-transition-colors',
              viewMode === ViewMode.Physical
                ? 'tw-bg-accent tw-text-bg'
                : 'tw-text-muted tw-hover:text-text'
            )}
          >
            <div className="tw-flex tw-items-center tw-gap-2">
              <Server className="tw-h-4 tw-w-4" />
              <span className="tw-hidden sm:tw-inline-block">Infrastructure</span>
            </div>
          </button>
          <button
            onClick={toggleViewMode}
            className={cn(
              'tw-px-3 tw-py-1 tw-rounded tw-text-sm tw-font-medium tw-transition-colors',
              viewMode === ViewMode.Logical
                ? 'tw-bg-accent tw-text-bg'
                : 'tw-text-muted tw-hover:text-text'
            )}
          >
            <div className="tw-flex tw-items-center tw-gap-2">
              <LayoutDashboard className="tw-h-4 tw-w-4" />
              <span className="tw-hidden sm:tw-inline-block">Cluster</span>
            </div>
          </button>
        </div>

        {/* Cluster health pill */}
        <div className="tw-relative">
          <button
            onClick={() => setIsHealthTimelineOpen(!isHealthTimelineOpen)}
            className="tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-rounded-md tw-bg-bg tw-border tw-border-border tw-transition-colors hover:tw-bg-bg/80"
          >
            <HealthBadge status={clusterHealth} size="sm" />
          </button>

          {/* Health timeline dropdown */}
          {isHealthTimelineOpen && (
            <div className="tw-absolute tw-top-full tw-left-0 tw-mt-2 tw-w-80 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-p-4 tw-z-50">
              <h4 className="tw-text-sm tw-font-semibold tw-text-text tw-mb-3">Cluster Health History</h4>
              <div className="tw-space-y-3">
                {/* Timeline bars */}
                <div className="tw-h-20 tw-bg-bg tw-rounded-md tw-p-3 tw-flex tw-items-end tw-justify-between tw-gap-0.5">
                  {healthHistory.map((entry, _index) => {
                    // Calculate height based on status (Healthy = 100%, Degraded = 60%, Failed = 20%)
                    let height = '100%';
                    if (entry.status === 'Degraded') height = '60%';
                    else if (entry.status === 'Failed') height = '20%';

                    return (
                      <div key={entry.timestamp} className="tw-flex-1 tw-flex tw-flex-col tw-items-center tw-justify-end">
                        <div
                          className={cn(
                            'tw-w-full tw-rounded-t-sm tw-transition-all',
                            getHealthColor(entry.status)
                          )}
                          style={{ height }}
                          title={`${new Date(entry.timestamp).toLocaleTimeString()}: ${entry.status}`}
                        />
                      </div>
                    );
                  })}
                </div>

                {/* Timeline legend */}
                <div className="tw-flex tw-justify-between text-xs">
                  <div className="tw-flex tw-items-center tw-gap-2">
                    <div className="tw-flex tw-items-center tw-gap-1">
                      <div className="tw-w-2 tw-h-2 tw-rounded-full tw-bg-green-500" />
                      <span className="tw-text-muted">Healthy</span>
                    </div>
                    <div className="tw-flex tw-items-center tw-gap-1">
                      <div className="tw-w-2 tw-h-2 tw-rounded-full tw-bg-yellow-500" />
                      <span className="tw-text-muted">Degraded</span>
                    </div>
                    <div className="tw-flex tw-items-center tw-gap-1">
                      <div className="tw-w-2 tw-h-2 tw-rounded-full tw-bg-red-500" />
                      <span className="tw-text-muted">Failed</span>
                    </div>
                  </div>
                </div>

                {/* Timeline footer */}
                <div className="tw-flex tw-justify-between tw-items-center tw-text-xs tw-text-muted">
                  <span>Last 1 hour</span>
                  <button className="tw-text-accent hover:tw-underline tw-flex tw-items-center tw-gap-1">
                    <Activity className="tw-h-3 tw-w-3" />
                    View all metrics
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Middle section: Breadcrumbs */}
      <nav className="tw-flex-1 tw-flex tw-items-center tw-px-4 tw-hidden md:tw-flex">
        <ol className="tw-flex tw-items-center tw-gap-2 tw-text-sm">
          {breadcrumbs.map((crumb, index) => (
            <li key={index} className="tw-flex tw-items-center tw-gap-2">
              {index > 0 && <span className="tw-text-muted tw-px-1">/</span>}
              <span
                className={cn(
                  'tw-text-sm',
                  crumb.current ? 'tw-text-text tw-font-medium' : 'tw-text-muted hover:tw-text-text'
                )}
              >
                {crumb.label}
              </span>
            </li>
          ))}
        </ol>
      </nav>

      {/* Right section: Actions */}
      <div className="tw-flex tw-items-center tw-gap-2 tw-flex-shrink-0">
        {/* Last refresh time */}
        <span className="tw-text-xs tw-text-muted tw-hidden lg:tw-inline-block">
          Last updated: {lastRefreshTime.toLocaleTimeString()}
        </span>

        {/* Refresh button */}
        <Button
          variant="ghost"
          size="icon"
          onClick={handleRefresh}
          isLoading={isRefreshing}
          aria-label="Refresh data"
        >
          <RefreshCw className="tw-h-4 tw-w-4" />
        </Button>

        {/* Node selector for Swagger */}
        <select
          value={selectedNodeId || ''}
          onChange={e => onNodeSelect(e.target.value)}
          className="tw-bg-bg tw-border tw-border-border tw-rounded-md tw-px-3 tw-py-1.5 tw-text-sm tw-text-text tw-hidden lg:tw-block"
          aria-label="Select node for Swagger UI"
        >
          <option value="">Select node...</option>
          {nodes.map(node => (
            <option key={node.id} value={node.id}>
              {node.name || node.id} ({node.host})
            </option>
          ))}
        </select>

        {/* Command palette trigger */}
        <Button
          variant="ghost"
          size="icon"
          onClick={onOpenCommandPalette}
          aria-label="Open command palette"
          className="tw-hidden sm:tw-flex"
        >
          <Search className="tw-h-4 tw-w-4" />
        </Button>

        {/* Overflow menu */}
        <div className="tw-relative">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setIsOverflowMenuOpen(!isOverflowMenuOpen)}
            aria-label="Open menu"
          >
            <Menu className="tw-h-4 tw-w-4" />
          </Button>

          {isOverflowMenuOpen && (
            <div className="tw-absolute tw-top-full tw-right-0 tw-mt-2 tw-w-56 tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-py-1 tw-z-50">
              {/* Theme selector */}
              <div className="tw-px-2 tw-py-1">
                <p className="tw-text-xs tw-text-muted tw-uppercase tw-font-medium tw-mb-1 tw-px-2">Theme</p>
                <button
                  onClick={() => {
                    setThemeMode(ThemeMode.Light);
                    setIsOverflowMenuOpen(false);
                  }}
                  className={cn(
                    'tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-2 tw-py-1.5 tw-rounded tw-text-left tw-text-sm tw-transition-colors hover:tw-bg-bg',
                    themeMode === ThemeMode.Light && 'tw-bg-bg tw-text-accent'
                  )}
                >
                  <Sun className="tw-h-4 tw-w-4" />
                  Light
                </button>
                <button
                  onClick={() => {
                    setThemeMode(ThemeMode.Dark);
                    setIsOverflowMenuOpen(false);
                  }}
                  className={cn(
                    'tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-2 tw-py-1.5 tw-rounded tw-text-left tw-text-sm tw-transition-colors hover:tw-bg-bg',
                    themeMode === ThemeMode.Dark && 'tw-bg-bg tw-text-accent'
                  )}
                >
                  <Moon className="tw-h-4 tw-w-4" />
                  Dark
                </button>
                <button
                  onClick={() => {
                    setThemeMode(ThemeMode.System);
                    setIsOverflowMenuOpen(false);
                  }}
                  className={cn(
                    'tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-2 tw-py-1.5 tw-rounded tw-text-left tw-text-sm tw-transition-colors hover:tw-bg-bg',
                    themeMode === ThemeMode.System && 'tw-bg-bg tw-text-accent'
                  )}
                >
                  <Monitor className="tw-h-4 tw-w-4" />
                  System
                </button>
              </div>

              <div className="tw-border-t tw-border-border tw-my-1" />

              {/* Export options */}
              <div className="tw-px-2 tw-py-1">
                <p className="tw-text-xs tw-text-muted tw-uppercase tw-font-medium tw-mb-1 tw-px-2">Export</p>
                <button
                  onClick={handleExportHealthReport}
                  className="tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-2 tw-py-1.5 tw-rounded tw-text-left tw-text-sm tw-transition-colors hover:tw-bg-bg"
                >
                  <FileText className="tw-h-4 tw-w-4" />
                  Health Report (PDF)
                </button>
                <button
                  onClick={() => {
                    /* Export topology will be implemented later */
                    setIsOverflowMenuOpen(false);
                  }}
                  className="tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-2 tw-py-1.5 tw-rounded tw-text-left tw-text-sm tw-transition-colors hover:tw-bg-bg"
                >
                  <Download className="tw-h-4 tw-w-4" />
                  Topology (PNG/SVG)
                </button>
              </div>

              {/* Custom menu items */}
              {customMenuItems && customMenuItems.length > 0 && (
                <>
                  <div className="tw-border-t tw-border-border tw-my-1" />
                  <div className="tw-px-2 tw-py-1">
                    {customMenuItems.map((item, index) => (
                      <button
                        key={index}
                        onClick={() => {
                          item.onClick();
                          setIsOverflowMenuOpen(false);
                        }}
                        className="tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-2 tw-py-1.5 tw-rounded tw-text-left tw-text-sm tw-transition-colors hover:tw-bg-bg"
                      >
                        {item.icon}
                        {item.label}
                      </button>
                    ))}
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
