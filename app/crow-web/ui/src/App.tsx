// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { Suspense, useState, useCallback, useMemo, lazy, useEffect } from 'react';
import { Server, Database, Plus, Trash2, Activity, RotateCw, Square, HardDrive, Boxes, Move } from 'lucide-react';
import type { CenterPanelMode } from './shell/Header';
import { ViewModeProvider, useViewMode } from './contexts/ViewModeContext';
import { SelectionProvider, useSelection } from './contexts/SelectionContext';
import { ToastProvider, useToast } from './contexts/ToastContext';
import { ActivityProvider, useActivity } from './contexts/ActivityContext';
import { usePhysicalTree } from './data/usePhysicalTree';
import { useLogicalTree } from './data/useLogicalTree';
import { useCapacityTree } from './data/useCapacityTree';
import { Header, ClusterHealth } from './shell/Header';
import { Sidebar } from './shell/Sidebar';
import { ToastContainer } from './components/ToastContainer';
import { TreeNode } from './components/Tree';
import { ContextMenu, useContextMenu, MenuItemOrSeparator } from './components/ContextMenu';
import type { MenuTarget } from './topology/TopologyCanvas';
import {
  AddRackDialog,
  AddNodeDialog,
  AddStoreDialog,
  AddGroupDialog,
  AddReplicaDialog,
  AddDiskGroupDialog,
  AddDiskDialog,
  MoveDiskDialog,
  DeployServerDialog,
  DeployDiskdbDialog,
  ConfirmDeleteDialog,
  InitClusterDialog,
  ZoneSelectDialog,
} from './components/dialogs';
import { ViewMode } from './types';
import {
  removeRack,
  removeNode,
  removeStore,
  removeGroup,
  removeReplica,
  stopServer,
  restartServer,
  pingNode,
  setApiBase,
  resetCluster,
  triggerDiskdbScan,
  recalcDiskdbUsage,
  compactDiskdbZones,
  rebuildDiskdbZoneBitmap,
  setDiskStatus,
  setDiskGroupStatus,
  restartDiskdb,
  stopDiskdb,
  removeServer,
  removeDiskdb,
  removeDiskGroup,
  removeDisk,
  listServers,
} from './api';
import { deployPortDefaultsForNode, diskdbPortDefaultsForNode, nextIdFromSuffix, nextNumericId } from './components/dialogs/defaults';
import { buildCrowKVServers, crowKvServerNodeIds } from './data/crowKvServers';
import { isCrowKVServerAvailable } from './data/crowKvServers';
import { toUiHealth } from './utils/entityDisplay';

const TopologyCanvas = lazy(() =>
  import('./topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })),
);
const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));
const SwaggerPanel = lazy(() => import('./panels/SwaggerPanel').then((m) => ({ default: m.SwaggerPanel })));
const KvOperatorPanel = lazy(() => import('./panels/KvOperatorPanel').then((m) => ({ default: m.KvOperatorPanel })));
const CapacityPanel = lazy(() => import('./panels/CapacityPanel').then((m) => ({ default: m.CapacityPanel })));

export interface CrowConsoleProps {
  /** API prefix for all backend calls (default "/api"). */
  apiPrefix?: string;
  /** Mount hint for host routers (default "/"). Not used for navigation in v1. */
  basePath?: string;
  /** Hide all mutating controls. */
  readonly?: boolean;
  /** Opt feature areas in/out. */
  modules?: Partial<Record<'racks' | 'nodes' | 'stores' | 'groups' | 'replicas' | 'kv' | 'swagger' | 'activity', boolean>>;
  /** Initial view mode (default Physical). */
  initialViewMode?: ViewMode;
  /** Pre-select a node for the Swagger panel. */
  initialNodeId?: string;
  /** Structured event callback for host integration. */
  onEvent?: (event: { type: string; payload?: unknown }) => void;
}

function AppContent({ apiPrefix = '/api', readonly = false, modules, initialNodeId = '', onEvent }: CrowConsoleProps) {
  const { viewMode } = useViewMode();
  const { selectedEntity } = useSelection();
  const { success, error } = useToast();
  const { log } = useActivity();

  // Re-root data-plane traffic onto the host-provided apiPrefix. The
  // standalone mount also sets this pre-render in `main.tsx`; this keeps
  // an embedding host's prop authoritative.
  useEffect(() => {
    setApiBase(apiPrefix);
  }, [apiPrefix]);

  const [lastUsedRackId, setLastUsedRackId] = useState<number>(0);
  const [rememberedDeployPorts, setRememberedDeployPorts] = useState<{ mgmt: number[]; grpc: number[]; diskdbRpc: number[] }>({ mgmt: [], grpc: [], diskdbRpc: [] });
  const [lastRefreshTime, setLastRefreshTime] = useState<Date>(new Date());
  const [refreshing, setRefreshing] = useState(false);
  const [centerPanel, setCenterPanel] = useState<CenterPanelMode>('topology');
  const [sidebarWidth, setSidebarWidth] = useState(280);
  const [inspectorWidth, setInspectorWidth] = useState(320);
  const [resizing, setResizing] = useState<'left' | 'right' | null>(null);
  const [canvasFocusRequest, setCanvasFocusRequest] = useState<{ targetId: string; subtree: boolean; nonce: number } | null>(null);

  const [dialog, setDialog] = useState<{
    addRack?: boolean;
    addNode?: { rackId: number };
    addStore?: boolean;
    addGroup?: { storeId: string };
    addReplica?: { storeId: string; groupId: string };
    addDiskGroup?: { nodeId: number };
    addDisk?: { nodeId: number; dgId: number };
    moveDisk?: { diskId: string; rackId: number; nodeId: number; dgId: number };
    deployServer?: { nodeId: number };
    deployDiskdb?: { nodeId: number } | null;
    delete?: { type: string; id: string | number; onDelete: () => Promise<void>; cascadeWarning?: string };
    initCluster?: boolean;
    compactZones?: { diskId: string; zoneCount?: number };
    rebuildBitmap?: { diskId: string; zoneCount?: number };
  }>({});

  const { menuState, openMenu, closeMenu } = useContextMenu();

  const physicalActive = viewMode === ViewMode.Physical;
  const capacityActive = viewMode === ViewMode.Capacity;
  const { racks, nodes, nodeStores, nodeHealthById, loading: physLoading, error: physError, refresh: refreshPhysical } = usePhysicalTree({
    enabled: true,
    recursive: 2,
    pollIntervalActive: 1000,
    pollIntervalInactive: 30000,
  });
  const { stores, groups, loading: logLoading, error: logError, refresh: refreshLogical } = useLogicalTree({
    enabled: true,
    recursive: 2,
    pollIntervalActive: 1000,
    pollIntervalInactive: 30000,
  });
  const { instances: diskdbInstances, usage: capacityUsage, scanStatus: capacityScanStatus, loading: capLoading, error: capError, refresh: refreshCapacity, nodeDiskGroups, fetchNodeDiskGroups } = useCapacityTree({
    enabled: viewMode === ViewMode.Capacity,
    pollIntervalActive: 5000,
    pollIntervalInactive: 30000,
  });

  const loading = physLoading || logLoading || capLoading;
  const dataError = physError || logError || capError;
  const servers = useMemo(() => buildCrowKVServers(nodes, racks), [nodes, racks]);
  const serverNodeIds = useMemo(() => crowKvServerNodeIds(servers), [servers]);
  const [allServers, setAllServers] = useState<import('./api').ServerSummary[]>([]);
  const refreshAllServers = useCallback(async () => {
    try {
      setAllServers(await listServers());
    } catch (err) {
      setAllServers([]);
      error(`Failed to load server list: ${err instanceof Error ? err.message : 'backend unreachable'}`);
    }
  }, [error]);
  useEffect(() => {
    if (capacityActive) {
      refreshAllServers();
    }
  }, [capacityActive, diskdbInstances, refreshAllServers]);
  const diskdbNodeIds = useMemo(
    () => new Set(allServers.filter((s) => s.service_type === 'diskdb' && s.node_id != null).map((s) => s.node_id!)),
    [allServers],
  );
  // Cluster is initialized once the system store (store 0) exists.
  const clusterInitialized = useMemo(
    () => stores.some((s) => String(s.store_id) === '0'),
    [stores],
  );

  const clusterHealth: ClusterHealth = useMemo(() => {
    if (dataError) return 'Failed';
    if (groups.length === 0) return 'Unknown';
    const statuses = groups.map((g) => toUiHealth(String((g as any).state || (g as any).health || '')));
    if (statuses.some((status) => status === 'Failed')) return 'Failed';
    if (statuses.some((status) => status === 'Degraded')) return 'Degraded';
    if (statuses.every((status) => status === 'Healthy')) return 'Healthy';
    return 'Unknown';
  }, [groups, dataError]);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      await Promise.all([refreshPhysical(), refreshLogical(), refreshCapacity()]);
      if (capacityActive) {
        await Promise.all([fetchNodeDiskGroups(nodes.map((n) => n.id)), refreshAllServers()]);
      }
      setLastRefreshTime(new Date());
    } finally {
      setRefreshing(false);
    }
  }, [refreshPhysical, refreshLogical, refreshCapacity, capacityActive, fetchNodeDiskGroups, nodes, refreshAllServers]);

  // Fetch node disk-groups when the Capacity view is active.
  useEffect(() => {
    if (capacityActive && nodes.length > 0) {
      fetchNodeDiskGroups(nodes.map((n) => n.id));
    }
  }, [capacityActive, nodes, fetchNodeDiskGroups]);

  // After cluster init succeeds, refresh the tree so the system group
  // appears. Init only bootstraps store 0 / group 0; store creation is
  // a separate step the user initiates via the "+" button.
  const handleInitSuccess = useCallback(async () => {
    await handleRefresh();
    setDialog((d) => ({ ...d, initCluster: false }));
  }, [handleRefresh]);

  useEffect(() => {
    if (!resizing) return;

    const onMouseMove = (event: MouseEvent) => {
      if (resizing === 'left') {
        setSidebarWidth(Math.min(420, Math.max(200, event.clientX)));
        return;
      }
      setInspectorWidth(Math.min(560, Math.max(280, window.innerWidth - event.clientX)));
    };

    const onMouseUp = () => setResizing(null);

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, [resizing]);

  /** Run a mutation, surface toast + activity, then refresh. */
  const runMutation = useCallback(
    async (action: string, target: string, fn: () => Promise<unknown>) => {
      try {
        await fn();
        log({ action, target, status: 'Success' });
        success(`${action}: ${target}`);
        onEvent?.({ type: 'mutation', payload: { action, target } });
        await handleRefresh();
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'failed';
        log({ action, target, status: 'Failed', message: msg });
        error(`${action} failed: ${msg}`);
      }
    },
    [log, success, error, onEvent, handleRefresh],
  );

  const requestDelete = useCallback(
    (type: string, id: string | number, onDelete: () => Promise<void>, cascadeWarning?: string) => {
      setDialog((d) => ({ ...d, delete: { type, id, onDelete, cascadeWarning } }));
    },
    [],
  );

  const handleResetCluster = useCallback(() => {
    setDialog((d) => ({
      ...d,
      delete: {
        type: 'Cluster',
        id: 'all',
        onDelete: async () => { await runMutation('Reset Cluster', 'all', () => resetCluster()); },
      },
    }));
  }, [runMutation]);

  /** Build per-layer context menu items for a normalized target. */
  const buildMenuItems = useCallback(
    (t: MenuTarget): MenuItemOrSeparator[] => {
      if (readonly) return [];
      const items: MenuItemOrSeparator[] = [];
      const p = t.parentIds || {};

      if (physicalActive) {
        if (t.type === 'Rack' && modules?.nodes !== false) {
          const rackId = Number(t.rawId);
          items.push({
            id: 'add-node',
            label: 'Add Node',
            icon: <Server className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addNode: { rackId } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-rack',
            label: 'Delete Rack',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Rack', rackId, async () => { await runMutation('Delete Rack', `Rack ${rackId}`, () => removeRack(rackId)); }),
          });
        } else if (t.type === 'Node') {
          const nodeId = Number(t.rawId);
          const hasServer = serverNodeIds.has(nodeId);
          const hasDiskdb = diskdbNodeIds.has(nodeId);
          // Add Services — deploy Crow Storage and/or DiskDB.
          if (!hasServer) {
            items.push({
              id: 'deploy',
              label: 'Deploy Crow Storage',
              icon: <Server className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployServer: { nodeId } })),
            });
          }
          if (!hasDiskdb) {
            items.push({
              id: 'deploy-diskdb',
              label: 'Deploy DiskDB',
              icon: <HardDrive className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: { nodeId } })),
            });
          }
          items.push({
            id: 'ping',
            label: 'Ping',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () =>
              runMutation('Ping Node', t.label || t.id, async () => {
                const r = await pingNode(nodeId);
                if (!r.ok) throw new Error(r.error || 'unreachable');
              }),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-node',
            label: 'Delete Node',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            // Cascade: stop + remove all services before deleting the node.
            onSelect: () => requestDelete('Node', nodeId, async () => {
              await runMutation('Delete Node', t.label || t.id, async () => {
                if (hasDiskdb) await removeDiskdb(nodeId);
                if (hasServer) await removeServer(nodeId);
                await removeNode(nodeId);
              });
            }),
          });
        } else if (t.type === 'Server') {
          // Server context menu: dispatch on service_type (KV vs DiskDB).
          const nodeId = Number(p.node_id);
          if (p.service_type === 'Diskdb') {
            items.push({
              id: 'ddb-restart',
              label: 'Restart DiskDB',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
            });
            items.push({
              id: 'ddb-stop',
              label: 'Stop DiskDB',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-ddb',
              label: 'Delete DiskDB',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('DiskDB', t.label || t.id, async () => {
                await runMutation('Delete DiskDB', t.label || t.id, () => removeDiskdb(nodeId));
              }),
            });
          } else {
            // CrowKV service context menu: restart, stop, delete.
            items.push({
              id: 'restart',
              label: 'Restart Crow Storage',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart Crow Storage', t.label || t.id, () => restartServer(nodeId)),
            });
            items.push({
              id: 'stop',
              label: 'Stop Crow Storage',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop Crow Storage', t.label || t.id, () => stopServer(nodeId)),
            });
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-service',
              label: 'Delete Crow Storage',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('Crow Storage', t.label || t.id, async () => {
                await runMutation('Delete Crow Storage', t.label || t.id, () => removeServer(nodeId));
              }),
            });
          }
        }
      } else if (capacityActive) {
        // Capacity view context menus: rack, node, diskdb instance, disk-group, disk.
        if (t.type === 'Rack' && modules?.nodes !== false) {
          const rackId = Number(t.rawId);
          items.push({
            id: 'add-node',
            label: 'Add Node',
            icon: <Server className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addNode: { rackId } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-rack',
            label: 'Delete Rack',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Rack', rackId, async () => { await runMutation('Delete Rack', `Rack ${rackId}`, () => removeRack(rackId)); }),
          });
        } else if (t.type === 'Node') {
          const nodeId = Number(t.rawId ?? t.id);
          const hasDiskdb = diskdbNodeIds.has(nodeId);
          // Add Disk Group is always available on a node.
          items.push({
            id: 'add-dg',
            label: 'Add Disk Group',
            icon: <Boxes className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addDiskGroup: { nodeId } })),
          });
          items.push({ id: 's1', separator: true });
          if (!hasDiskdb) {
            items.push({
              id: 'ddb-deploy',
              label: 'Deploy DiskDB',
              icon: <HardDrive className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: { nodeId } })),
            });
          } else {
            items.push({
              id: 'ddb-restart',
              label: 'Restart DiskDB',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(nodeId)),
            });
            items.push({
              id: 'ddb-stop',
              label: 'Stop DiskDB',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(nodeId)),
            });
          }
          items.push({ id: 's2', separator: true });
          items.push({
            id: 'del-node',
            label: 'Delete Node',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Node', nodeId, async () => {
              await runMutation('Delete Node', t.label || t.id, async () => {
                if (hasDiskdb) await removeDiskdb(nodeId);
                await removeNode(nodeId);
              });
            }),
          });
        } else if (t.type === 'DiskGroup') {
          // disk-group — logical container: add disk, set status, delete
          const dgId = Number(t.rawId);
          const dgNodeId = Number(p.node_id);
          const dgRackId = Number(p.rack_id);
          items.push({
            id: 'add-disk',
            label: 'Add Disk',
            icon: <HardDrive className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addDisk: { nodeId: dgNodeId, dgId } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'dg-set-up',
            label: 'Set Disk Group Up',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Set Disk Group Up', t.label || t.id, () => setDiskGroupStatus(dgRackId, dgNodeId, dgId, 'Up')),
          });
          items.push({
            id: 'dg-set-down',
            label: 'Set Disk Group Down',
            icon: <Square className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Set Disk Group Down', t.label || t.id, () => setDiskGroupStatus(dgRackId, dgNodeId, dgId, 'Offline')),
          });
          items.push({ id: 's2', separator: true });
          items.push({
            id: 'del-dg',
            label: 'Delete Disk Group',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Disk Group', dgId, async () => {
              await runMutation('Delete Disk Group', t.label || t.id, () => removeDiskGroup(dgNodeId, dgId));
            }, 'All disks in this disk group will also be removed.'),
          });
        } else if (t.type === 'Disk') {
          // disk — all operations: compact, rebuild, consistency scan,
          // recalc usage, set status, delete
          const diskId = String(p.disk_id || t.rawId || t.id);
          const diskNodeId = Number(p.node_id);
          const diskDgId = Number(p.disk_group_id);
          // Look up zone count from capacity usage data.
          let diskZoneCount: number | undefined;
          for (const dg of capacityUsage?.disk_groups || []) {
            const found = (dg.disks || []).find((d) => d.disk_id === diskId);
            if (found) { diskZoneCount = found.zone_count; break; }
          }
          items.push({
            id: 'ddb-compact',
            label: 'Compact Zones',
            icon: <Database className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, compactZones: { diskId, zoneCount: diskZoneCount } })),
          });
          items.push({
            id: 'ddb-rebuild',
            label: 'Rebuild Bitmap',
            icon: <RotateCw className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, rebuildBitmap: { diskId, zoneCount: diskZoneCount } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'ddb-scan',
            label: 'Trigger Consistency Scan',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Trigger Consistency Scan', t.label || t.id, () => triggerDiskdbScan(diskDgId)),
          });
          items.push({
            id: 'ddb-recalc',
            label: 'Recalc Usage',
            icon: <RotateCw className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Recalc Usage', t.label || t.id, () => recalcDiskdbUsage(diskDgId)),
          });
          items.push({ id: 's2', separator: true });
          items.push({
            id: 'disk-set-down',
            label: 'Set Disk Down',
            icon: <Square className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Set Disk Down', t.label || t.id, () => setDiskStatus(diskId, 'Offline')),
          });
          items.push({
            id: 'disk-set-up',
            label: 'Set Disk Up',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Set Disk Up', t.label || t.id, () => setDiskStatus(diskId, 'Up')),
          });
          items.push({ id: 's3', separator: true });
          items.push({
            id: 'move-disk',
            label: 'Move Disk',
            icon: <Move className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, moveDisk: { diskId, rackId: Number(p.rack_id), nodeId: diskNodeId, dgId: diskDgId } })),
          });
          items.push({
            id: 'del-disk',
            label: 'Delete Disk',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Disk', diskId, async () => {
              await runMutation('Delete Disk', t.label || t.id, () => removeDisk(diskNodeId, diskDgId, diskId));
            }, 'All zones on this disk will be lost.'),
          });
        }
      } else {
        if (t.type === 'Store' && modules?.groups !== false) {
          items.push({
            id: 'add-group',
            label: 'Add Group',
            icon: <Database className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addGroup: { storeId: t.id } })),
          });
          // System store (store 0) cannot be deleted individually.
          if (t.id !== '0') {
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-store',
              label: 'Delete Store',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => requestDelete('Store', t.id, async () => { await runMutation('Delete Store', t.id, () => removeStore(t.id)); }),
            });
          }
        } else if (t.type === 'Group') {
          const storeId = p.store_id;
          const isSystemGroup = storeId === '0' && t.id === '0';
          if (modules?.replicas !== false) {
            items.push({
              id: 'add-replica',
              label: 'Add Replica',
              icon: <Plus className="tw-h-4 tw-w-4" />,
              onSelect: () => {
                if (storeId) setDialog((d) => ({ ...d, addReplica: { storeId: String(storeId), groupId: t.id } }));
              },
            });
          }
          // System group (store 0, group 0) cannot be deleted individually.
          if (!isSystemGroup) {
            items.push({ id: 's1', separator: true });
            items.push({
              id: 'del-group',
              label: 'Delete Group',
              icon: <Trash2 className="tw-h-4 tw-w-4" />,
              destructive: true,
              onSelect: () => {
                if (storeId)
                  requestDelete('Group', t.id, async () => {
                    await runMutation('Delete Group', `${storeId}/${t.id}`, () => removeGroup(String(storeId), t.id));
                  });
              },
            });
          }
        } else if (t.type === 'Replica') {
          const storeId = p.store_id;
          const groupId = p.group_id;
          items.push({
            id: 'del-replica',
            label: 'Delete Replica',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => {
              if (storeId && groupId)
                requestDelete('Replica', t.id, async () => {
                  await runMutation('Delete Replica', `${storeId}/${groupId}/${t.id}`, () => removeReplica(String(storeId), String(groupId), t.id));
                });
            },
          });
        }
      }
      return items;
    },
    [readonly, physicalActive, capacityActive, modules, requestDelete, runMutation, serverNodeIds, diskdbNodeIds, capacityUsage],
  );

  const onTreeContextMenu = useCallback(
    (node: TreeNode, event: React.MouseEvent) => {
      const target: MenuTarget = {
        type: node.type,
        id: node.rawId != null ? String(node.rawId) : node.id,
        rawId: node.rawId,
        parentIds: node.parentIds,
        label: node.label,
      };
      const items = buildMenuItems(target);
      if (items.length > 0) openMenu(event, items);
    },
    [buildMenuItems, openMenu],
  );

  const onCanvasContextMenu = useCallback(
    (target: MenuTarget, event: React.MouseEvent) => {
      const items = buildMenuItems(target);
      if (items.length > 0) openMenu(event, items);
    },
    [buildMenuItems, openMenu],
  );

  const onTreeNodeClick = useCallback((node: TreeNode) => {
    setCanvasFocusRequest({ targetId: node.id, subtree: true, nonce: Date.now() });
  }, []);

  const handleAdd = useCallback(() => {
    if (readonly) return;
    if (physicalActive || capacityActive) setDialog((d) => ({ ...d, addRack: true }));
    else if (!clusterInitialized) setDialog((d) => ({ ...d, initCluster: true }));
    else setDialog((d) => ({ ...d, addStore: true }));
  }, [readonly, physicalActive, capacityActive, clusterInitialized]);

  const closeDialogs = useCallback(() => setDialog({}), []);

  const swaggerEnabled = modules?.swagger !== false;
  const kvEnabled = modules?.kv !== false;

  const rackIds = useMemo(() => racks.map((r) => r.id), [racks]);
  const nodeIds = useMemo(() => nodes.map((n) => n.id), [nodes]);
  const serverBackedNodeIds = useMemo(() => new Set(serverNodeIds), [serverNodeIds]);
  const apiTargetNodeId = useMemo(() => {
    if (selectedEntity?.type === 'Node' && serverBackedNodeIds.has(Number(selectedEntity.id))) return Number(selectedEntity.id);
    const selectedParentNodeId = selectedEntity?.parentIds?.node_id;
    if (selectedParentNodeId != null && serverBackedNodeIds.has(Number(selectedParentNodeId))) return Number(selectedParentNodeId);
    if (initialNodeId && serverBackedNodeIds.has(Number(initialNodeId))) return Number(initialNodeId);
    return servers[0]?.node_id ?? 0;
  }, [initialNodeId, selectedEntity, serverBackedNodeIds, servers]);

  const defaultAddNodeRackId = useMemo(() => {
    if (dialog.addNode?.rackId) return dialog.addNode.rackId;
    if (lastUsedRackId && rackIds.includes(lastUsedRackId)) return lastUsedRackId;
    return racks[0]?.id ?? 0;
  }, [dialog.addNode?.rackId, lastUsedRackId, rackIds, racks]);

  const deployDialogDefaults = useMemo(() => {
    if (!dialog.deployServer?.nodeId) {
      return { defaultRestPort: '19910', defaultRpcPort: '19920' };
    }
    return deployPortDefaultsForNode(
      servers,
      dialog.deployServer.nodeId,
      19910,
      19920,
      rememberedDeployPorts.mgmt,
      rememberedDeployPorts.grpc,
    );
  }, [dialog.deployServer?.nodeId, rememberedDeployPorts, servers]);

  const addNodeDeployDefaults = useMemo(
    () => {
      const nextNodeId = Number(nextIdFromSuffix(nodeIds, 1));
      return deployPortDefaultsForNode(
        servers,
        nextNodeId,
        19910,
        19920,
        rememberedDeployPorts.mgmt,
        rememberedDeployPorts.grpc,
      );
    },
    [nodeIds, rememberedDeployPorts, servers],
  );

  const deployDiskdbDefaults = useMemo(() => {
    if (!dialog.deployDiskdb?.nodeId) return '29920';
    return diskdbPortDefaultsForNode(
      diskdbInstances,
      dialog.deployDiskdb.nodeId,
      undefined,
      rememberedDeployPorts.diskdbRpc,
    );
  }, [dialog.deployDiskdb?.nodeId, diskdbInstances, rememberedDeployPorts.diskdbRpc]);

  const storeDialogDefaults = useMemo(() => {
    const availableNodeIds = servers.filter((server) => isCrowKVServerAvailable(server)).map((server) => server.node_id);
    const defaultNodeIds = availableNodeIds.length <= 7 ? availableNodeIds : availableNodeIds.slice(0, 3);
    return {
      storeId: nextNumericId(stores.map((s) => String(s.store_id)), 1),
      nodeIds: defaultNodeIds.length > 0 ? defaultNodeIds : (nodes[0] ? [nodes[0].id] : []),
    };
  }, [nodes, servers, stores]);

  const groupDialogDefaults = useMemo(() => {
    const defaults: Record<string, { groupId: string; replicaId: string; nodeIds: number[] }> = {};
    const activeNodeIds = servers
      .filter((server) => isCrowKVServerAvailable(server))
      .map((server) => server.node_id);
    for (const store of stores) {
      const storeId = String(store.store_id);
      const groupsInStore = groups.filter((g) => String(g.store_id) === storeId);
      const groupId = nextNumericId(groupsInStore.map((g) => String(g.group_id)), 1);

      const replicaIds: string[] = [];
      for (const group of groupsInStore) {
        for (const replica of group.replicas || []) {
          replicaIds.push(String(replica.replica_id));
        }
      }

      const replicaId = nextNumericId(replicaIds, 1);
      const storeNodeIds = store.nodes.filter((nodeId) => activeNodeIds.includes(nodeId));
      const nodeIds = storeNodeIds.length > 0 ? storeNodeIds : activeNodeIds.slice(0, 3);

      defaults[storeId] = { groupId, replicaId, nodeIds };
    }
    return defaults;
  }, [groups, nodes, servers, stores]);

  const replicaDialogDefaults = useMemo(() => {
    const defaults: Record<string, { nodeId: number; replicaId: string }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const existingReplicaIds = (group.replicas || []).map((replica) => String(replica.replica_id));
      const usedNodeIds = new Set((group.replicas || []).map((replica) => replica.node_id || 0));
      const preferredNode =
        servers.find((server) => !usedNodeIds.has(server.node_id))?.node_id ||
        servers[0]?.node_id ||
        nodes[0];

      defaults[key] = {
        nodeId: typeof preferredNode === 'number' ? preferredNode : (preferredNode?.id ?? 0),
        replicaId: nextNumericId(existingReplicaIds, 1),
      };
    }

    return defaults;
  }, [groups, nodes, servers]);

  const replicaDialogNodeInfo = useMemo(() => {
    const info: Record<string, { allNodes: typeof nodes; usedNodeIds: Set<number> }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const usedNodeIds = new Set((group.replicas || []).map((replica) => replica.node_id || 0));
      info[key] = { allNodes: nodes, usedNodeIds };
    }

    return info;
  }, [groups, nodes]);

  return (
    <div className="tw-min-h-screen tw-bg-bg tw-text-text crow-console">
      <Header
        clusterHealth={clusterHealth}
        onRefresh={handleRefresh}
        refreshing={refreshing}
        apiTargetNodeId={String(apiTargetNodeId)}
        showSwagger={swaggerEnabled}
        swaggerActive={centerPanel === 'swagger'}
        onToggleSwagger={() => setCenterPanel((p) => (p === 'swagger' ? 'topology' : 'swagger'))}
        showKV={kvEnabled}
        kvActive={centerPanel === 'kv'}
        onToggleKV={() => setCenterPanel((p) => (p === 'kv' ? 'topology' : 'kv'))}
        centerPanel={centerPanel}
        onShowTopology={() => setCenterPanel('topology')}
        onResetCluster={readonly ? undefined : handleResetCluster}
      />

      {dataError && (
        <div
          role="alert"
          className="tw-fixed tw-top-16 tw-left-1/2 -tw-translate-x-1/2 tw-z-50 tw-bg-failed/10 tw-border tw-border-failed/30 tw-text-failed tw-px-4 tw-py-2 tw-rounded-md tw-text-sm tw-shadow-lg"
        >
          Backend unreachable — retrying
        </div>
      )}

      <Sidebar
        racks={racks}
        servers={servers}
        stores={stores}
        nodeStores={nodeStores}
        nodeHealthById={nodeHealthById}
        loading={loading}
        readonly={readonly}
        width={sidebarWidth}
        clusterInitialized={clusterInitialized}
        onNodeClick={onTreeNodeClick}
        onNodeContextMenu={onTreeContextMenu}
        onAdd={handleAdd}
        diskdbInstances={diskdbInstances}
        capacityUsage={capacityUsage}
        nodeDiskGroups={nodeDiskGroups}
        diskdbNodeIds={diskdbNodeIds}
      />

      <div
        className="tw-fixed tw-top-14 tw-bottom-0 tw-z-30 tw-w-2 tw-cursor-col-resize hover:tw-bg-accent/20"
        style={{ left: sidebarWidth - 1 }}
        onMouseDown={() => setResizing('left')}
        aria-hidden="true"
      />

      <main
        className="tw-mt-14 tw-h-[calc(100vh-3.5rem)] tw-transition-[margin]"
        style={{
          marginLeft: sidebarWidth,
          marginRight: selectedEntity ? inspectorWidth : 0,
        }}
      >
        <Suspense fallback={<BodyFallback />}>
          {centerPanel === 'swagger' && swaggerEnabled ? (
            <SwaggerPanel nodeId={apiTargetNodeId} apiPrefix={apiPrefix} servers={servers} />
          ) : centerPanel === 'kv' && kvEnabled ? (
            <KvOperatorPanel stores={stores} selectedEntity={selectedEntity} readonly={readonly} backendError={!!dataError} loading={loading} />
          ) : capacityActive ? (
            <CapacityPanel
              instances={diskdbInstances}
              usage={capacityUsage}
              scanStatus={capacityScanStatus}
              loading={capLoading}
              readonly={readonly}
              onRefresh={refreshCapacity}
            />
          ) : (
            <TopologyCanvas
              racks={racks}
              nodes={nodes}
              servers={servers}
              stores={stores}
              nodeStores={nodeStores}
              nodeHealthById={nodeHealthById}
              diskdbNodeIds={diskdbNodeIds}
              refreshToken={lastRefreshTime.getTime()}
              focusRequest={canvasFocusRequest}
              onEntityContextMenu={onCanvasContextMenu}
            />
          )}
        </Suspense>
      </main>

      <Suspense fallback={null}>
        <Inspector readonly={readonly} modules={modules} nodes={nodes} servers={servers} stores={stores} width={inspectorWidth} />
      </Suspense>

      {selectedEntity && (
        <div
          className="tw-fixed tw-top-14 tw-bottom-0 tw-z-30 tw-w-2 tw-cursor-col-resize hover:tw-bg-accent/20"
          style={{ right: inspectorWidth - 1 }}
          onMouseDown={() => setResizing('right')}
          aria-hidden="true"
        />
      )}

      {menuState && <ContextMenu items={menuState.items} position={menuState.position} onClose={closeMenu} />}

      {/* Dialogs */}
      <AddRackDialog
        isOpen={!!dialog.addRack}
        onClose={closeDialogs}
        existingRackIds={rackIds.map(String)}
        onSuccess={handleRefresh}
      />
      {dialog.addNode && (
        <AddNodeDialog
          isOpen
          onClose={closeDialogs}
          racks={racks}
          defaultRackId={String(defaultAddNodeRackId)}
          existingNodeIds={nodeIds.map(String)}
          defaultRestPort={addNodeDeployDefaults.defaultRestPort}
          defaultRpcPort={addNodeDeployDefaults.defaultRpcPort}
          onCreatedRackId={(rackId) => setLastUsedRackId(Number(rackId))}
          onSuccess={handleRefresh}
        />
      )}
      <InitClusterDialog
        isOpen={!!dialog.initCluster}
        onClose={closeDialogs}
        nodes={nodes}
        servers={servers}
        defaultNodeIds={storeDialogDefaults.nodeIds}
        onSuccess={handleInitSuccess}
      />
      <AddStoreDialog
        isOpen={!!dialog.addStore}
        onClose={closeDialogs}
        nodes={nodes}
        servers={servers}
        defaultStoreId={storeDialogDefaults.storeId}
        defaultNodeIds={storeDialogDefaults.nodeIds}
        onSuccess={handleRefresh}
      />
      {dialog.addGroup && (
        <AddGroupDialog
          isOpen
          onClose={closeDialogs}
          storeId={dialog.addGroup.storeId}
          stores={stores}
          nodes={nodes}
          servers={servers}
          defaultGroupId={groupDialogDefaults[dialog.addGroup.storeId]?.groupId || '1'}
          defaultReplicaId={groupDialogDefaults[dialog.addGroup.storeId]?.replicaId || '1'}
          defaultNodeIds={groupDialogDefaults[dialog.addGroup.storeId]?.nodeIds || []}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.addReplica && (
        <AddReplicaDialog
          isOpen
          onClose={closeDialogs}
          storeId={dialog.addReplica.storeId}
          groupId={dialog.addReplica.groupId}
          nodes={replicaDialogNodeInfo[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.allNodes || []}
          usedNodeIds={replicaDialogNodeInfo[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.usedNodeIds || new Set()}
          defaultNodeId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.nodeId ?? 0}
          defaultReplicaId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.replicaId || ''}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.deployServer && (
        <DeployServerDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.deployServer.nodeId}
          defaultRestPort={deployDialogDefaults.defaultRestPort}
          defaultRpcPort={deployDialogDefaults.defaultRpcPort}
          onSuccess={async ({ restPort, rpcPort }) => {
            setRememberedDeployPorts((prev) => ({
              mgmt: prev.mgmt.includes(restPort) ? prev.mgmt : [...prev.mgmt, restPort],
              grpc: prev.grpc.includes(rpcPort) ? prev.grpc : [...prev.grpc, rpcPort],
              diskdbRpc: prev.diskdbRpc,
            }));
            await handleRefresh();
          }}
        />
      )}
      {dialog.delete && (
        <ConfirmDeleteDialog
          isOpen
          onClose={closeDialogs}
          resourceType={dialog.delete.type}
          resourceId={String(dialog.delete.id)}
          onDelete={dialog.delete.onDelete}
          cascadeWarning={dialog.delete.cascadeWarning}
        />
      )}
      {dialog.addDiskGroup && (
        <AddDiskGroupDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.addDiskGroup.nodeId}
          existingDgIds={(nodeDiskGroups[dialog.addDiskGroup.nodeId]?.diskGroups || []).map((dg) => dg.id)}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.addDisk && (
        <AddDiskDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.addDisk.nodeId}
          dgId={dialog.addDisk.dgId}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.moveDisk && (
        <MoveDiskDialog
          isOpen
          onClose={closeDialogs}
          diskId={dialog.moveDisk.diskId}
          currentRackId={dialog.moveDisk.rackId}
          currentNodeId={dialog.moveDisk.nodeId}
          currentDgId={dialog.moveDisk.dgId}
          racks={racks}
          diskGroupsByNode={Object.fromEntries(
            Object.entries(nodeDiskGroups).map(([k, v]) => [Number(k), v.diskGroups])
          )}
          onSuccess={handleRefresh}
        />
      )}
      <DeployDiskdbDialog
        isOpen={!!dialog.deployDiskdb}
        onClose={closeDialogs}
        nodes={nodes}
        defaultNodeId={dialog.deployDiskdb?.nodeId}
        defaultRpcPort={deployDiskdbDefaults}
        onSuccess={async () => {
          setRememberedDeployPorts((prev) => ({
            mgmt: prev.mgmt,
            grpc: prev.grpc,
            diskdbRpc: prev.diskdbRpc.includes(Number(deployDiskdbDefaults))
              ? prev.diskdbRpc
              : [...prev.diskdbRpc, Number(deployDiskdbDefaults)],
          }));
          await handleRefresh();
        }}
      />

      {dialog.compactZones && (
        <ZoneSelectDialog
          isOpen
          onClose={closeDialogs}
          title="Compact Zones"
          description={`Compact zones on disk ${dialog.compactZones.diskId.slice(0, 12)}…`}
          confirmLabel="Compact"
          diskId={dialog.compactZones.diskId}
          zoneCount={dialog.compactZones.zoneCount}
          onConfirm={async (diskId, zones) => {
            await compactDiskdbZones(diskId, zones ?? undefined);
            await handleRefresh();
          }}
        />
      )}
      {dialog.rebuildBitmap && (
        <ZoneSelectDialog
          isOpen
          onClose={closeDialogs}
          title="Rebuild Bitmap"
          description={`Rebuild zone bitmap on disk ${dialog.rebuildBitmap.diskId.slice(0, 12)}…`}
          confirmLabel="Rebuild"
          diskId={dialog.rebuildBitmap.diskId}
          zoneCount={dialog.rebuildBitmap.zoneCount}
          onConfirm={async (diskId, zones) => {
            await rebuildDiskdbZoneBitmap(diskId, zones ?? undefined);
            await handleRefresh();
          }}
        />
      )}

      <ToastContainer />
    </div>
  );
}

function BodyFallback() {
  return (
    <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">
      Loading…
    </div>
  );
}

export default function App(props: CrowConsoleProps = {}) {
  return (
    <ViewModeProvider initialViewMode={props.initialViewMode}>
      <SelectionProvider>
        <ToastProvider>
          <ActivityProvider>
            <AppContent {...props} />
          </ActivityProvider>
        </ToastProvider>
      </SelectionProvider>
    </ViewModeProvider>
  );
}
