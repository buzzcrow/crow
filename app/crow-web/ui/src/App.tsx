// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { Suspense, useState, useCallback, useMemo, lazy, useEffect } from 'react';
import { Server, Database, Plus, Trash2, Activity, RotateCw, Square, HardDrive } from 'lucide-react';
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
  DeployServerDialog,
  DeployDiskdbDialog,
  ConfirmDeleteDialog,
  InitClusterDialog,
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
  restartDiskdb,
  stopDiskdb,
} from './api';
import { deployPortDefaultsForNode, nextIdFromSuffix, nextNumericId } from './components/dialogs/defaults';
import { buildCrowKVServers, crowKvServerNodeIds } from './data/crowKvServers';
import { isCrowKVServerAvailable } from './data/crowKvServers';
import { toUiHealth } from './utils/entityDisplay';

const TopologyCanvas = lazy(() =>
  import('./topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })),
);
const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));
const SwaggerPanel = lazy(() => import('./panels/SwaggerPanel').then((m) => ({ default: m.SwaggerPanel })));
const KvOperatorPanel = lazy(() => import('./panels/KvOperatorPanel').then((m) => ({ default: m.KvOperatorPanel })));

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
  const [rememberedDeployPorts, setRememberedDeployPorts] = useState<{ mgmt: number[]; grpc: number[] }>({ mgmt: [], grpc: [] });
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
    deployServer?: { nodeId: number };
    deployDiskdb?: boolean;
    delete?: { type: string; id: string | number; onDelete: () => Promise<void> };
    initCluster?: boolean;
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
  const { instances: diskdbInstances, usage: capacityUsage, loading: capLoading, error: capError, refresh: refreshCapacity } = useCapacityTree({
    enabled: viewMode === ViewMode.Capacity,
    pollIntervalActive: 5000,
    pollIntervalInactive: 30000,
  });

  const loading = physLoading || logLoading || capLoading;
  const dataError = physError || logError || capError;
  const servers = useMemo(() => buildCrowKVServers(nodes, racks), [nodes, racks]);
  const serverNodeIds = useMemo(() => crowKvServerNodeIds(servers), [servers]);
  const diskdbNodeIds = useMemo(() => new Set(diskdbInstances.map((i) => i.instance_id)), [diskdbInstances]);
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
      setLastRefreshTime(new Date());
    } finally {
      setRefreshing(false);
    }
  }, [refreshPhysical, refreshLogical, refreshCapacity]);

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
    (type: string, id: string | number, onDelete: () => Promise<void>) => {
      setDialog((d) => ({ ...d, delete: { type, id, onDelete } }));
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
          if (!hasServer) {
            items.push({
              id: 'deploy',
              label: 'Deploy Crow Storage',
              icon: <Server className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployServer: { nodeId } })),
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
          if (hasServer) {
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
          }
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-node',
            label: 'Delete Node',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Node', nodeId, async () => { await runMutation('Delete Node', t.label || t.id, () => removeNode(nodeId)); }),
          });
        }
      } else if (capacityActive) {
        // Capacity view context menus: node, diskdb instance, disk-group, disk.
        if (t.type === 'Node') {
          const nodeId = Number(t.rawId ?? t.id);
          const hasDiskdb = diskdbNodeIds.has(nodeId);
          if (!hasDiskdb) {
            items.push({
              id: 'ddb-deploy',
              label: 'Deploy DiskDB',
              icon: <HardDrive className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployDiskdb: true })),
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
        } else if (t.type === 'Server') {
          // diskdb instance — restart / stop
          items.push({
            id: 'ddb-restart',
            label: 'Restart DiskDB',
            icon: <RotateCw className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Restart DiskDB', t.label || t.id, () => restartDiskdb(Number(p.instance_id))),
          });
          items.push({
            id: 'ddb-stop',
            label: 'Stop DiskDB',
            icon: <Square className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Stop DiskDB', t.label || t.id, () => stopDiskdb(Number(p.instance_id))),
          });
        } else if (t.type === 'Group') {
          // disk-group — trigger scan, recalc usage
          const dgId = Number(t.rawId);
          items.push({
            id: 'ddb-scan',
            label: 'Trigger Scan',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Trigger Scan', t.label || t.id, () => triggerDiskdbScan(dgId)),
          });
          items.push({
            id: 'ddb-recalc',
            label: 'Recalc Usage',
            icon: <RotateCw className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Recalc Usage', t.label || t.id, () => recalcDiskdbUsage(dgId)),
          });
        } else if (t.type === 'Replica') {
          // disk — compact, rebuild, set status
          const diskId = String(p.disk_id || t.rawId || t.id);
          items.push({
            id: 'ddb-compact',
            label: 'Compact Zones',
            icon: <Database className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Compact Zones', t.label || t.id, () => compactDiskdbZones(diskId)),
          });
          items.push({
            id: 'ddb-rebuild',
            label: 'Rebuild Bitmap',
            icon: <RotateCw className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Rebuild Bitmap', t.label || t.id, () => rebuildDiskdbZoneBitmap(diskId)),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'disk-set-down',
            label: 'Set Disk Down',
            icon: <Square className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Set Disk Down', t.label || t.id, () => setDiskStatus(diskId, 'Down')),
          });
          items.push({
            id: 'disk-set-up',
            label: 'Set Disk Up',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () => runMutation('Set Disk Up', t.label || t.id, () => setDiskStatus(diskId, 'Up')),
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
    [readonly, physicalActive, capacityActive, modules, requestDelete, runMutation, serverNodeIds, diskdbNodeIds],
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
        />
      )}
      <DeployDiskdbDialog
        isOpen={!!dialog.deployDiskdb}
        onClose={closeDialogs}
        nodes={nodes}
        onSuccess={handleRefresh}
      />

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
