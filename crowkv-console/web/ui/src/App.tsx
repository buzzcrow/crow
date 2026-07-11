import { Suspense, useState, useCallback, useMemo, lazy, useEffect } from 'react';
import { Server, Database, Plus, Trash2, Activity, RotateCw, Square } from 'lucide-react';
import { ViewModeProvider, useViewMode } from './contexts/ViewModeContext';
import { SelectionProvider, useSelection } from './contexts/SelectionContext';
import { ToastProvider, useToast } from './contexts/ToastContext';
import { ActivityProvider, useActivity } from './contexts/ActivityContext';
import { usePhysicalTree } from './data/usePhysicalTree';
import { useLogicalTree } from './data/useLogicalTree';
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
  ConfirmDeleteDialog,
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
} from './api';
import { deployPortDefaultsForNode, nextIdFromSuffix, nextNumericId } from './components/dialogs/defaults';
import { buildCrowKVServers, crowkvServerNodeIds } from './data/crowkvServers';
import { isCrowKVServerAvailable } from './data/crowkvServers';
import { toUiHealth } from './utils/entityDisplay';

const TopologyCanvas = lazy(() =>
  import('./topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })),
);
const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));
const SwaggerPanel = lazy(() => import('./panels/SwaggerPanel').then((m) => ({ default: m.SwaggerPanel })));

export interface CrowkvConsoleProps {
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

function AppContent({ apiPrefix = '/api', readonly = false, modules, initialNodeId = '', onEvent }: CrowkvConsoleProps) {
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

  const [lastUsedRackId, setLastUsedRackId] = useState<string>('');
  const [rememberedDeployPorts, setRememberedDeployPorts] = useState<{ mgmt: number[]; grpc: number[] }>({ mgmt: [], grpc: [] });
  const [lastRefreshTime, setLastRefreshTime] = useState<Date>(new Date());
  const [refreshing, setRefreshing] = useState(false);
  const [showSwagger, setShowSwagger] = useState(false);
  const [sidebarWidth, setSidebarWidth] = useState(350);
  const [inspectorWidth, setInspectorWidth] = useState(320);
  const [resizing, setResizing] = useState<'left' | 'right' | null>(null);
  const [canvasFocusRequest, setCanvasFocusRequest] = useState<{ targetId: string; subtree: boolean; nonce: number } | null>(null);

  const [dialog, setDialog] = useState<{
    addRack?: boolean;
    addNode?: { rackId: string };
    addStore?: boolean;
    addGroup?: { storeId: string };
    addReplica?: { storeId: string; groupId: string };
    deployServer?: { nodeId: string };
    delete?: { type: string; id: string; onDelete: () => Promise<void> };
  }>({});

  const { menuState, openMenu, closeMenu } = useContextMenu();

  const physicalActive = viewMode === ViewMode.Physical;
  const { racks, nodes, nodeStores, nodeHealthById, loading: physLoading, error: physError, refresh: refreshPhysical } = usePhysicalTree({
    enabled: true,
    recursive: 2,
    pollIntervalActive: 3000,
    pollIntervalInactive: 30000,
  });
  const { stores, groups, loading: logLoading, error: logError, refresh: refreshLogical } = useLogicalTree({
    enabled: true,
    recursive: 2,
    pollIntervalActive: 3000,
    pollIntervalInactive: 30000,
  });

  const loading = physLoading || logLoading;
  const dataError = physError || logError;
  const servers = useMemo(() => buildCrowKVServers(nodes, racks), [nodes, racks]);
  const serverNodeIds = useMemo(() => crowkvServerNodeIds(servers), [servers]);

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
      await Promise.all([refreshPhysical(), refreshLogical()]);
      setLastRefreshTime(new Date());
    } finally {
      setRefreshing(false);
    }
  }, [refreshPhysical, refreshLogical]);

  useEffect(() => {
    if (!resizing) return;

    const onMouseMove = (event: MouseEvent) => {
      if (resizing === 'left') {
        setSidebarWidth(Math.min(480, Math.max(220, event.clientX)));
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
    (type: string, id: string, onDelete: () => Promise<void>) => {
      setDialog((d) => ({ ...d, delete: { type, id, onDelete } }));
    },
    [],
  );

  /** Build per-layer context menu items for a normalized target. */
  const buildMenuItems = useCallback(
    (t: MenuTarget): MenuItemOrSeparator[] => {
      if (readonly) return [];
      const items: MenuItemOrSeparator[] = [];
      const p = t.parentIds || {};

      if (physicalActive) {
        if (t.type === 'Rack' && modules?.nodes !== false) {
          items.push({
            id: 'add-node',
            label: 'Add Node',
            icon: <Server className="tw-h-4 tw-w-4" />,
            onSelect: () => setDialog((d) => ({ ...d, addNode: { rackId: t.id } })),
          });
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-rack',
            label: 'Delete Rack',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Rack', t.id, async () => { await runMutation('Delete Rack', t.id, () => removeRack(t.id)); }),
          });
        } else if (t.type === 'Node') {
          const hasServer = serverNodeIds.has(t.id);
          if (!hasServer) {
            items.push({
              id: 'deploy',
              label: 'Deploy CrowKV',
              icon: <Server className="tw-h-4 tw-w-4" />,
              onSelect: () => setDialog((d) => ({ ...d, deployServer: { nodeId: t.id } })),
            });
          }
          items.push({
            id: 'ping',
            label: 'Ping',
            icon: <Activity className="tw-h-4 tw-w-4" />,
            onSelect: () =>
              runMutation('Ping Node', t.id, async () => {
                const r = await pingNode(t.id);
                if (!r.ok) throw new Error(r.error || 'unreachable');
              }),
          });
          if (hasServer) {
            items.push({
              id: 'restart',
              label: 'Restart CrowKV',
              icon: <RotateCw className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Restart CrowKV', t.id, () => restartServer(t.id)),
            });
            items.push({
              id: 'stop',
              label: 'Stop CrowKV',
              icon: <Square className="tw-h-4 tw-w-4" />,
              onSelect: () => runMutation('Stop CrowKV', t.id, () => stopServer(t.id)),
            });
          }
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-node',
            label: 'Delete Node',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Node', t.id, async () => { await runMutation('Delete Node', t.id, () => removeNode(t.id)); }),
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
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-store',
            label: 'Delete Store',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => requestDelete('Store', t.id, async () => { await runMutation('Delete Store', t.id, () => removeStore(t.id)); }),
          });
        } else if (t.type === 'Group') {
          const storeId = p.store_id;
          if (modules?.replicas !== false) {
            items.push({
              id: 'add-replica',
              label: 'Add Replica',
              icon: <Plus className="tw-h-4 tw-w-4" />,
              onSelect: () => {
                if (storeId) setDialog((d) => ({ ...d, addReplica: { storeId, groupId: t.id } }));
              },
            });
          }
          items.push({ id: 's1', separator: true });
          items.push({
            id: 'del-group',
            label: 'Delete Group',
            icon: <Trash2 className="tw-h-4 tw-w-4" />,
            destructive: true,
            onSelect: () => {
              if (storeId)
                requestDelete('Group', t.id, async () => {
                  await runMutation('Delete Group', `${storeId}/${t.id}`, () => removeGroup(storeId, t.id));
                });
            },
          });
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
                  await runMutation('Delete Replica', `${storeId}/${groupId}/${t.id}`, () => removeReplica(storeId, groupId, t.id));
                });
            },
          });
        }
      }
      return items;
    },
    [readonly, physicalActive, modules, requestDelete, runMutation, serverNodeIds],
  );

  const onTreeContextMenu = useCallback(
    (node: TreeNode, event: React.MouseEvent) => {
      const target: MenuTarget = {
        type: node.type,
        id: node.rawId ?? node.id,
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
    if (physicalActive) setDialog((d) => ({ ...d, addRack: true }));
    else setDialog((d) => ({ ...d, addStore: true }));
  }, [readonly, physicalActive]);

  const closeDialogs = useCallback(() => setDialog({}), []);

  const swaggerEnabled = modules?.swagger !== false;

  const rackIds = useMemo(() => racks.map((r) => r.id), [racks]);
  const nodeIds = useMemo(() => nodes.map((n) => n.id), [nodes]);
  const serverBackedNodeIds = useMemo(() => new Set(serverNodeIds), [serverNodeIds]);
  const apiTargetNodeId = useMemo(() => {
    if (selectedEntity?.type === 'Node' && serverBackedNodeIds.has(selectedEntity.id)) return selectedEntity.id;
    const selectedParentNodeId = selectedEntity?.parentIds?.node_id;
    if (selectedParentNodeId && serverBackedNodeIds.has(selectedParentNodeId)) return selectedParentNodeId;
    if (initialNodeId && serverBackedNodeIds.has(initialNodeId)) return initialNodeId;
    return servers[0]?.node_id || '';
  }, [initialNodeId, selectedEntity, serverBackedNodeIds, servers]);

  const defaultAddNodeRackId = useMemo(() => {
    if (dialog.addNode?.rackId) return dialog.addNode.rackId;
    if (lastUsedRackId && rackIds.includes(lastUsedRackId)) return lastUsedRackId;
    return racks[0]?.id || '';
  }, [dialog.addNode?.rackId, lastUsedRackId, rackIds, racks]);

  const deployDialogDefaults = useMemo(() => {
    if (!dialog.deployServer?.nodeId) {
      return { defaultMgmtPort: '19910', defaultGrpcPort: '19920' };
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
      const nextNodeId = nextIdFromSuffix(nodeIds, 1);
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
    const defaultNodeIds = servers.filter((server) => isCrowKVServerAvailable(server)).slice(0, 1).map((server) => server.node_id);
    return {
      storeId: nextNumericId(stores.map((s) => String(s.store_id)), 1),
      nodeIds: defaultNodeIds.length > 0 ? defaultNodeIds : (nodes[0] ? [nodes[0].id] : []),
    };
  }, [nodes, servers, stores]);

  const groupDialogDefaults = useMemo(() => {
    const defaults: Record<string, { groupId: string; replicaId: string; nodeIds: string[] }> = {};
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
      const preferredNodes = servers.map((server) => server.node_id);
      const nodeIds = preferredNodes.length > 0 ? [preferredNodes[0]] : (nodes[0] ? [nodes[0].id] : []);

      defaults[storeId] = { groupId, replicaId, nodeIds };
    }
    return defaults;
  }, [groups, nodes, servers, stores]);

  const replicaDialogDefaults = useMemo(() => {
    const defaults: Record<string, { nodeId: string; replicaId: string }> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const existingReplicaIds = (group.replicas || []).map((replica) => String(replica.replica_id));
      const usedNodeIds = new Set((group.replicas || []).map((replica) => String(replica.node_id || '')));
      const preferredNode =
        servers.find((server) => !usedNodeIds.has(server.node_id))?.node_id ||
        servers[0]?.node_id ||
        nodes[0];

      defaults[key] = {
        nodeId: typeof preferredNode === 'string' ? preferredNode : (preferredNode?.id || ''),
        replicaId: nextNumericId(existingReplicaIds, 1),
      };
    }

    return defaults;
  }, [groups, nodes, servers]);

  const replicaDialogAvailableNodes = useMemo(() => {
    const available: Record<string, typeof nodes> = {};

    for (const group of groups) {
      const key = `${group.store_id}:${group.group_id}`;
      const usedNodeIds = new Set((group.replicas || []).map((replica) => String(replica.node_id || '')));
      available[key] = nodes.filter((node) => !usedNodeIds.has(node.id));
    }

    return available;
  }, [groups, nodes]);

  return (
    <div className="tw-min-h-screen tw-bg-bg tw-text-text crowkv-console">
      <Header
        clusterHealth={clusterHealth}
        onRefresh={handleRefresh}
        refreshing={refreshing}
        apiTargetNodeId={apiTargetNodeId}
        showSwagger={swaggerEnabled}
        swaggerActive={showSwagger}
        onToggleSwagger={() => setShowSwagger((v) => !v)}
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
        onNodeClick={onTreeNodeClick}
        onNodeContextMenu={onTreeContextMenu}
        onAdd={handleAdd}
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
          {showSwagger && swaggerEnabled ? (
            <SwaggerPanel nodeId={apiTargetNodeId} apiPrefix={apiPrefix} />
          ) : (
            <TopologyCanvas
              racks={racks}
              nodes={nodes}
              servers={servers}
              stores={stores}
              nodeStores={nodeStores}
              nodeHealthById={nodeHealthById}
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
        existingRackIds={rackIds}
        onSuccess={handleRefresh}
      />
      {dialog.addNode && (
        <AddNodeDialog
          isOpen
          onClose={closeDialogs}
          racks={racks}
          defaultRackId={defaultAddNodeRackId}
          existingNodeIds={nodeIds}
          defaultMgmtPort={addNodeDeployDefaults.defaultMgmtPort}
          defaultGrpcPort={addNodeDeployDefaults.defaultGrpcPort}
          onCreatedRackId={setLastUsedRackId}
          onSuccess={handleRefresh}
        />
      )}
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
          nodes={replicaDialogAvailableNodes[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`] || []}
          defaultNodeId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.nodeId || ''}
          defaultReplicaId={replicaDialogDefaults[`${dialog.addReplica.storeId}:${dialog.addReplica.groupId}`]?.replicaId || ''}
          onSuccess={handleRefresh}
        />
      )}
      {dialog.deployServer && (
        <DeployServerDialog
          isOpen
          onClose={closeDialogs}
          nodeId={dialog.deployServer.nodeId}
          defaultMgmtPort={deployDialogDefaults.defaultMgmtPort}
          defaultGrpcPort={deployDialogDefaults.defaultGrpcPort}
          onSuccess={async ({ mgmtPort, grpcPort }) => {
            setRememberedDeployPorts((prev) => ({
              mgmt: prev.mgmt.includes(mgmtPort) ? prev.mgmt : [...prev.mgmt, mgmtPort],
              grpc: prev.grpc.includes(grpcPort) ? prev.grpc : [...prev.grpc, grpcPort],
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
          resourceId={dialog.delete.id}
          onDelete={dialog.delete.onDelete}
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

export default function App(props: CrowkvConsoleProps = {}) {
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
