import React, { Suspense, useState, useCallback, useMemo, lazy } from 'react';
import { Plus, Server, Database, Trash2 } from 'lucide-react';
import { ViewModeProvider, useViewMode } from './contexts/ViewModeContext';
import { ThemeProvider } from './contexts/ThemeContext';
import { SelectionProvider, useSelection } from './contexts/SelectionContext';
import { ToastProvider } from './contexts/ToastContext';
import { ActivityProvider } from './contexts/ActivityContext';
import { usePhysicalTree } from './data/usePhysicalTree';
import { useLogicalTree } from './data/useLogicalTree';
import { Header } from './shell/Header';
import { Sidebar } from './shell/Sidebar';
import { ToastContainer } from './components/ToastContainer';
import { TreeNode } from './components/Tree';
import { ContextMenu, useContextMenu, MenuItemOrSeparator } from './components/ContextMenu';
import {
  AddRackDialog,
  AddNodeDialog,
  AddStoreDialog,
  AddGroupDialog,
  AddReplicaDialog,
  DeployServerDialog,
  ConfirmDeleteDialog
} from './components/dialogs';
import { CustomAction, CustomPanel, ThemeMode, ViewMode } from './types';
import {
  removeRack,
  removeNode,
  removeStore,
  removeGroup,
  removeReplica,
  stopServer
} from './api';

// Lazy-loaded chunks: each pulls heavy deps (reactflow / portal-only UI) the
// initial render doesn't need.
const TopologyCanvas = lazy(() =>
  import('./topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })),
);
const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));
const CommandPalette = lazy(() =>
  import('./shell/CommandPalette').then((m) => ({ default: m.CommandPalette })),
);

export interface CrowkvConsoleProps {
  /** Custom logo replacing the default CrowKV brand in the header. */
  brandLogo?: React.ReactNode;
  /** Initial theme mode; defaults to system. */
  themeMode?: ThemeMode;
  /** Initial view mode; defaults to Logical. */
  initialViewMode?: ViewMode;
  /** Custom inspector / context-menu actions. */
  customActions?: CustomAction[];
  /** Custom inspector panels rendered after built-in tabs. */
  customPanels?: CustomPanel[];
  /** API prefix forwarded to custom panels; defaults to '/api'. */
  apiPrefix?: string;
  /** Structured event callback for host integration. */
  onEvent?: (event: { type: string; payload?: unknown }) => void;
}

function AppContent({ brandLogo, customPanels, customActions, apiPrefix, onEvent }: CrowkvConsoleProps) {
  const [selectedNodeId, setSelectedNodeId] = useState<string>('');
  const [lastRefreshTime, setLastRefreshTime] = useState<Date>(new Date());
  const [isCommandPaletteOpen, setIsCommandPaletteOpen] = useState(false);
  const { viewMode } = useViewMode();
  const { selectedEntity } = useSelection();

  // Dialog states
  const [dialogState, setDialogState] = useState<{
    addRack: boolean;
    addNode: boolean;
    addStore: boolean;
    addGroup: boolean;
    addReplica: boolean;
    deployServer: { nodeId: string } | null;
    delete: { type: string; id: string; onDelete: () => void | Promise<void> } | null;
  }>({
    addRack: false,
    addNode: false,
    addStore: false,
    addGroup: false,
    addReplica: false,
    deployServer: null,
    delete: null,
  });

  // Context menu state
  const { menuState, openMenu, closeMenu } = useContextMenu();

  // Data hooks
  const { racks, nodes, loading: physicalLoading, refresh: refreshPhysical } = usePhysicalTree({
    enabled: true,
    recursive: 2,
  });
  const { stores, loading: logicalLoading, refresh: refreshLogical } = useLogicalTree({
    enabled: true,
    recursive: 2,
  });

  const loading = physicalLoading || logicalLoading;

  // Refresh everything
  const handleRefresh = useCallback(async () => {
    await Promise.all([refreshPhysical(), refreshLogical()]);
    setLastRefreshTime(new Date());
  }, [refreshPhysical, refreshLogical]);

  // Dialog helpers
  type BooleanDialogKey = 'addRack' | 'addNode' | 'addStore' | 'addGroup' | 'addReplica';
  const openDialog = useCallback((type: BooleanDialogKey) => {
    setDialogState(prev => ({ ...prev, [type]: true }));
  }, []);

  const closeDialog = useCallback((type: BooleanDialogKey) => {
    setDialogState(prev => ({ ...prev, [type]: false }));
  }, []);

  // The Sidebar tree builds composite ids like `rack-r1`, `node-n1`,
  // `store-7`, etc. so React keys stay unique across views. API
  // handlers must use the unprefixed backend id, exposed as
  // `node.rawId`. We keep a defensive `prefix-` strip as a fallback so
  // any future caller that forgets to set `rawId` still talks to the
  // right backend route instead of 404'ing with `node node-n1 not found`.
  const backendId = useCallback((node: TreeNode): string => {
    if (node.rawId) return node.rawId;
    const prefix = `${node.type.toLowerCase()}-`;
    return node.id.startsWith(prefix) ? node.id.slice(prefix.length) : node.id;
  }, []);

  // Context menu handler
  const handleNodeContextMenu = useCallback((node: TreeNode, event: React.MouseEvent) => {
    const items: MenuItemOrSeparator[] = [];

    // Physical view actions
    if (viewMode === ViewMode.Physical) {
      if (node.type === 'Rack') {
        items.push({
          id: 'add-node',
          label: 'Add Node',
          icon: <Server className="tw-h-4 tw-w-4" />,
          onSelect: () => {
            setDialogState(prev => ({ ...prev, addNode: true }));
          },
        });
        items.push({ id: 'sep1', separator: true });
        items.push({
          id: 'delete-rack',
          label: 'Delete Rack',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => {
            setDialogState(prev => ({
              ...prev,
              delete: {
                type: 'Rack',
                id: backendId(node),
                onDelete: async () => {
                  await removeRack(backendId(node));
                  await handleRefresh();
                }
              }
            }));
          },
        });
      } else if (node.type === 'Node') {
        items.push({
          id: 'deploy-server',
          label: 'Deploy Server',
          icon: <Server className="tw-h-4 tw-w-4" />,
          onSelect: () => {
            setDialogState(prev => ({ ...prev, deployServer: { nodeId: backendId(node) } }));
          },
        });
        items.push({
          id: 'stop-server',
          label: 'Stop Server',
          icon: <Server className="tw-h-4 tw-w-4" />,
          onSelect: async () => {
            try {
              await stopServer(backendId(node));
              await handleRefresh();
            } catch {
              // Errors surfaced via fetchWithOptions; nothing extra here.
            }
          },
        });
        items.push({ id: 'sep1', separator: true });
        items.push({
          id: 'delete-node',
          label: 'Delete Node',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => {
            setDialogState(prev => ({
              ...prev,
              delete: {
                type: 'Node',
                id: backendId(node),
                onDelete: async () => {
                  await removeNode(backendId(node));
                  await handleRefresh();
                }
              }
            }));
          },
        });
      }
    } else {
      // Logical view actions
      if (node.type === 'Store') {
        items.push({
          id: 'add-group',
          label: 'Add Group',
          icon: <Database className="tw-h-4 tw-w-4" />,
          onSelect: () => {
            setDialogState(prev => ({ ...prev, addGroup: true }));
          },
        });
        items.push({ id: 'sep1', separator: true });
        items.push({
          id: 'delete-store',
          label: 'Delete Store',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => {
            setDialogState(prev => ({
              ...prev,
              delete: {
                type: 'Store',
                id: backendId(node),
                onDelete: async () => {
                  await removeStore(backendId(node));
                  await handleRefresh();
                }
              }
            }));
          },
        });
      } else if (node.type === 'Group') {
        items.push({
          id: 'add-replica',
          label: 'Add Replica',
          icon: <Plus className="tw-h-4 tw-w-4" />,
          onSelect: () => {
            setDialogState(prev => ({ ...prev, addReplica: true }));
          },
        });
        items.push({ id: 'sep1', separator: true });
        items.push({
          id: 'delete-group',
          label: 'Delete Group',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => {
            setDialogState(prev => ({
              ...prev,
              delete: {
                type: 'Group',
                id: backendId(node),
                onDelete: async () => {
                  const storeId = node.parentIds?.store_id as string;
                  if (storeId) {
                    await removeGroup(storeId, backendId(node));
                    await handleRefresh();
                  }
                }
              }
            }));
          },
        });
      } else if (node.type === 'Replica') {
        items.push({
          id: 'delete-replica',
          label: 'Delete Replica',
          icon: <Trash2 className="tw-h-4 tw-w-4" />,
          destructive: true,
          onSelect: () => {
            setDialogState(prev => ({
              ...prev,
              delete: {
                type: 'Replica',
                id: backendId(node),
                onDelete: async () => {
                  const storeId = node.parentIds?.store_id as string;
                  const groupId = node.parentIds?.group_id as string;
                  if (storeId && groupId) {
                    await removeReplica(storeId, groupId, backendId(node));
                    await handleRefresh();
                  }
                }
              }
            }));
          },
        });
      }
    }

    if (items.length > 0) {
      openMenu(event, items);
    }
  }, [viewMode, openMenu, handleRefresh]);

  // Handle node click from sidebar (selection is handled inside Sidebar/Tree via SelectionContext)
  const handleNodeClick = useCallback((_node: TreeNode) => {
    // Inspector / panels will react to SelectionContext.
  }, []);

  // Handle add button click from sidebar
  const handleAdd = useCallback(() => {
    if (viewMode === ViewMode.Physical) {
      openDialog('addRack');
    } else {
      openDialog('addStore');
    }
  }, [viewMode, openDialog]);

  const handleOpenCommandPalette = useCallback(() => {
    setIsCommandPaletteOpen(true);
  }, []);

  // Get store/group info for dialogs
  const currentStoreId = useMemo(() => {
    if (selectedEntity?.type === 'Store') return selectedEntity.id;
    if (selectedEntity?.type === 'Group') return selectedEntity.parentIds?.store_id as string;
    if (selectedEntity?.type === 'Replica') return selectedEntity.parentIds?.store_id as string;
    if (stores.length > 0) return stores[0].store_id;
    return '';
  }, [selectedEntity, stores]);

  const currentGroupId = useMemo(() => {
    if (selectedEntity?.type === 'Group') return selectedEntity.id;
    if (selectedEntity?.type === 'Replica') return selectedEntity.parentIds?.group_id as string;
    return '';
  }, [selectedEntity]);

  const currentRackId = useMemo(() => {
    if (selectedEntity?.type === 'Rack') return selectedEntity.id;
    if (selectedEntity?.type === 'Node') return selectedEntity.parentIds?.rack_id as string;
    if (racks.length > 0) return racks[0].id;
    return '';
  }, [selectedEntity, racks]);

  return (
    <div className="tw-min-h-screen tw-bg-bg tw-text-text crowkv-console">
      <Header
        brandLogo={brandLogo}
        clusterHealth="Healthy"
        lastRefreshTime={lastRefreshTime}
        onRefresh={handleRefresh}
        nodes={nodes.map(node => ({ id: node.id, host: node.host }))}
        selectedNodeId={selectedNodeId}
        onNodeSelect={setSelectedNodeId}
        onOpenCommandPalette={handleOpenCommandPalette}
      />

      <div className="tw-flex">
        <Sidebar
          racks={racks}
          stores={stores}
          loading={loading}
          onNodeClick={handleNodeClick}
          onNodeContextMenu={handleNodeContextMenu}
          onAdd={handleAdd}
        />

        <main className="tw-flex-1 tw-ml-64 tw-mt-14 tw-min-h-[calc(100vh-3.5rem)] tw-h-[calc(100vh-3.5rem)]">
          <Suspense fallback={<CanvasFallback />}>
            <TopologyCanvas racks={racks} nodes={nodes} stores={stores} />
          </Suspense>
        </main>
      </div>

      <Suspense fallback={null}>
        <Inspector
          customPanels={customPanels}
          customActions={customActions}
          apiPrefix={apiPrefix}
          onEvent={onEvent}
          nodes={nodes}
          stores={stores}
        />
      </Suspense>

      {isCommandPaletteOpen && (
        <Suspense fallback={null}>
          <CommandPalette
            isOpen={isCommandPaletteOpen}
            onClose={() => setIsCommandPaletteOpen(false)}
            racks={racks}
            nodes={nodes}
            stores={stores}
            onRefresh={handleRefresh}
          />
        </Suspense>
      )}

      {/* Context menu */}
      {menuState && (
        <ContextMenu
          items={menuState.items}
          position={menuState.position}
          onClose={closeMenu}
        />
      )}

      {/* Dialogs */}
      <AddRackDialog
        isOpen={dialogState.addRack}
        onClose={() => closeDialog('addRack')}
        onSuccess={handleRefresh}
      />
      <AddNodeDialog
        isOpen={dialogState.addNode}
        onClose={() => closeDialog('addNode')}
        racks={racks}
        defaultRackId={currentRackId}
        onSuccess={handleRefresh}
      />
      <AddStoreDialog
        isOpen={dialogState.addStore}
        onClose={() => closeDialog('addStore')}
        nodes={nodes}
        onSuccess={handleRefresh}
      />
      <AddGroupDialog
        isOpen={dialogState.addGroup}
        onClose={() => closeDialog('addGroup')}
        storeId={currentStoreId}
        nodes={nodes}
        onSuccess={handleRefresh}
      />
      {dialogState.deployServer && (
        <DeployServerDialog
          isOpen={true}
          onClose={() => setDialogState(prev => ({ ...prev, deployServer: null }))}
          nodeId={dialogState.deployServer.nodeId}
          onSuccess={handleRefresh}
        />
      )}
      <AddReplicaDialog
        isOpen={dialogState.addReplica}
        onClose={() => closeDialog('addReplica')}
        storeId={currentStoreId}
        groupId={currentGroupId}
        nodes={nodes}
        onSuccess={handleRefresh}
      />
      {dialogState.delete && (
        <ConfirmDeleteDialog
          isOpen={true}
          onClose={() => setDialogState(prev => ({ ...prev, delete: null }))}
          resourceType={dialogState.delete.type}
          resourceId={dialogState.delete.id}
          onDelete={dialogState.delete.onDelete}
        />
      )}

      <ToastContainer />
    </div>
  );
}

function CanvasFallback() {
  return (
    <div className="tw-w-full tw-h-full tw-flex tw-items-center tw-justify-center tw-text-muted tw-text-sm">
      Loading topology...
    </div>
  );
}

export default function App(props: CrowkvConsoleProps = {}) {
  return (
    <ThemeProvider initialThemeMode={props.themeMode}>
      <ViewModeProvider initialViewMode={props.initialViewMode}>
        <SelectionProvider>
          <ToastProvider>
            <ActivityProvider>
              <AppContent {...props} />
            </ActivityProvider>
          </ToastProvider>
        </SelectionProvider>
      </ViewModeProvider>
    </ThemeProvider>
  );
}
