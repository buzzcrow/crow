import React, { Suspense, lazy, useState, useCallback } from 'react';
import { ViewModeProvider } from './contexts/ViewModeContext';
import { ThemeProvider } from './contexts/ThemeContext';
import { SelectionProvider } from './contexts/SelectionContext';
import { ToastProvider } from './contexts/ToastContext';
import { ActivityProvider } from './contexts/ActivityContext';
import { usePhysicalTree } from './data/usePhysicalTree';
import { useLogicalTree } from './data/useLogicalTree';
import { Header } from './shell/Header';
import { Sidebar } from './shell/Sidebar';
import { ToastContainer } from './components/ToastContainer';
import { TreeNode } from './components/Tree';
import { useEffect } from 'react';
import { CustomAction, CustomPanel, ThemeMode, ViewMode } from './types';

// Lazy-loaded chunks: each pulls heavy deps (reactflow / portal-only UI) the
// initial render does not need.
const TopologyCanvas = lazy(() =>
  import('./topology/TopologyCanvas').then((m) => ({ default: m.TopologyCanvas })),
);
const Inspector = lazy(() => import('./shell/Inspector').then((m) => ({ default: m.Inspector })));
const CommandPalette = lazy(() =>
  import('./shell/CommandPalette').then((m) => ({ default: m.CommandPalette })),
);

/** Inline Cmd/Ctrl+K hotkey so we don't statically pull the CommandPalette chunk. */
function useCommandPaletteHotkey(open: () => void) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        open();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [open]);
}

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

  // Manual refresh
  const handleRefresh = useCallback(async () => {
    await Promise.all([refreshPhysical(), refreshLogical()]);
    setLastRefreshTime(new Date());
  }, [refreshPhysical, refreshLogical]);

  // Handle node click from sidebar (selection is handled inside Sidebar/Tree via SelectionContext)
  const handleNodeClick = useCallback((_node: TreeNode) => {
    // Inspector / panels will react to SelectionContext.
  }, []);

  const handleOpenCommandPalette = useCallback(() => {
    setIsCommandPaletteOpen(true);
  }, []);

  useCommandPaletteHotkey(handleOpenCommandPalette);

  // TODO: Compute aggregate cluster health from physical+logical trees.
  const clusterHealth = 'Healthy' as const;

  return (
    <div className="tw-min-h-screen tw-bg-bg tw-text-text crowkv-console">
      <Header
        brandLogo={brandLogo}
        clusterHealth={clusterHealth}
        lastRefreshTime={lastRefreshTime}
        onRefresh={handleRefresh}
        nodes={nodes.map(node => ({ id: node.id, host: node.host }))}
        selectedNodeId={selectedNodeId}
        onNodeSelect={setSelectedNodeId}
        onOpenCommandPalette={handleOpenCommandPalette}
      />

      <div className="tw-flex">
        <Sidebar racks={racks} stores={stores} loading={loading} onNodeClick={handleNodeClick} />

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
