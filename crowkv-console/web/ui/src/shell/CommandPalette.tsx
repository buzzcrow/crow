import { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import {
  Search,
  Server,
  HardDrive,
  Database,
  Users,
  LayoutDashboard,
  RefreshCw,
  CornerDownLeft,
  ArrowUp,
  ArrowDown,
} from 'lucide-react';
import { Rack, Node, StoreView } from '../types';
import { useViewMode } from '../contexts/ViewModeContext';
import { useSelection } from '../contexts/SelectionContext';
import { fuzzySearch } from '../utils/fuzzySearch';
import { cn } from '../utils/cn';
import {
  buildCommands,
  CommandItem,
  CommandCategory,
} from '../data/commandPaletteActions';

interface CommandPaletteProps {
  isOpen: boolean;
  onClose: () => void;
  racks: Rack[];
  nodes: Node[];
  stores: StoreView[];
  onRefresh: () => void | Promise<void>;
}

const CATEGORY_ORDER: CommandCategory[] = ['Entities', 'Actions', 'Views'];

function renderIcon(name: string | undefined) {
  const cls = 'tw-h-4 tw-w-4 tw-text-muted';
  switch (name) {
    case 'server':
      return <Server className={cls} />;
    case 'hard-drive':
      return <HardDrive className={cls} />;
    case 'database':
      return <Database className={cls} />;
    case 'users':
      return <Users className={cls} />;
    case 'layout-dashboard':
      return <LayoutDashboard className={cls} />;
    case 'refresh-cw':
      return <RefreshCw className={cls} />;
    default:
      return <Search className={cls} />;
  }
}

/**
 * Cmd/Ctrl+K command palette. Fuzzy-searches over entities, actions, and view
 * commands; supports keyboard navigation and category grouping in results.
 */
export function CommandPalette({
  isOpen,
  onClose,
  racks,
  nodes,
  stores,
  onRefresh,
}: CommandPaletteProps) {
  const { viewMode, toggleViewMode } = useViewMode();
  const { selectEntity } = useSelection();

  const [query, setQuery] = useState('');
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Reset state every time the palette opens.
  useEffect(() => {
    if (isOpen) {
      setQuery('');
      setHighlightedIndex(0);
      // Defer focus until after the modal mounts.
      requestAnimationFrame(() => inputRef.current?.focus());
    }
  }, [isOpen]);

  // Rebuild the command list whenever cluster data changes.
  const commands = useMemo<CommandItem[]>(
    () => buildCommands({ racks, nodes, stores }),
    [racks, nodes, stores],
  );

  // Fuzzy-filter commands by query (label + description + keywords).
  const filtered = useMemo<CommandItem[]>(() => {
    if (!query.trim()) return commands;
    const results = fuzzySearch(commands, query, (cmd) => [
      cmd.label,
      cmd.description || '',
      ...(cmd.keywords || []),
    ]);
    return results.map((r) => r.item);
  }, [commands, query]);

  // Group filtered commands by category for display, preserving original order
  // within each group.
  const grouped = useMemo(() => {
    const byCategory = new Map<CommandCategory, CommandItem[]>();
    for (const cmd of filtered) {
      const list = byCategory.get(cmd.category) || [];
      list.push(cmd);
      byCategory.set(cmd.category, list);
    }
    const sections: { category: CommandCategory; items: CommandItem[] }[] = [];
    for (const cat of CATEGORY_ORDER) {
      const items = byCategory.get(cat);
      if (items && items.length > 0) sections.push({ category: cat, items });
    }
    // Build a flat index list that mirrors the rendered order so keyboard nav
    // matches what the user sees.
    const flat: CommandItem[] = sections.flatMap((s) => s.items);
    return { sections, flat };
  }, [filtered]);

  // Keep highlighted index in range when the result set changes.
  useEffect(() => {
    if (highlightedIndex >= grouped.flat.length) {
      setHighlightedIndex(Math.max(0, grouped.flat.length - 1));
    }
  }, [grouped.flat.length, highlightedIndex]);

  const runCommand = useCallback(
    (cmd: CommandItem) => {
      cmd.handler({
        viewMode,
        toggleViewMode,
        selectEntity,
        refresh: onRefresh,
      });
      onClose();
    },
    [viewMode, toggleViewMode, selectEntity, onRefresh, onClose],
  );

  // Keyboard handling: ArrowUp/Down to navigate, Enter to execute, Escape to close.
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setHighlightedIndex((i) => Math.min(i + 1, grouped.flat.length - 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        setHighlightedIndex((i) => Math.max(i - 1, 0));
      } else if (e.key === 'Enter') {
        e.preventDefault();
        const cmd = grouped.flat[highlightedIndex];
        if (cmd) runCommand(cmd);
      } else if (e.key === 'Escape') {
        e.preventDefault();
        onClose();
      }
    },
    [grouped.flat, highlightedIndex, runCommand, onClose],
  );

  // Global Cmd/Ctrl+K hotkey to open the palette is registered in a sibling
  // hook (see useCommandPaletteHotkey below).

  // Scroll the highlighted entry into view as the user navigates.
  useEffect(() => {
    if (!listRef.current) return;
    const el = listRef.current.querySelector<HTMLElement>(
      `[data-command-index="${highlightedIndex}"]`,
    );
    if (el) {
      el.scrollIntoView({ block: 'nearest' });
    }
  }, [highlightedIndex]);

  if (!isOpen) return null;

  return (
    <div
      className="tw-fixed tw-inset-0 tw-z-[100] tw-flex tw-items-start tw-justify-center tw-pt-[15vh] tw-bg-black/60 tw-animate-fade-in"
      role="dialog"
      aria-modal="true"
      aria-label="Command palette"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="tw-w-full tw-max-w-xl tw-bg-panel tw-border tw-border-border tw-rounded-lg tw-shadow-2xl tw-animate-scale-in tw-flex tw-flex-col tw-overflow-hidden">
        {/* Input */}
        <div className="tw-flex tw-items-center tw-gap-2 tw-px-4 tw-py-3 tw-border-b tw-border-border">
          <Search className="tw-h-4 tw-w-4 tw-text-muted tw-flex-shrink-0" />
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setHighlightedIndex(0);
            }}
            onKeyDown={handleKeyDown}
            placeholder="Search entities, actions, views..."
            className="tw-flex-1 tw-bg-transparent tw-text-text tw-text-sm tw-outline-none tw-placeholder-muted"
            aria-label="Command palette search"
            aria-controls="command-palette-list"
            aria-activedescendant={
              grouped.flat[highlightedIndex]?.id
                ? `cmd-${grouped.flat[highlightedIndex].id}`
                : undefined
            }
          />
          <kbd className="tw-text-xs tw-text-muted tw-px-1.5 tw-py-0.5 tw-bg-bg tw-border tw-border-border tw-rounded">
            ESC
          </kbd>
        </div>

        {/* Results */}
        <div
          ref={listRef}
          id="command-palette-list"
          className="tw-max-h-[50vh] tw-overflow-y-auto tw-py-2 focus:tw-outline-none"
          role="listbox"
          aria-label="Command palette results"
          tabIndex={0}
        >
          {grouped.sections.length === 0 ? (
            <div className="tw-px-4 tw-py-8 tw-text-center tw-text-sm tw-text-muted">
              No results for &quot;{query}&quot;.
            </div>
          ) : (
            grouped.sections.map((section) => (
              <div key={section.category} className="tw-mb-2 last:tw-mb-0">
                <div className="tw-px-4 tw-py-1 tw-text-[10px] tw-uppercase tw-tracking-wider tw-font-semibold tw-text-muted">
                  {section.category}
                </div>
                {section.items.map((cmd) => {
                  const flatIndex = grouped.flat.indexOf(cmd);
                  const isActive = flatIndex === highlightedIndex;
                  return (
                    <div
                      key={cmd.id}
                      id={`cmd-${cmd.id}`}
                      data-command-index={flatIndex}
                      role="option"
                      aria-selected={isActive}
                      onMouseEnter={() => setHighlightedIndex(flatIndex)}
                      onClick={() => runCommand(cmd)}
                      className={cn(
                        'tw-flex tw-items-center tw-gap-3 tw-px-4 tw-py-2 tw-cursor-pointer tw-transition-colors',
                        isActive ? 'tw-bg-accent/10' : 'hover:tw-bg-bg',
                      )}
                    >
                      <span className="tw-flex-shrink-0">{renderIcon(cmd.iconName)}</span>
                      <div className="tw-flex-1 tw-min-w-0">
                        <div
                          className={cn(
                            'tw-text-sm tw-truncate',
                            isActive ? 'tw-text-accent' : 'tw-text-text',
                          )}
                        >
                          {cmd.label}
                        </div>
                        {cmd.description && (
                          <div className="tw-text-xs tw-text-muted tw-truncate">
                            {cmd.description}
                          </div>
                        )}
                      </div>
                      {cmd.shortcut && (
                        <kbd className="tw-text-xs tw-text-muted tw-px-1.5 tw-py-0.5 tw-bg-bg tw-border tw-border-border tw-rounded tw-flex-shrink-0">
                          {cmd.shortcut}
                        </kbd>
                      )}
                    </div>
                  );
                })}
              </div>
            ))
          )}
        </div>

        {/* Footer hint */}
        <div className="tw-flex tw-items-center tw-justify-between tw-px-4 tw-py-2 tw-border-t tw-border-border tw-text-xs tw-text-muted">
          <div className="tw-flex tw-items-center tw-gap-3">
            <span className="tw-flex tw-items-center tw-gap-1">
              <ArrowUp className="tw-h-3 tw-w-3" />
              <ArrowDown className="tw-h-3 tw-w-3" />
              navigate
            </span>
            <span className="tw-flex tw-items-center tw-gap-1">
              <CornerDownLeft className="tw-h-3 tw-w-3" />
              select
            </span>
          </div>
          <span>{grouped.flat.length} result{grouped.flat.length === 1 ? '' : 's'}</span>
        </div>
      </div>
    </div>
  );
}

/**
 * Hook that wires Cmd/Ctrl+K (and the `/` quick-open shortcut on Linux) to a
 * setter. Mount once near the root.
 */
export function useCommandPaletteHotkey(open: () => void) {
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
