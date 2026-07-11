import { ReactNode, useRef, useEffect, useState, useCallback } from 'react';
import { cn } from '../utils/cn';

export interface MenuItem {
  /** Stable id for the item. */
  id: string;
  /** Label shown in the menu. */
  label: string;
  /** Optional icon for the item. */
  icon?: ReactNode;
  /** Optional sublabel / hint. */
  hint?: string;
  /** Whether the item is disabled. */
  disabled?: boolean;
  /** Whether the item is destructive (shows red). */
  destructive?: boolean;
  /** Invoked when the user clicks the item. */
  onSelect: () => void | Promise<void>;
}

export interface MenuSeparator {
  id: string;
  separator: true;
}

export type MenuItemOrSeparator = MenuItem | MenuSeparator;

export interface ContextMenuProps {
  /** The menu items or separators to render. */
  items: MenuItemOrSeparator[];
  /** Position of the menu (screen coordinates). */
  position: { x: number; y: number };
  /** Called when the menu should close. */
  onClose: () => void;
}

/**
 * Reusable context menu component with keyboard navigation.
 */
export function ContextMenu({ items, position, onClose }: ContextMenuProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [focusedIndex, setFocusedIndex] = useState<number>(0);

  // Get the index of the first enabled item
  const getFirstEnabledIndex = useCallback(() => {
    for (let i = 0; i < items.length; i++) {
      const item = items[i];
      if (!('separator' in item) && !item.disabled) {
        return i;
      }
    }
    return -1;
  }, [items]);

  // Get the index of the last enabled item
  const getLastEnabledIndex = useCallback(() => {
    for (let i = items.length - 1; i >= 0; i--) {
      const item = items[i];
      if (!('separator' in item) && !item.disabled) {
        return i;
      }
    }
    return -1;
  }, [items]);

  // Get the next enabled item index
  const getNextEnabledIndex = useCallback((current: number) => {
    for (let i = current + 1; i < items.length; i++) {
      const item = items[i];
      if (!('separator' in item) && !item.disabled) {
        return i;
      }
    }
    return getFirstEnabledIndex();
  }, [items, getFirstEnabledIndex]);

  // Get the previous enabled item index
  const getPrevEnabledIndex = useCallback((current: number) => {
    for (let i = current - 1; i >= 0; i--) {
      const item = items[i];
      if (!('separator' in item) && !item.disabled) {
        return i;
      }
    }
    return getLastEnabledIndex();
  }, [items, getLastEnabledIndex]);

  // Handle keyboard navigation
  useEffect(() => {
    if (!containerRef.current) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      switch (e.key) {
        case 'ArrowDown':
          e.preventDefault();
          setFocusedIndex(getNextEnabledIndex);
          break;
        case 'ArrowUp':
          e.preventDefault();
          setFocusedIndex(getPrevEnabledIndex);
          break;
        case 'Enter':
        case ' ':
          e.preventDefault();
          const focusedItem = items[focusedIndex];
          if (focusedItem && !('separator' in focusedItem) && !focusedItem.disabled) {
            void focusedItem.onSelect();
            onClose();
          }
          break;
        case 'Escape':
          onClose();
          break;
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [focusedIndex, items, onClose, getNextEnabledIndex, getPrevEnabledIndex]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as globalThis.Node)) {
        onClose();
      }
    };
    window.addEventListener('mousedown', handler);
    return () => window.removeEventListener('mousedown', handler);
  }, [onClose]);

  // Focus the menu when it opens
  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.focus();
      setFocusedIndex(getFirstEnabledIndex());
    }
  }, [getFirstEnabledIndex]);

  // Adjust position to fit in viewport
  const adjustedPosition = useRef(position);
  useEffect(() => {
    if (containerRef.current) {
      const rect = containerRef.current.getBoundingClientRect();
      let x = position.x;
      let y = position.y;

      // Don't go off the right edge
      if (x + rect.width > window.innerWidth) {
        x = Math.max(0, window.innerWidth - rect.width);
      }

      // Don't go off the bottom edge
      if (y + rect.height > window.innerHeight) {
        y = Math.max(0, window.innerHeight - rect.height);
      }

      adjustedPosition.current = { x, y };
    }
  }, [position]);

  return (
    <div
      ref={containerRef}
      className="tw-fixed tw-z-[200] tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-py-1 tw-animate-fade-in tw-min-w-[180px]"
      role="menu"
      tabIndex={-1}
      style={{ left: adjustedPosition.current.x, top: adjustedPosition.current.y }}
    >
      {items.map((item, index) => {
        if ('separator' in item) {
          return (
            <div
              key={item.id}
              className="tw-my-1 tw-h-px tw-bg-border tw-mx-2"
              role="separator"
            />
          );
        }

        const isFocused = index === focusedIndex;

        return (
          <button
            key={item.id}
            role="menuitem"
            tabIndex={isFocused ? 0 : -1}
            onClick={async () => {
              if (!item.disabled) {
                await item.onSelect();
                onClose();
              }
            }}
            onMouseEnter={() => !item.disabled && setFocusedIndex(index)}
            disabled={item.disabled}
            className={cn(
              'tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-text-left tw-text-sm tw-transition-colors',
              item.disabled ? 'tw-text-muted tw-cursor-not-allowed' : 'tw-text-text hover:tw-bg-bg',
              item.destructive && !item.disabled ? 'tw-text-failed hover:tw-bg-failed/10' : '',
              isFocused ? 'tw-bg-bg' : ''
            )}
          >
            {item.icon && <span className="tw-flex-shrink-0">{item.icon}</span>}
            <div className="tw-flex-1 tw-min-w-0">
              <div>{item.label}</div>
              {item.hint && <div className="tw-text-[10px] tw-text-muted">{item.hint}</div>}
            </div>
          </button>
        );
      })}
    </div>
  );
}

/**
 * Hook for managing context menu state in a component.
 */
export function useContextMenu() {
  const [menuState, setMenuState] = useState<{
    items: MenuItemOrSeparator[];
    position: { x: number; y: number };
  } | null>(null);

  const openMenu = useCallback(
    (e: React.MouseEvent, items: MenuItemOrSeparator[]) => {
      e.preventDefault();
      e.stopPropagation();
      setMenuState({
        items,
        position: { x: e.clientX, y: e.clientY },
      });
    },
    []
  );

  const closeMenu = useCallback(() => {
    setMenuState(null);
  }, []);

  return {
    menuState,
    openMenu,
    closeMenu,
  };
}
