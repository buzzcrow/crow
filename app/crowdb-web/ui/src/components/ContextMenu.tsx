// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { ReactNode, useRef, useEffect, useState, useCallback } from 'react';
import { ChevronRight } from 'lucide-react';
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
  onSelect?: () => void | Promise<void>;
  /** Optional submenu items. When set, renders a expandable submenu
   * instead of invoking `onSelect` on click. */
  submenu?: MenuItemOrSeparator[];
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
  const [openSubmenu, setOpenSubmenu] = useState<string | null>(null);

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
            if (focusedItem.submenu) {
              setOpenSubmenu(openSubmenu === focusedItem.id ? null : focusedItem.id);
            } else {
              void focusedItem.onSelect?.();
              onClose();
            }
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

  // Adjust position to fit in viewport. Re-runs when a submenu
  // opens/closes because the menu height changes — without this,
  // submenu items near the bottom of the viewport are pushed
  // off-screen and become unclickable.
  const [adjustedPosition, setAdjustedPosition] = useState(position);
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

      setAdjustedPosition({ x, y });
    }
  }, [position, openSubmenu]);

  return (
    <div
      ref={containerRef}
      className="tw-fixed tw-z-[200] tw-bg-panel tw-border tw-border-border tw-rounded-md tw-shadow-lg tw-py-1 tw-animate-fade-in tw-min-w-[180px]"
      role="menu"
      tabIndex={-1}
      style={{ left: adjustedPosition.x, top: adjustedPosition.y }}
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
        const hasSubmenu = !!item.submenu;
        const isSubmenuOpen = openSubmenu === item.id;

        return (
          <div key={item.id}>
            <button
              role="menuitem"
              tabIndex={isFocused ? 0 : -1}
              onClick={async () => {
                if (item.disabled) return;
                if (hasSubmenu) {
                  // Always open on click — never toggle. The submenu
                  // opens on hover (onMouseEnter) too; toggling here
                  // races with that re-render and can close the
                  // submenu when the click lands after the hover
                  // state has already committed.
                  setOpenSubmenu(item.id);
                } else {
                  await item.onSelect?.();
                  onClose();
                }
              }}
              onMouseEnter={() => {
                if (!item.disabled) {
                  setFocusedIndex(index);
                  if (hasSubmenu) setOpenSubmenu(item.id);
                }
              }}
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
              {hasSubmenu && (
                <ChevronRight className={cn('tw-h-3.5 tw-w-3.5 tw-flex-shrink-0 tw-transition-transform', isSubmenuOpen && 'tw-rotate-90')} />
              )}
            </button>
            {hasSubmenu && isSubmenuOpen && (
              <div className="tw-ml-4 tw-border-l tw-border-border">
                {item.submenu!.map((subItem) => {
                  if ('separator' in subItem) {
                    return <div key={subItem.id} className="tw-my-1 tw-h-px tw-bg-border tw-mx-2" role="separator" />;
                  }
                  return (
                    <button
                      key={subItem.id}
                      role="menuitem"
                      onClick={async () => {
                        if (!subItem.disabled) {
                          await subItem.onSelect?.();
                          onClose();
                        }
                      }}
                      disabled={subItem.disabled}
                      className={cn(
                        'tw-w-full tw-flex tw-items-center tw-gap-2 tw-px-3 tw-py-1.5 tw-text-left tw-text-sm tw-transition-colors',
                        subItem.disabled ? 'tw-text-muted tw-cursor-not-allowed' : 'tw-text-text hover:tw-bg-bg',
                        subItem.destructive && !subItem.disabled ? 'tw-text-failed hover:tw-bg-failed/10' : '',
                      )}
                    >
                      {subItem.icon && <span className="tw-flex-shrink-0">{subItem.icon}</span>}
                      <div className="tw-flex-1 tw-min-w-0">
                        <div>{subItem.label}</div>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
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
