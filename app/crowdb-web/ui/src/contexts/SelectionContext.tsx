// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, useCallback, ReactNode } from 'react';
import { Domain } from '../types';

export type EntityType = 'Datacenter' | 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica' | 'DiskGroup' | 'Disk';

/**
 * The single selected entity. `parentIds` carries the ancestor chain in
 * snake_case (`rack_id`, `store_id`, `group_id`, `node_id`) so API calls and
 * cross-jumps can resolve the full path.
 */
export interface SelectedEntity {
  type: EntityType;
  id: string;
  parentIds?: Record<string, string | number>;
  domain: Domain;
  name?: string;
  /** Service flavor for `Server` entities: KV vs DiskDB. */
  serviceType?: 'kv' | 'diskdb';
}

interface SelectionContextType {
  selectedEntity: SelectedEntity | null;
  selectEntity: (entity: SelectedEntity | null) => void;
  clearSelection: () => void;
  isSelected: (entityId: string) => boolean;
}

const SelectionContext = createContext<SelectionContextType | undefined>(undefined);

export function SelectionProvider({ children }: { children: ReactNode }) {
  const [selectedEntity, setSelectedEntity] = useState<SelectedEntity | null>(null);

  const selectEntity = useCallback((entity: SelectedEntity | null) => {
    setSelectedEntity(entity);
  }, []);

  const clearSelection = useCallback(() => setSelectedEntity(null), []);

  const isSelected = useCallback(
    (entityId: string) => selectedEntity?.id === entityId,
    [selectedEntity],
  );

  return (
    <SelectionContext.Provider value={{ selectedEntity, selectEntity, clearSelection, isSelected }}>
      {children}
    </SelectionContext.Provider>
  );
}

export function useSelection() {
  const context = useContext(SelectionContext);
  if (context === undefined) {
    throw new Error('useSelection must be used within a SelectionProvider');
  }
  return context;
}
