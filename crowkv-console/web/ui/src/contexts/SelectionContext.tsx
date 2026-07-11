import { createContext, useContext, useState, useEffect, ReactNode, useCallback } from 'react';
import { ViewMode } from '../types';
import { localStorage } from '../utils/localStorage';

const RECENT_ITEMS_KEY = 'recentItems' as const;
const FAVORITES_KEY = 'favorites' as const;
const MAX_RECENT_ITEMS = 10;

export interface SelectedEntity {
  type: 'Rack' | 'Node' | 'Server' | 'Store' | 'Group' | 'Replica';
  id: string;
  parentIds?: Record<string, string>;
  viewMode: ViewMode;
  name?: string;
}

interface SelectionContextType {
  // Single selection
  selectedEntity: SelectedEntity | null;
  selectEntity: (entity: SelectedEntity | null) => void;
  clearSelection: () => void;

  // Multi selection
  selectedEntities: SelectedEntity[];
  addToSelection: (entity: SelectedEntity) => void;
  removeFromSelection: (entityId: string) => void;
  toggleSelection: (entity: SelectedEntity) => void;
  isSelected: (entityId: string) => boolean;
  clearMultiSelection: () => void;

  // Recent items
  recentItems: SelectedEntity[];
  addToRecentItems: (entity: SelectedEntity) => void;
  clearRecentItems: () => void;

  // Favorites
  favorites: SelectedEntity[];
  addToFavorites: (entity: SelectedEntity) => void;
  removeFromFavorites: (entityId: string) => void;
  isFavorite: (entityId: string) => boolean;
  clearFavorites: () => void;
}

const SelectionContext = createContext<SelectionContextType | undefined>(undefined);

interface SelectionProviderProps {
  children: ReactNode;
}

export function SelectionProvider({ children }: SelectionProviderProps) {
  // Single selection
  const [selectedEntity, setSelectedEntity] = useState<SelectedEntity | null>(null);

  // Multi selection
  const [selectedEntities, setSelectedEntities] = useState<SelectedEntity[]>([]);

  // Recent items (persisted)
  const [recentItems, setRecentItems] = useState<SelectedEntity[]>(() => {
    return localStorage.get<SelectedEntity[]>(RECENT_ITEMS_KEY, []);
  });

  // Favorites (persisted)
  const [favorites, setFavorites] = useState<SelectedEntity[]>(() => {
    return localStorage.get<SelectedEntity[]>(FAVORITES_KEY, []);
  });

  // Persist recent items to localStorage
  useEffect(() => {
    localStorage.set(RECENT_ITEMS_KEY, recentItems);
  }, [recentItems]);

  // Persist favorites to localStorage
  useEffect(() => {
    localStorage.set(FAVORITES_KEY, favorites);
  }, [favorites]);

  const selectEntity = useCallback((entity: SelectedEntity | null) => {
    setSelectedEntity(entity);
    if (entity) {
      addToRecentItems(entity);
    }
  }, []);

  const clearSelection = useCallback(() => {
    setSelectedEntity(null);
  }, []);

  const addToSelection = useCallback((entity: SelectedEntity) => {
    setSelectedEntities(prev => {
      if (prev.some(e => e.id === entity.id)) return prev;
      return [...prev, entity];
    });
  }, []);

  const removeFromSelection = useCallback((entityId: string) => {
    setSelectedEntities(prev => prev.filter(e => e.id !== entityId));
  }, []);

  const toggleSelection = useCallback((entity: SelectedEntity) => {
    setSelectedEntities(prev => {
      if (prev.some(e => e.id === entity.id)) {
        return prev.filter(e => e.id !== entity.id);
      } else {
        return [...prev, entity];
      }
    });
  }, []);

  const isSelected = useCallback((entityId: string) => {
    return selectedEntities.some(e => e.id === entityId) || selectedEntity?.id === entityId;
  }, [selectedEntities, selectedEntity]);

  const clearMultiSelection = useCallback(() => {
    setSelectedEntities([]);
  }, []);

  const addToRecentItems = useCallback((entity: SelectedEntity) => {
    setRecentItems(prev => {
      // Remove existing entry if present
      const filtered = prev.filter(e => e.id !== entity.id);
      // Add to beginning and limit to MAX_RECENT_ITEMS
      return [entity, ...filtered].slice(0, MAX_RECENT_ITEMS);
    });
  }, []);

  const clearRecentItems = useCallback(() => {
    setRecentItems([]);
  }, []);

  const addToFavorites = useCallback((entity: SelectedEntity) => {
    setFavorites(prev => {
      if (prev.some(e => e.id === entity.id)) return prev;
      return [...prev, entity];
    });
  }, []);

  const removeFromFavorites = useCallback((entityId: string) => {
    setFavorites(prev => prev.filter(e => e.id !== entityId));
  }, []);

  const isFavorite = useCallback((entityId: string) => {
    return favorites.some(e => e.id === entityId);
  }, [favorites]);

  const clearFavorites = useCallback(() => {
    setFavorites([]);
  }, []);

  return (
    <SelectionContext.Provider
      value={{
        selectedEntity,
        selectEntity,
        clearSelection,
        selectedEntities,
        addToSelection,
        removeFromSelection,
        toggleSelection,
        isSelected,
        clearMultiSelection,
        recentItems,
        addToRecentItems,
        clearRecentItems,
        favorites,
        addToFavorites,
        removeFromFavorites,
        isFavorite,
        clearFavorites,
      }}
    >
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
