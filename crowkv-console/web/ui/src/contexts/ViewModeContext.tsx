import { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { ViewMode } from '../types';
import { localStorage } from '../utils/localStorage';

const STORAGE_KEY = 'viewMode' as const;

interface ViewModeContextType {
  viewMode: ViewMode;
  setViewMode: (mode: ViewMode) => void;
  toggleViewMode: () => void;
}

const ViewModeContext = createContext<ViewModeContextType | undefined>(undefined);

interface ViewModeProviderProps {
  children: ReactNode;
  initialViewMode?: ViewMode;
}

export function ViewModeProvider({ children, initialViewMode }: ViewModeProviderProps) {
  const [viewMode, setViewMode] = useState<ViewMode>(() => {
    // Use initial prop if provided, otherwise load from storage or default to Logical
    if (initialViewMode) return initialViewMode;
    return localStorage.get<ViewMode>(STORAGE_KEY, ViewMode.Logical);
  });

  // Persist view mode changes to localStorage
  useEffect(() => {
    localStorage.set(STORAGE_KEY, viewMode);
  }, [viewMode]);

  const toggleViewMode = () => {
    setViewMode(prev => prev === ViewMode.Physical ? ViewMode.Logical : ViewMode.Physical);
  };

  return (
    <ViewModeContext.Provider value={{ viewMode, setViewMode, toggleViewMode }}>
      {children}
    </ViewModeContext.Provider>
  );
}

export function useViewMode() {
  const context = useContext(ViewModeContext);
  if (context === undefined) {
    throw new Error('useViewMode must be used within a ViewModeProvider');
  }
  return context;
}
