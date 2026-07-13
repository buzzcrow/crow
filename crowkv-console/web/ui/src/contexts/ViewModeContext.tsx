// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, ReactNode } from 'react';
import { ViewMode } from '../types';

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
  const [viewMode, setViewMode] = useState<ViewMode>(initialViewMode ?? ViewMode.Physical);

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
