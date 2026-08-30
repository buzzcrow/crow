// Copyright 2026-present Gian <crow.db@outlook.com>

import { createContext, useContext, useState, ReactNode } from 'react';
import { Domain } from '../types';

interface DomainContextType {
  domain: Domain;
  setDomain: (domain: Domain) => void;
}

const DomainContext = createContext<DomainContextType | undefined>(undefined);

interface DomainProviderProps {
  children: ReactNode;
  initialDomain?: Domain;
}

export function DomainProvider({ children, initialDomain }: DomainProviderProps) {
  const [domain, setDomain] = useState<Domain>(initialDomain ?? Domain.Cluster);

  return (
    <DomainContext.Provider value={{ domain, setDomain }}>
      {children}
    </DomainContext.Provider>
  );
}

export function useDomain() {
  const context = useContext(DomainContext);
  if (context === undefined) {
    throw new Error('useDomain must be used within a DomainProvider');
  }
  return context;
}
