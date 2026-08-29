// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { createContext, useContext, useState, useCallback, ReactNode } from 'react';
import { nextId } from '../utils/ids';
import { ActivityLogEntry } from '../types';

interface ActivityContextType {
  entries: ActivityLogEntry[];
  /** Append a new entry. Returns the assigned id. */
  log: (entry: Omit<ActivityLogEntry, 'id' | 'timestamp'> & { timestamp?: number }) => string;
  /** Remove all entries. */
  clear: () => void;
}

const ActivityContext = createContext<ActivityContextType | undefined>(undefined);

const MAX_ENTRIES = 1000;

/**
 * Client-side activity log. v1 keeps entries in memory only; switching to
 * server-backed storage later is a drop-in.
 */
export function ActivityProvider({ children }: { children: ReactNode }) {
  const [entries, setEntries] = useState<ActivityLogEntry[]>([]);

  const log = useCallback(
    (entry: Omit<ActivityLogEntry, 'id' | 'timestamp'> & { timestamp?: number }): string => {
      const id = nextId('act');
      const fullEntry: ActivityLogEntry = {
        id,
        timestamp: entry.timestamp ?? Date.now(),
        action: entry.action,
        target: entry.target,
        status: entry.status,
        message: entry.message,
      };
      setEntries((prev) => {
        const next = [fullEntry, ...prev];
        return next.length > MAX_ENTRIES ? next.slice(0, MAX_ENTRIES) : next;
      });
      return id;
    },
    [],
  );

  const clear = useCallback(() => {
    setEntries([]);
  }, []);

  return (
    <ActivityContext.Provider value={{ entries, log, clear }}>
      {children}
    </ActivityContext.Provider>
  );
}

export function useActivity() {
  const ctx = useContext(ActivityContext);
  if (!ctx) throw new Error('useActivity must be used within an ActivityProvider');
  return ctx;
}
