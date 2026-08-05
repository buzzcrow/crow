// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { getGroup, listStores, listGroups } from '../api';
import type { StoreView, GroupView, ReplicaView } from '../types';

interface UseLogicalTreeOptions {
  /** Polling interval in milliseconds when active (view is visible) */
  pollIntervalActive?: number;
  /** Polling interval in milliseconds when inactive (view is hidden) */
  pollIntervalInactive?: number;
  /** Whether polling is enabled */
  enabled?: boolean;
  /** Recursive depth to fetch */
  recursive?: number;
}

interface UseLogicalTreeResult {
  /** List of stores with nested groups */
  stores: StoreView[];
  /** Flat list of all groups across all stores */
  groups: GroupView[];
  /** Flat list of all replicas across all groups */
  replicas: ReplicaView[];
  /** Whether data is currently loading */
  loading: boolean;
  /** Error if fetch failed */
  error: Error | null;
  /** Manually trigger a refresh */
  refresh: () => Promise<void>;
  /** Get a specific store by ID */
  getStoreById: (storeId: string) => StoreView | undefined;
  /** Get a specific group by store ID and group ID */
  getGroupById: (storeId: string, groupId: string) => GroupView | undefined;
  /** Get a specific replica by store ID, group ID, and replica ID */
  getReplicaById: (storeId: string, groupId: string, replicaId: string) => ReplicaView | undefined;
}

/**
 * Hook for polling the logical cluster tree (stores -> groups -> replicas)
 */
export function useLogicalTree({
  pollIntervalActive = 5000,
  pollIntervalInactive = 30000,
  enabled = true,
  recursive = 3,
}: UseLogicalTreeOptions = {}): UseLogicalTreeResult {
  const [stores, setStores] = useState<StoreView[]>([]);
  const [groups, setGroups] = useState<GroupView[]>([]);
  const [replicas, setReplicas] = useState<ReplicaView[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isActiveRef = useRef(true);
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const hasLoadedRef = useRef(false);

  // Fetch logical tree data
  const fetchData = useCallback(async () => {
    if (!enabled) return;

    try {
      // Only show loading state on the initial fetch; subsequent polls
      // refresh silently to avoid flipping the sidebar placeholder.
      if (!hasLoadedRef.current) {
        setLoading(true);
      }
      // Note: do NOT clear the error optimistically here — clearing it before
      // the request resolves makes the header health pill flip between
      // Failed/Unknown every poll cycle when the server is down. It is cleared
      // only once the fetch chain actually succeeds (end of this try block).

      // Fetch stores with recursive depth
      const storesData = await listStores(recursive);
      const sourceStores = Array.isArray(storesData) ? storesData : [];

      // Build flat lists of groups and replicas
      const allGroups: GroupView[] = [];
      const allReplicas: ReplicaView[] = [];

      const enrichedStores: StoreView[] = [];
      for (const store of sourceStores) {
        // Fetch groups for each store (or use existing if recursive included them)
        let storeGroups: GroupView[];
        if (store.groups) {
          storeGroups = await Promise.all(store.groups.map(async g => {
            try {
              const detail = await getGroup(store.store_id, g.group_id, recursive);
              return {
                ...detail,
                store_id: store.store_id,
                replicas: detail.replicas || [],
              };
            } catch {
              return {
                store_id: store.store_id,
                group_id: g.group_id,
                leader: g.leader,
                replicas: [],
                state: g.health || 'Unknown',
              };
            }
          }));
        } else {
          const fetchedGroups = await listGroups(store.store_id, recursive);
          storeGroups = Array.isArray(fetchedGroups) ? fetchedGroups.map(g => ({
            ...g,
            store_id: store.store_id,
            replicas: g.replicas || [],
          })) : [];
        }
        allGroups.push(...storeGroups);
        enrichedStores.push({
          ...store,
          groups: storeGroups as any,
        });

        for (const group of storeGroups) {
          if (group.replicas && Array.isArray(group.replicas)) {
            allReplicas.push(...group.replicas.map(r => ({ ...r, store_id: store.store_id, group_id: group.group_id })));
          }
        }
      }

      setStores(enrichedStores);
      setGroups(allGroups);
      setReplicas(allReplicas);
      setError(null);
    } catch (err) {
      console.error('Failed to fetch logical tree:', err);
      setError(err instanceof Error ? err : new Error('Unknown error fetching logical tree'));
    } finally {
      hasLoadedRef.current = true;
      setLoading(false);
    }
  }, [enabled, recursive]);

  // Initial fetch
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Polling logic
  useEffect(() => {
    if (!enabled) return;

    const scheduleNextPoll = () => {
      // Clear any existing timeout
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }

      // Schedule next poll based on active status
      const interval = isActiveRef.current ? pollIntervalActive : pollIntervalInactive;
      pollTimeoutRef.current = setTimeout(async () => {
        await fetchData();
        scheduleNextPoll();
      }, interval);
    };

    scheduleNextPoll();

    return () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
    };
  }, [enabled, pollIntervalActive, pollIntervalInactive, fetchData]);

  // Track page visibility to adjust polling interval
  useEffect(() => {
    const handleVisibilityChange = () => {
      isActiveRef.current = document.visibilityState === 'visible';
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, []);

  // Get a store by ID
  const getStoreById = useCallback(
    (storeId: string): StoreView | undefined => {
      return stores.find(s => s.store_id === storeId);
    },
    [stores]
  );

  // Get a group by store ID and group ID
  const getGroupById = useCallback(
    (storeId: string, groupId: string): GroupView | undefined => {
      return groups.find(g => g.store_id === storeId && g.group_id === groupId);
    },
    [groups]
  );

  // Get a replica by store ID, group ID, and replica ID
  const getReplicaById = useCallback(
    (storeId: string, groupId: string, replicaId: string): ReplicaView | undefined => {
      return replicas.find(r => r.store_id === storeId && r.group_id === groupId && r.replica_id === replicaId);
    },
    [replicas]
  );

  return {
    stores,
    groups,
    replicas,
    loading,
    error,
    refresh: fetchData,
    getStoreById,
    getGroupById,
    getReplicaById,
  };
}
