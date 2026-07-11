import { useState, useEffect, useCallback, useRef } from 'react';
import { listRacks, listNodes } from '../api';
import type { Rack, Node } from '../types';

interface UsePhysicalTreeOptions {
  /** Polling interval in milliseconds when active (view is visible) */
  pollIntervalActive?: number;
  /** Polling interval in milliseconds when inactive (view is hidden) */
  pollIntervalInactive?: number;
  /** Whether polling is enabled */
  enabled?: boolean;
  /** Recursive depth to fetch */
  recursive?: number;
}

interface UsePhysicalTreeResult {
  /** List of racks with nested nodes */
  racks: Rack[];
  /** Flat list of all nodes across all racks */
  nodes: Node[];
  /** Whether data is currently loading */
  loading: boolean;
  /** Error if fetch failed */
  error: Error | null;
  /** Manually trigger a refresh */
  refresh: () => Promise<void>;
  /** Get a specific node by ID */
  getNodeById: (nodeId: string) => Node | undefined;
}

/**
 * Hook for polling the physical infrastructure tree (racks -> nodes -> servers -> stores -> groups)
 */
export function usePhysicalTree({
  pollIntervalActive = 5000,
  pollIntervalInactive = 30000,
  enabled = true,
  recursive = 3,
}: UsePhysicalTreeOptions = {}): UsePhysicalTreeResult {
  const [racks, setRacks] = useState<Rack[]>([]);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isActiveRef = useRef(true);
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Fetch physical tree data
  const fetchData = useCallback(async () => {
    if (!enabled) return;

    try {
      setLoading(true);
      setError(null);

      // Fetch racks with recursive depth
      const racksData = await listRacks(recursive);
      setRacks(Array.isArray(racksData) ? racksData : []);

      // Fetch flat list of nodes
      const nodesData = await listNodes(undefined, recursive);
      setNodes(Array.isArray(nodesData) ? nodesData : []);
    } catch (err) {
      console.error('Failed to fetch physical tree:', err);
      setError(err instanceof Error ? err : new Error('Unknown error fetching physical tree'));
    } finally {
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

  // Get a node by ID
  const getNodeById = useCallback(
    (nodeId: string): Node | undefined => {
      return nodes.find(n => n.id === nodeId);
    },
    [nodes]
  );

  return {
    racks,
    nodes,
    loading,
    error,
    refresh: fetchData,
    getNodeById,
  };
}
