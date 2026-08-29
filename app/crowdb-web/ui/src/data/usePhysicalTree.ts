// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { listRacks, listNodes, listNodeStores, pingNode } from '../api';
import { NodeHealth } from '../types';
import type { Rack, Node, NodeStore } from '../types';

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
  /** Per-node store/group detail (local + remotes), keyed by node id. */
  nodeStores: Record<string, NodeStore[]>;
  nodeHealthById: Record<string, NodeHealth>;
  /** Whether data is currently loading */
  loading: boolean;
  /** Error if fetch failed */
  error: Error | null;
  /** Manually trigger a refresh */
  refresh: () => Promise<void>;
  /** Get a specific node by ID */
  getNodeById: (nodeId: number) => Node | undefined;
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
  const [nodeStores, setNodeStores] = useState<Record<string, NodeStore[]>>({});
  const [nodeHealthById, setNodeHealthById] = useState<Record<string, NodeHealth>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isActiveRef = useRef(true);
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const hasLoadedRef = useRef(false);

  // Fetch physical tree data
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

      // Fetch racks with recursive depth
      const racksData = await listRacks(recursive);
      setRacks(Array.isArray(racksData) ? racksData : []);

      // Fetch flat list of nodes
      const nodesData = await listNodes(undefined, recursive);
      const nodeList = Array.isArray(nodesData) ? nodesData : [];
      setNodes(nodeList);

      const reachability = await Promise.all(
        nodeList.map(async (node) => {
          try {
            const result = await pingNode(node.id);
            return [node.id, result.ok ? NodeHealth.Up : NodeHealth.Down] as const;
          } catch {
            return [node.id, NodeHealth.Unknown] as const;
          }
        }),
      );
      setNodeHealthById(Object.fromEntries(reachability));

      // Only nodes with a running server are in the monitor cache; querying
      // a serverless node returns 404 (noisy console errors). Determine
      // which nodes host a server from the flat list and the recursive rack
      // view (`has_server`), and fetch per-node store detail only for those.
      const serverNodeIds = new Set<number>();
      for (const n of nodeList) if (n.server) serverNodeIds.add(n.id);
      for (const rack of Array.isArray(racksData) ? racksData : []) {
        for (const entry of rack.nodes || []) {
          if (entry.has_server || entry.server) {
            serverNodeIds.add(entry.id);
          }
        }
      }

      // Fetch per-node store/group detail (local + remotes) in parallel.
      // This is the physical/debugging view's source of peer wiring; the
      // recursive racks tree only inlines the local replica per group.
      const storeEntries = await Promise.all(
        [...serverNodeIds].map(async (id) => {
          try {
            const ns = await listNodeStores(id);
            return [id, Array.isArray(ns) ? ns : []] as const;
          } catch {
            return [id, [] as NodeStore[]] as const;
          }
        }),
      );
      setNodeStores(Object.fromEntries(storeEntries));
      setError(null);
    } catch (err) {
      console.error('Failed to fetch physical tree:', err);
      setError(err instanceof Error ? err : new Error('Unknown error fetching physical tree'));
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

  // Get a node by ID
  const getNodeById = useCallback(
    (nodeId: number): Node | undefined => {
      return nodes.find(n => n.id === nodeId);
    },
    [nodes]
  );

  return {
    racks,
    nodes,
    nodeStores,
    nodeHealthById,
    loading,
    error,
    refresh: fetchData,
    getNodeById,
  };
}
