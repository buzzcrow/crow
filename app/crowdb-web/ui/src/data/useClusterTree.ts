// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { listRacks, listNodes, listNodeStores, pingNode, listNodeDiskGroups, listDisksInGroup } from '../api';
import { NodeHealth } from '../types';
import type { Rack, Node, NodeStore, DiskGroupEntry, DiskEntry } from '../types';

export interface NodeDiskGroups {
  diskGroups: DiskGroupEntry[];
  disksByDg: Record<number, DiskEntry[]>;
}

interface UseClusterTreeOptions {
  pollIntervalActive?: number;
  pollIntervalInactive?: number;
  enabled?: boolean;
  recursive?: number;
}

interface UseClusterTreeResult {
  racks: Rack[];
  nodes: Node[];
  nodeStores: Record<string, NodeStore[]>;
  nodeHealthById: Record<string, NodeHealth>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
  getNodeById: (nodeId: number) => Node | undefined;
}

/**
 * Hook for polling the cluster infrastructure tree:
 * racks -> nodes -> { KV stores/groups, disk-groups/disks }.
 *
 * Merges the former `usePhysicalTree` (rack/node/KV) with the
 * disk-group/disk fetch logic from `useCapacityTree`.
 */
export function useClusterTree({
  pollIntervalActive = 3000,
  pollIntervalInactive = 30000,
  enabled = true,
  recursive = 3,
}: UseClusterTreeOptions = {}): UseClusterTreeResult {
  const [racks, setRacks] = useState<Rack[]>([]);
  const [nodes, setNodes] = useState<Node[]>([]);
  const [nodeStores, setNodeStores] = useState<Record<string, NodeStore[]>>({});
  const [nodeHealthById, setNodeHealthById] = useState<Record<string, NodeHealth>>({});
  const [nodeDiskGroups, setNodeDiskGroups] = useState<Record<number, NodeDiskGroups>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const isActiveRef = useRef(true);
  const pollTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const hasLoadedRef = useRef(false);

  const fetchData = useCallback(async () => {
    if (!enabled) return;

    try {
      if (!hasLoadedRef.current) {
        setLoading(true);
      }

      const racksData = await listRacks(recursive);
      setRacks(Array.isArray(racksData) ? racksData : []);

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

      // Determine which nodes host a server.
      const serverNodeIds = new Set<number>();
      for (const n of nodeList) if (n.server) serverNodeIds.add(n.id);
      for (const rack of Array.isArray(racksData) ? racksData : []) {
        for (const entry of rack.nodes || []) {
          if (entry.has_server || entry.server) {
            serverNodeIds.add(entry.id);
          }
        }
      }

      // Fetch per-node KV store/group detail in parallel.
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

      // Fetch disk-groups + disks for every node (not just server nodes —
      // disk-groups can exist on a node without a running KV server).
      const allNodeIds = nodeList.map((n) => n.id);
      await fetchDiskGroups(allNodeIds);

      setError(null);
    } catch (err) {
      console.error('Failed to fetch cluster tree:', err);
      setError(err instanceof Error ? err : new Error('Unknown error fetching cluster tree'));
    } finally {
      hasLoadedRef.current = true;
      setLoading(false);
    }
  }, [enabled, recursive]);

  const fetchDiskGroups = useCallback(
    async (nodeIds: number[]) => {
      if (!enabled || nodeIds.length === 0) {
        setNodeDiskGroups({});
        return;
      }
      try {
        // Fetch disk-groups for all nodes first; render immediately.
        const dgLists = await Promise.all(
          nodeIds.map(async (nodeId) => {
            try {
              const dgs = await listNodeDiskGroups(nodeId);
              return [nodeId, dgs] as const;
            } catch {
              return [nodeId, [] as DiskGroupEntry[]] as const;
            }
          }),
        );
        setNodeDiskGroups((prev) => {
          const map: Record<number, NodeDiskGroups> = { ...prev };
          for (const [id, dgs] of dgLists) {
            const existing = map[id];
            map[id] = {
              diskGroups: dgs,
              disksByDg: existing?.disksByDg ?? {},
            };
          }
          return map;
        });

        // Load disks for each DG in the background.
        await Promise.all(
          dgLists.flatMap(([nodeId, dgs]) =>
            dgs.map(async (dg) => {
              try {
                const disks = await listDisksInGroup(nodeId, dg.id);
                setNodeDiskGroups((prev) => {
                  const node = prev[nodeId] || {
                    diskGroups: dgs,
                    disksByDg: {},
                  };
                  if (node.diskGroups.length === 0 && dgs.length > 0) {
                    node.diskGroups = dgs;
                  }
                  node.disksByDg = { ...node.disksByDg, [dg.id]: disks };
                  return { ...prev, [nodeId]: node };
                });
              } catch {
                // leave disks undefined; UI retries on next poll
              }
            }),
          ),
        );
      } catch (err) {
        console.error('Failed to fetch node disk-groups:', err);
      }
    },
    [enabled],
  );

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (!enabled) return;

    const scheduleNextPoll = () => {
      if (pollTimeoutRef.current) {
        clearTimeout(pollTimeoutRef.current);
      }
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

  useEffect(() => {
    const handleVisibilityChange = () => {
      isActiveRef.current = document.visibilityState === 'visible';
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
  }, []);

  const getNodeById = useCallback(
    (nodeId: number): Node | undefined => {
      return nodes.find((n) => n.id === nodeId);
    },
    [nodes],
  );

  return {
    racks,
    nodes,
    nodeStores,
    nodeHealthById,
    nodeDiskGroups,
    loading,
    error,
    refresh: fetchData,
    getNodeById,
  };
}
