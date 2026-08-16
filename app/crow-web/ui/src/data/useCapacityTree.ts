// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from 'react';
import { listDiskdbInstances, getDiskdbUsage, getDiskdbScanStatus, listNodeDiskGroups, listDisksInGroup } from '../api';
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
  DiskGroupEntry,
  DiskEntry,
} from '../types';

interface UseCapacityTreeOptions {
  pollIntervalActive?: number;
  pollIntervalInactive?: number;
  enabled?: boolean;
}

export interface NodeDiskGroups {
  diskGroups: DiskGroupEntry[];
  disksByDg: Record<number, DiskEntry[]>;
}

interface UseCapacityTreeResult {
  instances: DiskdbInstanceInfo[];
  usage: CapacityUsageResponse | null;
  scanStatus: ScanStatusResponse | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  fetchNodeDiskGroups: (nodeIds: number[]) => Promise<void>;
}

export function useCapacityTree({
  pollIntervalActive = 5000,
  pollIntervalInactive = 30000,
  enabled = true,
}: UseCapacityTreeOptions = {}): UseCapacityTreeResult {
  const [instances, setInstances] = useState<DiskdbInstanceInfo[]>([]);
  const [usage, setUsage] = useState<CapacityUsageResponse | null>(null);
  const [scanStatus, setScanStatus] = useState<ScanStatusResponse | null>(null);
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

      const [instancesResult, usageResult, scanResult] = await Promise.allSettled([
        listDiskdbInstances(),
        getDiskdbUsage(),
        getDiskdbScanStatus(),
      ]);

      if (instancesResult.status === 'fulfilled') {
        setInstances(instancesResult.value);
      } else {
        setInstances([]);
      }

      if (usageResult.status === 'fulfilled') {
        setUsage(usageResult.value);
      } else {
        setUsage(null);
      }

      if (scanResult.status === 'fulfilled') {
        setScanStatus(scanResult.value);
      } else {
        setScanStatus(null);
      }

      setError(null);
    } catch (err) {
      console.error('Failed to fetch capacity tree:', err);
      setError(err instanceof Error ? err : new Error('Unknown error fetching capacity tree'));
    } finally {
      hasLoadedRef.current = true;
      setLoading(false);
    }
  }, [enabled]);

  const fetchNodeDiskGroups = useCallback(async (nodeIds: number[]) => {
    if (!enabled || nodeIds.length === 0) {
      setNodeDiskGroups({});
      return;
    }
    try {
      const entries = await Promise.all(
        nodeIds.map(async (nodeId) => {
          try {
            const dgs = await listNodeDiskGroups(nodeId);
            const disksByDg: Record<number, DiskEntry[]> = {};
            await Promise.all(
              dgs.map(async (dg) => {
                try {
                  disksByDg[dg.id] = await listDisksInGroup(nodeId, dg.id);
                } catch {
                  disksByDg[dg.id] = [];
                }
              }),
            );
            return [nodeId, { diskGroups: dgs, disksByDg }] as const;
          } catch {
            return [nodeId, { diskGroups: [] as DiskGroupEntry[], disksByDg: {} as Record<number, DiskEntry[]> }] as const;
          }
        }),
      );
      const map: Record<number, NodeDiskGroups> = {};
      for (const [id, val] of entries) {
        map[id] = val;
      }
      setNodeDiskGroups(map);
    } catch (err) {
      console.error('Failed to fetch node disk-groups:', err);
    }
  }, [enabled]);

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

  return {
    instances,
    usage,
    scanStatus,
    loading,
    error,
    refresh: fetchData,
    nodeDiskGroups,
    fetchNodeDiskGroups,
  };
}
