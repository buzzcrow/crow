// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import { useState, useEffect, useCallback, useRef } from "react";
import {
  listDiskdbInstances,
  getDiskdbUsage,
  getDiskdbScanStatus,
  getHardwareCapacity,
  listNodeDiskGroups,
  listDisksInGroup,
} from "../api";
import type {
  DiskdbInstanceInfo,
  CapacityUsageResponse,
  ScanStatusResponse,
  DiskGroupEntry,
  DiskEntry,
  HardwareCapacitySummary,
} from "../types";

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
  hardwareCapacity: HardwareCapacitySummary | null;
  scanStatus: ScanStatusResponse | null;
  loading: boolean;
  error: Error | null;
  refresh: () => Promise<void>;
  nodeDiskGroups: Record<number, NodeDiskGroups>;
  fetchNodeDiskGroups: (nodeIds: number[]) => Promise<void>;
}

export function useCapacityTree({
  pollIntervalActive = 3000,
  pollIntervalInactive = 30000,
  enabled = true,
}: UseCapacityTreeOptions = {}): UseCapacityTreeResult {
  const [instances, setInstances] = useState<DiskdbInstanceInfo[]>([]);
  const [usage, setUsage] = useState<CapacityUsageResponse | null>(null);
  const [hardwareCapacity, setHardwareCapacity] =
    useState<HardwareCapacitySummary | null>(null);
  const [scanStatus, setScanStatus] = useState<ScanStatusResponse | null>(null);
  const [nodeDiskGroups, setNodeDiskGroups] = useState<
    Record<number, NodeDiskGroups>
  >({});
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

      const [instancesResult, usageResult, hwCapResult, scanResult] =
        await Promise.allSettled([
          listDiskdbInstances(),
          getDiskdbUsage(),
          getHardwareCapacity(),
          getDiskdbScanStatus(),
        ]);

      if (instancesResult.status === "fulfilled") {
        setInstances(instancesResult.value);
      } else {
        setInstances([]);
      }

      if (usageResult.status === "fulfilled") {
        setUsage(usageResult.value);
      } else {
        setUsage(null);
      }

      if (hwCapResult.status === "fulfilled") {
        setHardwareCapacity(hwCapResult.value);
      } else {
        setHardwareCapacity(null);
      }

      if (scanResult.status === "fulfilled") {
        setScanStatus(scanResult.value);
      } else {
        setScanStatus(null);
      }

      setError(null);
    } catch (err) {
      console.error("Failed to fetch capacity tree:", err);
      setError(
        err instanceof Error
          ? err
          : new Error("Unknown error fetching capacity tree"),
      );
    } finally {
      hasLoadedRef.current = true;
      setLoading(false);
    }
  }, [enabled]);

  const fetchNodeDiskGroups = useCallback(
    async (nodeIds: number[]) => {
      if (!enabled || nodeIds.length === 0) {
        setNodeDiskGroups({});
        return;
      }
      try {
        // Fetch disk-groups for all nodes first and render them immediately.
        // Disks are loaded afterwards so a slow group-0 query for one DG
        // doesn't block rendering the rest of the tree.
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

        // Load disks for each DG in the background. Each completed fetch
        // merges into the existing node entry without waiting for all DGs.
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
                // leave disks undefined; the UI will retry on next poll
              }
            }),
          ),
        );
      } catch (err) {
        console.error("Failed to fetch node disk-groups:", err);
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

      const interval = isActiveRef.current
        ? pollIntervalActive
        : pollIntervalInactive;
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
      isActiveRef.current = document.visibilityState === "visible";
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () =>
      document.removeEventListener("visibilitychange", handleVisibilityChange);
  }, []);

  return {
    instances,
    usage,
    hardwareCapacity,
    scanStatus,
    loading,
    error,
    refresh: fetchData,
    nodeDiskGroups,
    fetchNodeDiskGroups,
  };
}
